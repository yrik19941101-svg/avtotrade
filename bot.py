import asyncio
import logging
import ccxt.async_support as ccxt
import pandas as pd
import json
from datetime import datetime

CONFIG_FILE = "config.json"

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TradingBot:
    def __init__(self, config):
        self.config = config
        self.exchange = getattr(ccxt, config["exchange"])({
            'enableRateLimit': True,
            'apiKey': config['api_key'],
            'secret': config['api_secret'],
            'options': {
                'defaultType': 'swap',
                'adjustForTimeDifference': True
            }
        })
        self.consecutive_losses = 0
        self.open_positions = set()
        self.pos_data = {}
        self.all_symbols = []
        self.signal_state = {}

    async def load_markets(self):
        await self.exchange.load_markets()
        all_swap = [symbol for symbol, market in self.exchange.markets.items()
                    if market['swap'] and market['quote'] == 'USDT']
        self.all_symbols = all_swap[:50]
        logger.info(f"Загружено {len(self.all_symbols)} фьючерсных пар")
        logger.info(f"Максимум позиций: {self.config['max_positions']}, таймфрейм: {self.config['timeframe']}")
        logger.info(f"Плечо: {self.config['trade_params']['default_leverage']}x")
        logger.info(f"Фиксированная сумма сделки: ${self.config['trade_params']['fixed_trade_amount']}")
        logger.info(f"Мартингейл: до {self.config['trade_params']['max_martingale_steps']} шагов")
        logger.info(f"Стоп-лосс/тейк-профит: {self.config['trade_params']['risk_percent']*100}% от суммы сделки")
        logger.info(f"Минимальный откат: {self.config['trade_params']['min_pullback_percent']}%")

    def get_trade_amount(self):
        """Фиксированная сумма с мартингейлом (без процента от баланса)"""
        base = self.config['trade_params']['fixed_trade_amount']
        step = self.consecutive_losses
        max_step = self.config['trade_params']['max_martingale_steps']
        if step > max_step:
            step = max_step
        multiplier = 2 ** step
        amount = base * multiplier
        logger.info(f"Шаг мартингейла: {step}, сумма сделки: {amount:.2f} USDT")
        return amount

    async def set_leverage(self, symbol, leverage, side):
        try:
            await self.exchange.set_leverage(leverage, symbol, params={'side': side})
            logger.info(f"Плечо {leverage}x для {symbol} ({side})")
        except Exception as e:
            logger.error(f"Ошибка установки плеча {symbol}: {e}")

    async def open_position(self, symbol, direction, price):
        if len(self.open_positions) >= self.config['max_positions']:
            logger.warning(f"Лимит позиций ({self.config['max_positions']}) достигнут, пропускаем {symbol}")
            return

        trade_amount = self.get_trade_amount()
        if trade_amount <= 0:
            logger.error("Сумма сделки слишком мала")
            return

        try:
            leverage = self.config['trade_params']['default_leverage']
            side = 'LONG' if direction == 'LONG' else 'SHORT'
            await self.set_leverage(symbol, leverage, side)

            quantity = round((trade_amount * leverage) / price, 5)
            if quantity <= 0:
                logger.error(f"Неверное количество {symbol}: {quantity}")
                return

            order_side = 'buy' if direction == 'LONG' else 'sell'
            await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=order_side,
                amount=quantity,
                params={'positionSide': side}
            )
            logger.info(f"🟢 ОТКРЫТА {direction} {symbol}: {quantity} по {price}, сумма {trade_amount:.2f} USDT, плечо {leverage}")

            risk_percent = self.config['trade_params']['risk_percent']
            if direction == 'LONG':
                stop_price = round(price * (1 - (1/leverage) * risk_percent), 5)
                take_price = round(price * (1 + (1/leverage) * risk_percent), 5)
            else:
                stop_price = round(price * (1 + (1/leverage) * risk_percent), 5)
                take_price = round(price * (1 - (1/leverage) * risk_percent), 5)

            self.open_positions.add(symbol)
            self.pos_data[symbol] = {
                'direction': direction,
                'entry_price': price,
                'quantity': quantity,
                'stop_price': stop_price,
                'take_price': take_price,
                'trade_amount': trade_amount,
                'leverage': leverage,
                'order_id': None
            }
            logger.info(f"Стоп-лосс: {stop_price:.5f} (изменение {abs(stop_price/price - 1)*100:.2f}%)")
            logger.info(f"Тейк-профит: {take_price:.5f} (изменение {abs(take_price/price - 1)*100:.2f}%)")

            if symbol in self.signal_state:
                self.signal_state[symbol]['waiting_for_pullback'] = False
        except Exception as e:
            logger.error(f"Ошибка открытия {symbol}: {e}")

    async def close_position(self, symbol, reason, current_price):
        pos = self.pos_data.get(symbol)
        if not pos:
            return
        try:
            close_side = 'sell' if pos['direction'] == 'LONG' else 'buy'
            await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=close_side,
                amount=pos['quantity'],
                params={'reduceOnly': True, 'positionSide': 'LONG' if pos['direction'] == 'LONG' else 'SHORT'}
            )
            logger.info(f"🔴 ЗАКРЫТА {symbol} по {reason}, цена {current_price}")
            if reason == 'stop_loss':
                self.consecutive_losses += 1
                logger.warning(f"Убытков подряд: {self.consecutive_losses}")
            else:
                self.consecutive_losses = 0
                logger.info(f"Тейк-профит, мартингейл сброшен")
            self.open_positions.discard(symbol)
            del self.pos_data[symbol]
        except Exception as e:
            logger.error(f"Ошибка закрытия {symbol}: {e}")

    async def monitor_positions(self):
        while True:
            for symbol, pos in list(self.pos_data.items()):
                try:
                    ticker = await self.exchange.fetch_ticker(symbol)
                    current_price = ticker['last']
                    should_close = False
                    reason = None
                    if pos['direction'] == 'LONG':
                        if current_price <= pos['stop_price']:
                            should_close = True
                            reason = 'stop_loss'
                        elif current_price >= pos['take_price']:
                            should_close = True
                            reason = 'take_profit'
                    else:
                        if current_price >= pos['stop_price']:
                            should_close = True
                            reason = 'stop_loss'
                        elif current_price <= pos['take_price']:
                            should_close = True
                            reason = 'take_profit'
                    if should_close:
                        await self.close_position(symbol, reason, current_price)
                except Exception as e:
                    logger.error(f"Ошибка мониторинга {symbol}: {e}")
            await asyncio.sleep(5)

    def calculate_heiken_ashi(self, df):
        df = df.copy()
        df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        ha_open = [df['open'].iloc[0]]
        for i in range(1, len(df)):
            ha_open.append((ha_open[i-1] + df['ha_close'].iloc[i-1]) / 2)
        df['ha_open'] = ha_open
        df['ha_high'] = df[['high', 'ha_open', 'ha_close']].max(axis=1)
        df['ha_low'] = df[['low', 'ha_open', 'ha_close']].min(axis=1)
        df['ha_color'] = df.apply(lambda row: 'green' if row['ha_close'] >= row['ha_open'] else 'red', axis=1)
        return df

    async def get_market_data(self, symbol, limit=50):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, self.config["timeframe"], limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logger.error(f"Ошибка данных {symbol}: {e}")
            return None

    async def process_symbol(self, symbol):
        if symbol in self.open_positions:
            return
        df = await self.get_market_data(symbol, limit=50)
        if df is None or len(df) < 20:
            return
        df = self.calculate_heiken_ashi(df)
        current_ts = df['timestamp'].iloc[-1]

        if symbol not in self.signal_state:
            self.signal_state[symbol] = {
                'last_candle_ts': None,
                'waiting_for_pullback': False,
                'signal_candle_close': None,
                'signal_direction': None
            }
        state = self.signal_state[symbol]

        if current_ts != state['last_candle_ts']:
            state['last_candle_ts'] = current_ts
            prev2 = df['ha_color'].iloc[-3]
            prev1 = df['ha_color'].iloc[-2]
            signal_candle = df.iloc[-2]
            if prev2 == 'red' and prev1 == 'green':
                state['waiting_for_pullback'] = True
                state['signal_direction'] = 'LONG'
                state['signal_candle_close'] = signal_candle['close']
                logger.info(f"{symbol}: сигнал LONG, ждём отката вниз")
            elif prev2 == 'green' and prev1 == 'red':
                state['waiting_for_pullback'] = True
                state['signal_direction'] = 'SHORT'
                state['signal_candle_close'] = signal_candle['close']
                logger.info(f"{symbol}: сигнал SHORT, ждём отката вверх")
            else:
                state['waiting_for_pullback'] = False
                state['signal_direction'] = None

        if state['waiting_for_pullback']:
            current_candle = df.iloc[-1]
            current_ha_open = df['ha_open'].iloc[-1]
            min_pullback = self.config['trade_params']['min_pullback_percent'] / 100.0
            if state['signal_direction'] == 'LONG':
                target_low = min(current_ha_open, state['signal_candle_close']) * (1 - min_pullback)
                if current_candle['low'] <= target_low:
                    await self.open_position(symbol, 'LONG', current_candle['close'])
                    state['waiting_for_pullback'] = False
            elif state['signal_direction'] == 'SHORT':
                target_high = max(current_ha_open, state['signal_candle_close']) * (1 + min_pullback)
                if current_candle['high'] >= target_high:
                    await self.open_position(symbol, 'SHORT', current_candle['close'])
                    state['waiting_for_pullback'] = False

    async def run(self):
        await self.load_markets()
        asyncio.create_task(self.monitor_positions())
        logger.info("✅ Бот запущен, ожидание сигналов...")
        while True:
            for symbol in self.all_symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.error(f"Ошибка {symbol}: {e}")
                await asyncio.sleep(0.5)
            await asyncio.sleep(60)

    async def close(self):
        await self.exchange.close()

async def main():
    config = load_config()
    bot = TradingBot(config)
    try:
        await bot.run()
    finally:
        await bot.close()

if __name__ == "__main__":
    asyncio.run(main())
