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
        self.state = {}
        self.open_positions = {}

    async def load_markets_and_symbols(self):
        # Загружаем рынки
        await self.exchange.load_markets()
        logger.info("Рынки загружены")
        # Фильтруем только USDT-фьючерсы (swap)
        all_symbols = [s for s in self.exchange.markets if self.exchange.markets[s]['swap'] and s.endswith('/USDT:USDT')]
        # Если в конфиге есть список symbols, используем его, иначе все найденные
        if 'symbols' in self.config and self.config['symbols']:
            self.all_symbols = [s for s in self.config['symbols'] if s in all_symbols]
            logger.info(f"Используем указанные символы: {len(self.all_symbols)}")
        else:
            self.all_symbols = all_symbols
            logger.info(f"Загружено {len(self.all_symbols)} USDT-фьючерсов")
        logger.info(f"✅ Бот запущен на BingX")
        logger.info(f"Таймфрейм: {self.config['timeframe']}")
        logger.info(f"Сумма сделки: ${self.config['trade_params']['default_trade_amount']}")
        logger.info(f"Мартингейл: до {self.config['trade_params']['max_martingale_steps']} шагов")
        logger.info(f"Плечо: {self.config['trade_params']['default_leverage']}x")
        logger.info(f"Риск/прибыль: {self.config['trade_params']['risk_percent']*100}% от суммы сделки")

    def get_leverage(self, symbol):
        return self.config['trade_params']['default_leverage']

    def get_trade_amount(self, symbol):
        step = self.state.get(symbol, {}).get('martingale_step', 0)
        base = self.config['trade_params']['default_trade_amount']
        max_step = self.config['trade_params']['max_martingale_steps']
        if step >= max_step:
            return base
        return base * (2 ** step)

    async def set_leverage(self, symbol, leverage, position_side):
        try:
            await self.exchange.set_leverage(leverage, symbol, params={'positionSide': position_side})
            logger.info(f"Плечо {leverage}x для {symbol} ({position_side})")
        except Exception as e:
            logger.error(f"Ошибка установки плеча для {symbol}: {e}")

    async def open_position(self, symbol, direction, price):
        try:
            leverage = self.get_leverage(symbol)
            trade_amount = self.get_trade_amount(symbol)
            position_side = 'LONG' if direction == 'LONG' else 'SHORT'
            await self.set_leverage(symbol, leverage, position_side)

            quantity = round((trade_amount * leverage) / price, 5)
            if quantity <= 0:
                logger.error(f"Неверное количество для {symbol}: {quantity}")
                return

            order_side = 'buy' if direction == 'LONG' else 'sell'
            order = await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=order_side,
                amount=quantity,
                params={'positionSide': position_side}
            )
            logger.info(f"🟢 ОТКРЫТА {direction} {symbol}: {quantity} по {price}, сумма {trade_amount} USDT, плечо {leverage}")

            risk_percent = self.config['trade_params']['risk_percent']
            # Для плеча 20, риск 50% => изменение цены = (1/20)*0.5 = 2.5%
            if direction == 'LONG':
                stop_price = price * (1 - (1/leverage) * risk_percent)
                take_price = price * (1 + (1/leverage) * risk_percent)
            else:
                stop_price = price * (1 + (1/leverage) * risk_percent)
                take_price = price * (1 - (1/leverage) * risk_percent)

            self.open_positions[symbol] = {
                'direction': direction,
                'entry_price': price,
                'quantity': quantity,
                'trade_amount': trade_amount,
                'leverage': leverage,
                'stop_price': stop_price,
                'take_price': take_price,
                'timestamp': datetime.now()
            }
            logger.info(f"Стоп-лосс: {stop_price:.5f} (изменение {abs(stop_price/price - 1)*100:.2f}%)")
            logger.info(f"Тейк-профит: {take_price:.5f} (изменение {abs(take_price/price - 1)*100:.2f}%)")
        except Exception as e:
            logger.error(f"Ошибка открытия позиции для {symbol}: {e}")

    async def close_position(self, symbol, reason, current_price):
        pos = self.open_positions[symbol]
        try:
            close_side = 'sell' if pos['direction'] == 'LONG' else 'buy'
            await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=close_side,
                amount=pos['quantity']
            )
            logger.info(f"🔴 ЗАКРЫТА позиция {symbol} по {reason}, цена {current_price}")
            if reason == 'stop_loss':
                step = self.state.get(symbol, {}).get('martingale_step', 0) + 1
                self.state.setdefault(symbol, {})['martingale_step'] = step
                logger.info(f"{symbol}: стоп-лосс, шаг мартингейла = {step}")
            else:
                if symbol in self.state:
                    self.state[symbol]['martingale_step'] = 0
                logger.info(f"{symbol}: тейк-профит, мартингейл сброшен")
            del self.open_positions[symbol]
        except Exception as e:
            logger.error(f"Ошибка закрытия позиции {symbol}: {e}")

    async def monitor_positions(self):
        while True:
            for symbol, pos in list(self.open_positions.items()):
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
                    logger.error(f"Ошибка мониторинга позиции {symbol}: {e}")
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

    async def get_market_data(self, symbol, limit=30):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, self.config["timeframe"], limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logger.error(f"Ошибка данных для {symbol}: {e}")
            return None

    async def process_symbol(self, symbol):
        if symbol in self.open_positions:
            return
        df = await self.get_market_data(symbol, limit=30)
        if df is None or len(df) < 10:
            return
        df = self.calculate_heiken_ashi(df)
        current_timestamp = df['timestamp'].iloc[-1]

        if symbol not in self.state:
            self.state[symbol] = {'last_timestamp': None, 'signal_sent': False, 'martingale_step': 0}
        state = self.state[symbol]

        if current_timestamp != state['last_timestamp']:
            state['last_timestamp'] = current_timestamp
            state['signal_sent'] = False

        if state['signal_sent']:
            return

        prev2_color = df['ha_color'].iloc[-3]
        prev1_color = df['ha_color'].iloc[-2]
        current_candle = df.iloc[-1]
        current_ha_open = df['ha_open'].iloc[-1]

        if prev2_color == 'red' and prev1_color == 'green':
            if current_candle['low'] < current_ha_open:
                await self.open_position(symbol, 'LONG', current_candle['close'])
                state['signal_sent'] = True
        elif prev2_color == 'green' and prev1_color == 'red':
            if current_candle['high'] > current_ha_open:
                await self.open_position(symbol, 'SHORT', current_candle['close'])
                state['signal_sent'] = True

    async def run(self):
        await self.load_markets_and_symbols()
        asyncio.create_task(self.monitor_positions())
        while True:
            for symbol in self.all_symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.error(f"Ошибка при обработке {symbol}: {e}")
                await asyncio.sleep(1.5)
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
