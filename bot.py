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
        self.open_positions = {}      # активные позиции по символам
        self.martingale_step = 0      # глобальный шаг мартингейла (0,1,2,3)
        self.total_losses = 0         # количество последовательных убытков
        self.signal_sent = {}         # флаг отправки сигнала на текущей свече для каждого символа

    async def load_markets(self):
        await self.exchange.load_markets()
        # Получаем все фьючерсные USDT пары
        all_swap = [symbol for symbol, market in self.exchange.markets.items()
                    if market['swap'] and market['quote'] == 'USDT']
        self.all_symbols = all_swap[:50]  # ограничим 50 парами
        logger.info(f"Загружено {len(self.all_symbols)} фьючерсных пар")
        logger.info(f"Максимум открытых позиций: {self.config['max_positions']}")
        logger.info(f"Начальная сумма сделки: ${self.config['trade_params']['default_trade_amount']}")
        logger.info(f"Мартингейл: до {self.config['trade_params']['max_martingale_steps']} шагов")
        logger.info(f"Плечо: {self.config['trade_params']['default_leverage']}x")
        logger.info(f"Риск/прибыль: {self.config['trade_params']['risk_percent']*100}%")

    def get_trade_amount(self):
        """Глобальный размер сделки в зависимости от текущего шага мартингейла"""
        base = self.config['trade_params']['default_trade_amount']
        max_step = self.config['trade_params']['max_martingale_steps']
        if self.martingale_step >= max_step:
            return base
        return base * (2 ** self.martingale_step)

    def get_leverage(self):
        return self.config['trade_params']['default_leverage']

    async def set_leverage(self, symbol, leverage, position_side):
        try:
            await self.exchange.set_leverage(leverage, symbol, params={'positionSide': position_side})
            logger.info(f"Плечо {leverage}x для {symbol} ({position_side})")
        except Exception as e:
            logger.error(f"Ошибка установки плеча {symbol}: {e}")

    async def open_position(self, symbol, direction, price):
        if len(self.open_positions) >= self.config['max_positions']:
            logger.warning(f"Лимит позиций ({self.config['max_positions']}) достигнут. Позиция для {symbol} не открыта.")
            return

        try:
            leverage = self.get_leverage()
            trade_amount = self.get_trade_amount()
            position_side = 'LONG' if direction == 'LONG' else 'SHORT'
            await self.set_leverage(symbol, leverage, position_side)

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
                params={'positionSide': position_side}
            )
            logger.info(f"🟢 ОТКРЫТА {direction} {symbol}: {quantity} по {price}, сумма {trade_amount} USDT, плечо {leverage}")

            risk_percent = self.config['trade_params']['risk_percent']
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
            logger.error(f"Ошибка открытия {symbol}: {e}")

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
            logger.info(f"🔴 ЗАКРЫТА {symbol} по {reason}, цена {current_price}")
            # Обновляем глобальный мартингейл
            if reason == 'stop_loss':
                self.martingale_step += 1
                logger.info(f"Стоп-лосс! Глобальный шаг мартингейла = {self.martingale_step}")
            else:  # take_profit
                self.martingale_step = 0
                logger.info(f"Тейк-профит! Глобальный шаг мартингейла сброшен до 0")
            # Удаляем позицию из отслеживания
            del self.open_positions[symbol]
        except Exception as e:
            logger.error(f"Ошибка закрытия {symbol}: {e}")

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
        # Если уже есть открытая позиция по этому символу, не открываем новую
        if symbol in self.open_positions:
            return

        df = await self.get_market_data(symbol, limit=50)
        if df is None or len(df) < 20:
            return
        df = self.calculate_heiken_ashi(df)
        current_timestamp = df['timestamp'].iloc[-1]

        # Инициализируем флаг отправки сигнала для этого символа
        if symbol not in self.signal_sent:
            self.signal_sent[symbol] = {'last_timestamp': None, 'sent': False}

        state = self.signal_sent[symbol]

        # Если появилась новая свеча, сбрасываем флаг
        if current_timestamp != state['last_timestamp']:
            state['last_timestamp'] = current_timestamp
            state['sent'] = False

        if state['sent']:
            return

        # Проверяем сигнал на закрытых свечах
        prev2_color = df['ha_color'].iloc[-3]
        prev1_color = df['ha_color'].iloc[-2]
        current_candle = df.iloc[-1]
        current_ha_open = df['ha_open'].iloc[-1]

        # LONG: предыдущая красная, затем закрылась зелёная + откат вниз на текущей свече
        if prev2_color == 'red' and prev1_color == 'green':
            if current_candle['low'] < current_ha_open:
                await self.open_position(symbol, 'LONG', current_candle['close'])
                state['sent'] = True
        # SHORT: предыдущая зеленая, затем закрылась красная + откат вверх
        elif prev2_color == 'green' and prev1_color == 'red':
            if current_candle['high'] > current_ha_open:
                await self.open_position(symbol, 'SHORT', current_candle['close'])
                state['sent'] = True

    async def run(self):
        await self.load_markets()
        asyncio.create_task(self.monitor_positions())
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
