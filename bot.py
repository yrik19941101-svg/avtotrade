import asyncio
import logging
import ccxt.async_support as ccxt
import pandas as pd
import json
from datetime import datetime, timedelta
from telegram import Bot

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
        self.open_positions = {}
        self.all_symbols = self.config['symbols']
        self.signal_state = {}
        self.telegram_bot = Bot(token=config["telegram_token"])

    async def get_balance(self):
        try:
            balance = await self.exchange.fetch_balance()
            return balance['USDT']['free']
        except Exception as e:
            logger.error(f"Ошибка баланса: {e}")
            return 0.0

    async def send_telegram(self, message):
        try:
            await self.telegram_bot.send_message(chat_id=self.config["telegram_chat_id"], text=message, parse_mode=None)
        except Exception as e:
            logger.error(f"Ошибка Telegram: {e}")

    async def load_markets(self):
        await self.exchange.load_markets()
        valid = [s for s in self.all_symbols if s in self.exchange.markets]
        self.all_symbols = valid
        logger.info(f"Загружено {len(self.all_symbols)} доступных пар")
        logger.info(f"Таймфрейм: {self.config['timeframe']}")

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

    async def set_leverage(self, symbol, leverage, side):
        try:
            await self.exchange.set_leverage(leverage, symbol, params={'side': side})
            logger.info(f"Плечо {leverage}x для {symbol} ({side})")
        except Exception as e:
            logger.error(f"Ошибка установки плеча {symbol}: {e}")

    async def open_position(self, symbol, direction):
        if len(self.open_positions) >= self.config['max_positions']:
            logger.warning(f"Лимит позиций ({self.config['max_positions']}) достигнут")
            return

        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            price = ticker['last']
            trade_amount = self.config['trade_params']['fixed_trade_amount']
            leverage = self.config['trade_params']['default_leverage']
            side = 'LONG' if direction == 'LONG' else 'SHORT'
            order_side = 'buy' if direction == 'LONG' else 'sell'

            await self.set_leverage(symbol, leverage, side)

            quantity = (trade_amount * leverage) / price
            quantity = round(quantity, 5)
            if quantity <= 0:
                logger.error(f"Неверное количество {symbol}: {quantity}")
                return

            order = await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=order_side,
                amount=quantity,
                params={'positionSide': side}
            )
            logger.info(f"🟢 ОТКРЫТА {direction} {symbol}: {quantity} по {price}, сумма {trade_amount} USDT")

            self.open_positions[symbol] = {
                'direction': direction,
                'entry_price': price,
                'quantity': quantity,
                'entry_time': datetime.now(),
                'trailing_stop_price': None,
                'trailing_activated': False
            }

            balance = await self.get_balance()
            emoji = "🟢" if direction == 'LONG' else "🔴"
            msg = (f"{emoji} ОТКРЫТА СДЕЛКА {direction}\n"
                   f"Монета: {symbol}\nЦена: {price:.5f}\nСумма: {trade_amount:.2f} USDT\n"
                   f"Плечо: {leverage}x\nКол-во: {quantity:.5f}\nБаланс: {balance:.2f} USDT")
            await self.send_telegram(msg)

        except Exception as e:
            logger.error(f"Ошибка открытия {symbol}: {e}")

    async def close_position(self, symbol, reason, current_price=None):
        pos = self.open_positions.get(symbol)
        if not pos:
            return

        try:
            if current_price is None:
                ticker = await self.exchange.fetch_ticker(symbol)
                current_price = ticker['last']

            close_side = 'sell' if pos['direction'] == 'LONG' else 'buy'
            side = 'LONG' if pos['direction'] == 'LONG' else 'SHORT'
            await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=close_side,
                amount=pos['quantity'],
                params={'positionSide': side}
            )
            logger.info(f"🔴 ЗАКРЫТА {symbol} по {reason}, цена {current_price}")
            del self.open_positions[symbol]

            balance = await self.get_balance()
            emoji = "🔴" if reason == 'stop_loss' else "🟢"
            msg = f"{emoji} СДЕЛКА ЗАКРЫТА\nМонета: {symbol}\nПричина: {reason}\nЦена: {current_price:.5f}\nБаланс: {balance:.2f} USDT"
            await self.send_telegram(msg)

        except Exception as e:
            logger.error(f"Ошибка закрытия {symbol}: {e}")

    async def monitor_positions(self):
        while True:
            for symbol, pos in list(self.open_positions.items()):
                try:
                    ticker = await self.exchange.fetch_ticker(symbol)
                    current_price = ticker['last']
                    direction = pos['direction']
                    entry_price = pos['entry_price']

                    # Проверка противоположного сигнала
                    df = await self.get_market_data(symbol, limit=5)
                    if df is not None:
                        ha_df = self.calculate_heiken_ashi(df)
                        current_color = ha_df['ha_color'].iloc[-1]
                        if direction == 'LONG' and current_color == 'red':
                            await self.close_position(symbol, 'opposite_signal', current_price)
                            continue
                        elif direction == 'SHORT' and current_color == 'green':
                            await self.close_position(symbol, 'opposite_signal', current_price)
                            continue

                    # Трейлинг-стоп
                    profit_percent = (current_price - entry_price) / entry_price if direction == 'LONG' else (entry_price - current_price) / entry_price
                    activation_percent = self.config['trade_params']['trailing_activation_percent'] / 100

                    if not pos['trailing_activated'] and profit_percent >= activation_percent:
                        pos['trailing_activated'] = True
                        # Устанавливаем стоп-лосс на небольшой плюс (компенсируем комиссии)
                        pos['trailing_stop_price'] = entry_price * (1 + 0.0005) if direction == 'LONG' else entry_price * (1 - 0.0005)
                        logger.info(f"{symbol}: трейлинг-стоп активирован на {pos['trailing_stop_price']:.5f}")

                    if pos['trailing_activated']:
                        if direction == 'LONG' and current_price <= pos['trailing_stop_price']:
                            await self.close_position(symbol, 'trailing_stop', current_price)
                        elif direction == 'SHORT' and current_price >= pos['trailing_stop_price']:
                            await self.close_position(symbol, 'trailing_stop', current_price)

                except Exception as e:
                    logger.error(f"Ошибка мониторинга {symbol}: {e}")

            await asyncio.sleep(5)

    async def process_symbol(self, symbol):
        if symbol in self.open_positions:
            return

        df = await self.get_market_data(symbol, limit=5)
        if df is None or len(df) < 3:
            return

        ha_df = self.calculate_heiken_ashi(df)
        prev_color = ha_df['ha_color'].iloc[-3]
        signal_color = ha_df['ha_color'].iloc[-2]
        current_color = ha_df['ha_color'].iloc[-1]

        # Проверка времени для входа
        current_time = datetime.now()
        # Получаем время начала свечи из DataFrame
        candle_open_time = df['timestamp'].iloc[-1]
        candle_close_time = candle_open_time + timedelta(minutes=30)
        minutes_to_close = (candle_close_time - current_time).total_seconds() / 60

        if minutes_to_close < self.config['trade_params']['min_minutes_to_close']:
            return

        # Сигнал на LONG
        if prev_color == 'red' and signal_color == 'green' and current_color == 'red':
            await self.open_position(symbol, 'LONG')
        # Сигнал на SHORT
        elif prev_color == 'green' and signal_color == 'red' and current_color == 'green':
            await self.open_position(symbol, 'SHORT')

    async def run(self):
        await self.load_markets()
        asyncio.create_task(self.monitor_positions())
        balance = await self.get_balance()
        await self.send_telegram(
            f"🚀 Бот запущен (BingX, стратегия Heiken Ashi)\n"
            f"Таймфрейм: {self.config['timeframe']}\n"
            f"Сумма сделки: {self.config['trade_params']['fixed_trade_amount']} USDT\n"
            f"Плечо: {self.config['trade_params']['default_leverage']}x\n"
            f"Макс. позиций: {self.config['max_positions']}\n"
            f"Баланс: {balance:.2f} USDT"
        )
        while True:
            for symbol in self.all_symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.error(f"Ошибка {symbol}: {e}")
                await asyncio.sleep(10)
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
