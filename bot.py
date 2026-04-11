import asyncio
import logging
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
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
        self.all_symbols = []
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
        self.all_symbols = [symbol for symbol, market in self.exchange.markets.items()
                            if market['swap'] and market['quote'] == 'USDT']
        logger.info(f"Загружено {len(self.all_symbols)} фьючерсных пар")
        logger.info(f"Таймфрейм: {self.config['timeframe']}")

    def add_indicators(self, df):
        df['ema_50'] = df['close'].ewm(span=50, adjust=False).mean()
        df['ema_200'] = df['close'].ewm(span=200, adjust=False).mean()
        # MACD
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp1 - exp2
        df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['signal']
        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        # Средний объём
        df['avg_volume'] = df['volume'].rolling(window=10).mean()
        return df

    async def check_signal(self, symbol):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, self.config['timeframe'], limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = self.add_indicators(df)
            last = df.iloc[-1]
            prev = df.iloc[-2]

            # Проверка тренда и отката
            # LONG
            if (last['close'] > last['ema_50'] and last['close'] > last['ema_200'] and
                prev['close'] < prev['ema_50'] and last['close'] > last['ema_50'] and
                last['rsi'] < 50 and
                last['macd'] > last['signal'] and last['macd_hist'] > 0 and
                last['volume'] < last['avg_volume']):
                return 'LONG'
            # SHORT
            elif (last['close'] < last['ema_50'] and last['close'] < last['ema_200'] and
                  prev['close'] > prev['ema_50'] and last['close'] < last['ema_50'] and
                  last['rsi'] > 50 and
                  last['macd'] < last['signal'] and last['macd_hist'] < 0 and
                  last['volume'] < last['avg_volume']):
                return 'SHORT'
            return None
        except Exception as e:
            logger.error(f"Ошибка проверки сигнала {symbol}: {e}")
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

            sl_tp_percent = self.config['trade_params']['sl_tp_percent']
            if direction == 'LONG':
                stop_price = round(price * (1 - sl_tp_percent), 5)
                take_price = round(price * (1 + sl_tp_percent), 5)
            else:
                stop_price = round(price * (1 + sl_tp_percent), 5)
                take_price = round(price * (1 - sl_tp_percent), 5)

            self.open_positions[symbol] = {
                'direction': direction,
                'entry_price': price,
                'quantity': quantity,
                'entry_time': datetime.now(),
                'stop_price': stop_price,
                'take_price': take_price,
                'trailing_activated': False,
                'trailing_stop_price': None
            }

            balance = await self.get_balance()
            emoji = "🟢" if direction == 'LONG' else "🔴"
            msg = (f"{emoji} ОТКРЫТА СДЕЛКА {direction}\n"
                   f"Монета: {symbol}\nЦена: {price:.5f}\nСумма: {trade_amount:.2f} USDT\n"
                   f"Плечо: {leverage}x\nКол-во: {quantity:.5f}\n"
                   f"SL: {stop_price:.5f} ({(sl_tp_percent*100):.2f}%)\n"
                   f"TP: {take_price:.5f} ({(sl_tp_percent*100):.2f}%)\n"
                   f"Баланс: {balance:.2f} USDT")
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
                    hold_time = (datetime.now() - pos['entry_time']).total_seconds()
                    min_hold = self.config['trade_params']['min_hold_seconds']

                    if hold_time < min_hold:
                        continue

                    # Стоп-лосс и тейк-профит
                    if direction == 'LONG':
                        if current_price <= pos['stop_price']:
                            await self.close_position(symbol, 'stop_loss', current_price)
                            continue
                        elif current_price >= pos['take_price']:
                            await self.close_position(symbol, 'take_profit', current_price)
                            continue
                    else:
                        if current_price >= pos['stop_price']:
                            await self.close_position(symbol, 'stop_loss', current_price)
                            continue
                        elif current_price <= pos['take_price']:
                            await self.close_position(symbol, 'take_profit', current_price)
                            continue

                    # Трейлинг-стоп (активация при 50% от TP)
                    activation_percent = self.config['trade_params']['trailing_activation_percent']
                    profit_percent = (current_price - entry_price) / entry_price if direction == 'LONG' else (entry_price - current_price) / entry_price
                    if not pos['trailing_activated'] and profit_percent >= activation_percent:
                        pos['trailing_activated'] = True
                        # Устанавливаем стоп-лосс на уровне входа (безубыток) + небольшой запас 0.05% на комиссии
                        trailing_stop_price = entry_price * (1 + 0.0005) if direction == 'LONG' else entry_price * (1 - 0.0005)
                        pos['trailing_stop_price'] = trailing_stop_price
                        logger.info(f"{symbol}: трейлинг-стоп активирован на {trailing_stop_price:.5f}")
                        await self.send_telegram(f"🔒 {symbol}: трейлинг-стоп активирован, стоп на {trailing_stop_price:.5f}")

                    if pos['trailing_activated']:
                        if direction == 'LONG' and current_price <= pos['trailing_stop_price']:
                            await self.close_position(symbol, 'trailing_stop', current_price)
                        elif direction == 'SHORT' and current_price >= pos['trailing_stop_price']:
                            await self.close_position(symbol, 'trailing_stop', current_price)

                except Exception as e:
                    logger.error(f"Ошибка мониторинга {symbol}: {e}")

            await asyncio.sleep(5)  # сканирование каждые 5 секунд

    async def run(self):
        await self.load_markets()
        asyncio.create_task(self.monitor_positions())
        balance = await self.get_balance()
        await self.send_telegram(
            f"🚀 Бот запущен (BingX, трендовый откат)\n"
            f"Таймфрейм: {self.config['timeframe']}\n"
            f"Сумма сделки: {self.config['trade_params']['fixed_trade_amount']} USDT\n"
            f"Плечо: {self.config['trade_params']['default_leverage']}x\n"
            f"Макс. позиций: {self.config['max_positions']}\n"
            f"SL/TP: {self.config['trade_params']['sl_tp_percent']*100:.2f}%\n"
            f"Трейлинг-стоп активация: {self.config['trade_params']['trailing_activation_percent']*100:.2f}%\n"
            f"Баланс: {balance:.2f} USDT"
        )
        while True:
            for symbol in self.all_symbols:
                try:
                    signal = await self.check_signal(symbol)
                    if signal and symbol not in self.open_positions:
                        await self.open_position(symbol, signal)
                except Exception as e:
                    logger.error(f"Ошибка {symbol}: {e}")
                await asyncio.sleep(5)  # проверка сигналов каждые 5 секунд
            await asyncio.sleep(60)  # пауза между полными циклами

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
