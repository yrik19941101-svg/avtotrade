import asyncio
import logging
import ccxt.async_support as ccxt
import pandas as pd
import json
from datetime import datetime
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
        self.blacklist = set()
        self.last_heartbeat = datetime.now()
        self.timeframes = config["timeframes"]
        self.trade_amount = config['trade_params']['fixed_trade_amount']
        self.leverage = config['trade_params']['default_leverage']
        self.sl_percent = config['trade_params']['sl_percent']
        self.tp_percent = config['trade_params']['tp_percent']
        self.partial_close_ratio = config['trade_params']['partial_close_ratio']
        self.trailing_buffer = config['trade_params']['trailing_buffer']

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

    async def heartbeat(self):
        now = datetime.now()
        if (now - self.last_heartbeat).total_seconds() >= 900:
            balance = await self.get_balance()
            msg = (f"🟢 БОТ АКТИВЕН\n"
                   f"Время: {now.strftime('%H:%M:%S')}\n"
                   f"Открытых позиций: {len(self.open_positions)}\n"
                   f"Баланс: {balance:.2f} USDT")
            await self.send_telegram(msg)
            self.last_heartbeat = now

    async def load_markets(self):
        await self.exchange.load_markets()
        self.all_symbols = [symbol for symbol, market in self.exchange.markets.items()
                            if market['swap'] and market['quote'] == 'USDT' and
                            symbol.count('/') == 1 and not symbol.startswith(('NCFX', 'NCCO', 'NCSI', 'NCSK'))]
        logger.info(f"Загружено {len(self.all_symbols)} фьючерсных пар")

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

    async def get_market_data(self, symbol, timeframe, limit=20):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            if 'pause currently' in str(e) or 'not found' in str(e):
                self.blacklist.add(symbol)
            else:
                logger.error(f"Ошибка данных {symbol} {timeframe}: {e}")
            return None

    async def check_signal_on_timeframe(self, symbol, timeframe):
        """Возвращает 'LONG' или 'SHORT' если на данном ТФ активен сигнал, иначе None"""
        df = await self.get_market_data(symbol, timeframe, limit=10)
        if df is None or len(df) < 4:
            return None
        ha_df = self.calculate_heiken_ashi(df)
        prev_color = ha_df['ha_color'].iloc[-3]
        signal_color = ha_df['ha_color'].iloc[-2]
        current_candle = ha_df.iloc[-1]
        current_ha_open = current_candle['ha_open']

        if prev_color == 'red' and signal_color == 'green':
            if current_candle['low'] < current_ha_open:
                return 'LONG'
        elif prev_color == 'green' and signal_color == 'red':
            if current_candle['high'] > current_ha_open:
                return 'SHORT'
        return None

    async def check_signal_combined(self, symbol):
        """Проверяет сигналы на обоих ТФ. Возвращает направление для ВХОДА (противоположное)"""
        if symbol in self.blacklist:
            return None
        sig1 = await self.check_signal_on_timeframe(symbol, self.timeframes[0])
        if sig1 is None:
            return None
        sig2 = await self.check_signal_on_timeframe(symbol, self.timeframes[1])
        if sig2 is None:
            return None
        if sig1 != sig2:
            return None
        # Сигналы совпадают, открываем противоположную сделку
        opposite = 'SHORT' if sig1 == 'LONG' else 'LONG'
        logger.info(f"{symbol}: сигналы {sig1} на {self.timeframes[0]} и {sig2} на {self.timeframes[1]} -> вход {opposite}")
        return opposite

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

        trade_amount = self.trade_amount
        leverage = self.leverage
        side = 'LONG' if direction == 'LONG' else 'SHORT'
        order_side = 'buy' if direction == 'LONG' else 'sell'

        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            price = ticker['last']
            quantity = (trade_amount * leverage) / price
            quantity = round(quantity, 5)
            if quantity <= 0:
                logger.error(f"Неверное количество {symbol}: {quantity}")
                return

            await self.set_leverage(symbol, leverage, side)

            await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=order_side,
                amount=quantity,
                params={'positionSide': side}
            )
            logger.info(f"🟢 ОТКРЫТА {direction} {symbol}: {quantity} по {price}, сумма {trade_amount} USDT")

            sl_price = price * (1 - (1/leverage) * self.sl_percent) if direction == 'LONG' else price * (1 + (1/leverage) * self.sl_percent)
            tp_price = price * (1 + (1/leverage) * self.tp_percent) if direction == 'LONG' else price * (1 - (1/leverage) * self.tp_percent)
            partial_price = price + (tp_price - price) * self.partial_close_ratio if direction == 'LONG' else price - (price - tp_price) * self.partial_close_ratio

            self.open_positions[symbol] = {
                'direction': direction,
                'entry_price': price,
                'quantity': quantity,
                'sl_price': sl_price,
                'tp_price': tp_price,
                'partial_price': partial_price,
                'partial_closed': False,
                'trade_amount': trade_amount,
                'leverage': leverage,
            }

            balance = await self.get_balance()
            emoji = "🟢" if direction == 'LONG' else "🔴"
            msg = (f"{emoji} ОТКРЫТА СДЕЛКА {direction} (инверсия сигнала)\n"
                   f"Монета: {symbol}\nЦена: {price:.5f}\nСумма: {trade_amount:.2f} USDT\n"
                   f"Плечо: {leverage}x\nКол-во: {quantity:.5f}\n"
                   f"SL: {sl_price:.5f} ({self.sl_percent*100:.0f}%)\n"
                   f"TP: {tp_price:.5f} ({self.tp_percent*100:.0f}%)\n"
                   f"Частичное закрытие при {partial_price:.5f}\n"
                   f"Баланс: {balance:.2f} USDT")
            await self.send_telegram(msg)

        except Exception as e:
            logger.error(f"Ошибка открытия {symbol}: {e}")

    async def close_position(self, symbol, reason, current_price, close_quantity=None):
        pos = self.open_positions.get(symbol)
        if not pos:
            return
        try:
            close_side = 'sell' if pos['direction'] == 'LONG' else 'buy'
            side = 'LONG' if pos['direction'] == 'LONG' else 'SHORT'
            quantity = close_quantity if close_quantity is not None else pos['quantity']
            if quantity <= 0:
                return
            await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=close_side,
                amount=quantity,
                params={'positionSide': side}
            )
            logger.info(f"🔴 ЗАКРЫТА часть {quantity} {symbol} по {reason}, цена {current_price}")

            if close_quantity is not None and close_quantity < pos['quantity']:
                pos['quantity'] -= close_quantity
                pos['trade_amount'] = (pos['trade_amount'] / (pos['quantity'] + close_quantity)) * pos['quantity']
                new_sl = pos['entry_price'] * (1 + self.trailing_buffer) if pos['direction'] == 'LONG' else pos['entry_price'] * (1 - self.trailing_buffer)
                pos['sl_price'] = new_sl
                logger.info(f"{symbol}: трейлинг-стоп обновлён до безубытка {new_sl:.5f}")
                await self.send_telegram(f"🔒 {symbol}: частичное закрытие, стоп подтянут к безубытку")
            else:
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

                    if not pos.get('partial_closed'):
                        if direction == 'LONG' and current_price >= pos['partial_price']:
                            half_qty = pos['quantity'] * self.partial_close_ratio
                            await self.close_position(symbol, 'partial_take_profit', current_price, half_qty)
                            pos['partial_closed'] = True
                        elif direction == 'SHORT' and current_price <= pos['partial_price']:
                            half_qty = pos['quantity'] * self.partial_close_ratio
                            await self.close_position(symbol, 'partial_take_profit', current_price, half_qty)
                            pos['partial_closed'] = True

                    if direction == 'LONG' and current_price <= pos['sl_price']:
                        await self.close_position(symbol, 'stop_loss', current_price)
                    elif direction == 'SHORT' and current_price >= pos['sl_price']:
                        await self.close_position(symbol, 'stop_loss', current_price)
                    elif direction == 'LONG' and current_price >= pos['tp_price']:
                        await self.close_position(symbol, 'take_profit', current_price)
                    elif direction == 'SHORT' and current_price <= pos['tp_price']:
                        await self.close_position(symbol, 'take_profit', current_price)

                except Exception as e:
                    logger.error(f"Ошибка мониторинга {symbol}: {e}")
            await asyncio.sleep(5)

    async def scan_symbols(self):
        while True:
            await self.heartbeat()
            logger.info(f"🔄 Начинаю сканирование {len(self.all_symbols)} монет...")
            for symbol in self.all_symbols:
                if symbol in self.open_positions or symbol in self.blacklist:
                    continue
                try:
                    signal = await self.check_signal_combined(symbol)
                    if signal:
                        await self.open_position(symbol, signal)
                except Exception as e:
                    logger.error(f"Ошибка сканирования {symbol}: {e}")
                await asyncio.sleep(0.5)
            logger.info(f"✅ Цикл сканирования завершён. Следующий через 30 секунд.")
            await asyncio.sleep(30)

    async def run(self):
        await self.load_markets()
        asyncio.create_task(self.monitor_positions())
        asyncio.create_task(self.scan_symbols())
        balance = await self.get_balance()
        await self.send_telegram(
            f"🚀 Бот запущен (инвертированная стратегия: сигналы 1h+15m совпадают -> вход противоположный)\n"
            f"Сумма сделки: {self.trade_amount} USDT\n"
            f"Плечо: {self.leverage}x\n"
            f"SL: {self.sl_percent*100:.0f}% от суммы, TP: {self.tp_percent*100:.0f}%\n"
            f"Частичное закрытие 50% при 50% TP, затем безубыток\n"
            f"Макс. позиций: {self.config['max_positions']}\n"
            f"Баланс: {balance:.2f} USDT"
        )
        while True:
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
