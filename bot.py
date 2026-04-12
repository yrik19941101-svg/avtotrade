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
        self.global_loss_streak = 0
        self.telegram_bot = Bot(token=config["telegram_token"])
        self.timeframes = config['timeframes']
        self.main_tf = config['main_timeframe']
        self.heartbeat_minutes = config.get('heartbeat_minutes', 15)
        self.heartbeat_to_telegram = config.get('heartbeat_to_telegram', True)
        self.last_heartbeat = datetime.now()

    def timeframe_to_minutes(self, tf):
        if tf.endswith('h'):
            return int(tf[:-1]) * 60
        elif tf.endswith('m'):
            return int(tf[:-1])
        else:
            return 60

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
        if (now - self.last_heartbeat).total_seconds() >= self.heartbeat_minutes * 60:
            balance = await self.get_balance()
            msg = (f"🟢 БОТ АКТИВЕН\n"
                   f"Время: {now.strftime('%H:%M:%S')}\n"
                   f"Открытых позиций: {len(self.open_positions)}\n"
                   f"Баланс: {balance:.2f} USDT\n"
                   f"Серия убытков: {self.global_loss_streak}\n"
                   f"Проверено монет: {len(self.all_symbols)}")
            if self.heartbeat_to_telegram:
                await self.send_telegram(msg)
            else:
                logger.info(msg)
            self.last_heartbeat = now

    async def load_markets(self):
        await self.exchange.load_markets()
        self.all_symbols = [symbol for symbol, market in self.exchange.markets.items()
                            if market['swap'] and market['quote'] == 'USDT' and
                            symbol.count('/') == 1 and not symbol.startswith(('NCFX', 'NCCO', 'NCSI', 'NCSK'))]
        logger.info(f"Загружено {len(self.all_symbols)} фьючерсных пар (крипто)")

    def get_trade_amount(self):
        base = self.config['trade_params']['fixed_trade_amount']
        if self.global_loss_streak == 0:
            return base
        elif self.global_loss_streak == 1:
            return base * 2
        else:
            return base

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

    async def get_market_data(self, symbol, timeframe, limit=10):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logger.error(f"Ошибка данных {symbol} {timeframe}: {e}")
            return None

    async def check_signal_on_timeframe(self, symbol, timeframe):
        df = await self.get_market_data(symbol, timeframe, limit=10)
        if df is None or len(df) < 3:
            return None
        ha_df = self.calculate_heiken_ashi(df)
        prev2 = ha_df['ha_color'].iloc[-3]
        prev1 = ha_df['ha_color'].iloc[-2]
        current_color = ha_df['ha_color'].iloc[-1]

        # Временной фильтр УБРАН – вход возможен в любой момент свечи
        # LONG: prev2 red, prev1 green, current red
        if prev2 == 'red' and prev1 == 'green' and current_color == 'red':
            return 'LONG'
        # SHORT: prev2 green, prev1 red, current green
        elif prev2 == 'green' and prev1 == 'red' and current_color == 'green':
            return 'SHORT'
        return None

    async def check_signal_all_timeframes(self, symbol):
        signals = []
        for tf in self.timeframes:
            sig = await self.check_signal_on_timeframe(symbol, tf)
            if sig is None:
                return None
            signals.append(sig)
        if all(s == 'LONG' for s in signals):
            return 'LONG'
        elif all(s == 'SHORT' for s in signals):
            return 'SHORT'
        else:
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

        trade_amount = self.get_trade_amount()
        leverage = self.config['trade_params']['default_leverage']
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

            sl_percent = self.config['trade_params']['sl_percent']
            tp_percent = self.config['trade_params']['tp_percent']
            if direction == 'LONG':
                stop_price = round(price * (1 - (1/leverage) * sl_percent), 5)
                take_price = round(price * (1 + (1/leverage) * tp_percent), 5)
            else:
                stop_price = round(price * (1 + (1/leverage) * sl_percent), 5)
                take_price = round(price * (1 - (1/leverage) * tp_percent), 5)

            # Пытаемся выставить лимитные ордера TP/SL (если не получится – мониторинг)
            try:
                await self.exchange.create_order(
                    symbol=symbol,
                    type='stop_market',
                    side='sell' if direction == 'LONG' else 'buy',
                    amount=quantity,
                    params={'stopPrice': stop_price, 'positionSide': side, 'reduceOnly': True}
                )
                await self.exchange.create_order(
                    symbol=symbol,
                    type='take_profit_market',
                    side='sell' if direction == 'LONG' else 'buy',
                    amount=quantity,
                    params={'stopPrice': take_price, 'positionSide': side, 'reduceOnly': True}
                )
                logger.info(f"✅ Установлены лимитные ордера SL={stop_price}, TP={take_price}")
                sl_tp_set = True
            except Exception as e:
                logger.warning(f"Не удалось установить лимитные ордера: {e}, буду использовать мониторинг")
                sl_tp_set = False

            self.open_positions[symbol] = {
                'direction': direction,
                'entry_price': price,
                'quantity': quantity,
                'entry_time': datetime.now(),
                'stop_price': stop_price,
                'take_price': take_price,
                'trade_amount': trade_amount,
                'leverage': leverage,
                'sl_tp_set': sl_tp_set
            }

            balance = await self.get_balance()
            emoji = "🟢" if direction == 'LONG' else "🔴"
            msg = (f"{emoji} ОТКРЫТА СДЕЛКА {direction}\n"
                   f"Монета: {symbol}\nЦена: {price:.5f}\nСумма: {trade_amount:.2f} USDT\n"
                   f"Плечо: {leverage}x\nКол-во: {quantity:.5f}\n"
                   f"SL: {stop_price:.5f} ({sl_percent*100:.0f}%)\n"
                   f"TP: {take_price:.5f} ({tp_percent*100:.0f}%)\n"
                   f"Баланс: {balance:.2f} USDT")
            await self.send_telegram(msg)

        except Exception as e:
            logger.error(f"Ошибка открытия {symbol}: {e}")

    async def close_position_by_monitoring(self, symbol, reason, current_price):
        pos = self.open_positions.get(symbol)
        if not pos:
            return
        try:
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

            if reason == 'stop_loss':
                self.global_loss_streak += 1
                if self.global_loss_streak > self.config['trade_params']['martingale_steps']:
                    self.global_loss_streak = 0
                logger.info(f"Стоп-лосс, серия убытков: {self.global_loss_streak}")
            else:
                self.global_loss_streak = 0
                logger.info(f"Тейк-профит, мартингейл сброшен")

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
                    if not pos.get('sl_tp_set'):
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        if pos['direction'] == 'LONG':
                            if current_price <= pos['stop_price']:
                                await self.close_position_by_monitoring(symbol, 'stop_loss', current_price)
                            elif current_price >= pos['take_price']:
                                await self.close_position_by_monitoring(symbol, 'take_profit', current_price)
                        else:
                            if current_price >= pos['stop_price']:
                                await self.close_position_by_monitoring(symbol, 'stop_loss', current_price)
                            elif current_price <= pos['take_price']:
                                await self.close_position_by_monitoring(symbol, 'take_profit', current_price)
                except Exception as e:
                    logger.error(f"Ошибка мониторинга {symbol}: {e}")
            await asyncio.sleep(5)

    async def scan_symbols(self):
        while True:
            await self.heartbeat()
            logger.info(f"🔄 Начинаю сканирование {len(self.all_symbols)} монет...")
            for symbol in self.all_symbols:
                if symbol in self.open_positions:
                    continue
                try:
                    signal = await self.check_signal_all_timeframes(symbol)
                    if signal:
                        await self.open_position(symbol, signal)
                except Exception as e:
                    logger.error(f"Ошибка сканирования {symbol}: {e}")
                await asyncio.sleep(0.5)
            logger.info(f"✅ Цикл сканирования завершён. Следующий через 10 секунд.")
            await asyncio.sleep(10)

    async def run(self):
        await self.load_markets()
        asyncio.create_task(self.monitor_positions())
        asyncio.create_task(self.scan_symbols())
        balance = await self.get_balance()
        await self.send_telegram(
            f"🚀 Мульти-ТФ бот запущен (без временного фильтра)\n"
            f"Таймфреймы: {', '.join(self.timeframes)}\n"
            f"Сумма сделки: {self.config['trade_params']['fixed_trade_amount']} USDT (мартингейл 2 колена)\n"
            f"Плечо: {self.config['trade_params']['default_leverage']}x\n"
            f"SL: {self.config['trade_params']['sl_percent']*100:.0f}%, TP: {self.config['trade_params']['tp_percent']*100:.0f}%\n"
            f"Макс. позиций: {self.config['max_positions']}\n"
            f"Баланс: {balance:.2f} USDT\n"
            f"Heartbeat: каждые {self.heartbeat_minutes} мин"
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
