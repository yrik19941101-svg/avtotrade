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
        self.telegram_bot = Bot(token=config["telegram_token"])
        self.position = None
        self.all_symbols = []
        self.blacklist = set()
        self.last_heartbeat = datetime.now()

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
                   f"Позиция: {'есть' if self.position else 'нет'}\n"
                   f"Баланс: {balance:.2f} USDT")
            await self.send_telegram(msg)
            self.last_heartbeat = now

    async def load_markets(self):
        await self.exchange.load_markets()
        self.all_symbols = [symbol for symbol, market in self.exchange.markets.items()
                            if market['swap'] and market['quote'] == 'USDT' and
                            symbol.count('/') == 1 and not symbol.startswith(('NCFX', 'NCCO', 'NCSI', 'NCSK'))]
        logger.info(f"Загружено {len(self.all_symbols)} фьючерсных пар")

    async def get_ema(self, symbol, timeframe, period, limit=150):
        df = await self.get_market_data(symbol, timeframe, limit)
        if df is None or len(df) < period:
            return None, None
        ema = df['close'].ewm(span=period, adjust=False).mean()
        return ema.iloc[-1], df['close'].iloc[-1]

    async def get_market_data(self, symbol, timeframe, limit=150):
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

    async def check_trend(self, symbol):
        ema50_1h, close_1h = await self.get_ema(symbol, self.config['trend_tf'], 50)
        ema50_4h, close_4h = await self.get_ema(symbol, self.config['trend_tf2'], 50)
        if ema50_1h is None or ema50_4h is None:
            return None
        long_condition = (close_1h > ema50_1h) and (close_4h > ema50_4h)
        short_condition = (close_1h < ema50_1h) and (close_4h < ema50_4h)
        if long_condition:
            return 'LONG'
        if short_condition:
            return 'SHORT'
        return None

    async def get_min_amount(self, symbol):
        """Возвращает минимальное количество для символа (из market['limits']['amount']['min'])"""
        market = self.exchange.market(symbol)
        return market['limits']['amount']['min'] if 'limits' in market and 'amount' in market['limits'] else 0.0001

    async def open_first_order(self, symbol, price, side):
        trade_amount = self.config['trade_params']['base_amount']
        order_side = 'buy' if side == 'LONG' else 'sell'
        try:
            quantity = trade_amount / price
            min_amount = await self.get_min_amount(symbol)
            if quantity < min_amount:
                logger.warning(f"{symbol}: количество {quantity} меньше минимального {min_amount}, пропускаем")
                self.blacklist.add(symbol)
                return False
            quantity = round(quantity, 5)
            if quantity <= 0:
                return False

            order = await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=order_side,
                amount=quantity,
                params={'positionSide': side}
            )
            logger.info(f"🟢 ОТКРЫТ ПЕРВЫЙ ОРДЕР {side} {symbol}: {quantity} по {price}, сумма {trade_amount} USDT")
            self.position = {
                'symbol': symbol,
                'side': side,
                'orders': [{'price': price, 'amount': trade_amount, 'quantity': quantity}],
                'step': 1,
                'avg_price': price,
                'total_qty': quantity
            }
            balance = await self.get_balance()
            msg = (f"🟢 ПЕРВЫЙ ОРДЕР {side}\n"
                   f"Монета: {symbol}\nЦена: {price:.5f}\nСумма: {trade_amount:.2f} USDT\n"
                   f"Баланс: {balance:.2f} USDT")
            await self.send_telegram(msg)
            return True
        except Exception as e:
            logger.error(f"Ошибка открытия первого ордера {symbol}: {e}")
            # Если ошибка связана с минимальным количеством, добавляем в чёрный список
            if 'minimum amount' in str(e).lower():
                self.blacklist.add(symbol)
            return False

    async def add_martingale_order(self, symbol, current_price):
        if not self.position or self.position['symbol'] != symbol:
            return
        if self.position['step'] >= self.config['trade_params']['max_steps']:
            return

        last_order = self.position['orders'][-1]
        step_percent = self.config['trade_params']['step_percent']
        side = self.position['side']

        if side == 'LONG':
            if current_price >= last_order['price'] * (1 - step_percent / 100):
                return
        else:
            if current_price <= last_order['price'] * (1 + step_percent / 100):
                return

        new_step = self.position['step'] + 1
        multiplier = self.config['trade_params']['martingale_multiplier']
        prev_amount = last_order['amount']
        new_amount = prev_amount * multiplier
        order_side = 'buy' if side == 'LONG' else 'sell'

        try:
            quantity = new_amount / current_price
            min_amount = await self.get_min_amount(symbol)
            if quantity < min_amount:
                logger.warning(f"{symbol}: количество {quantity} меньше минимального {min_amount}, не добавляем ордер")
                return
            quantity = round(quantity, 5)
            if quantity <= 0:
                return

            order = await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=order_side,
                amount=quantity,
                params={'positionSide': side}
            )
            logger.info(f"🟢 ДОБАВЛЕН ОРДЕР {side} (шаг {new_step}): {quantity} по {current_price}, сумма {new_amount} USDT")
            self.position['orders'].append({'price': current_price, 'amount': new_amount, 'quantity': quantity})
            self.position['step'] = new_step
            total_qty = sum(o['quantity'] for o in self.position['orders'])
            avg_price = sum(o['price'] * o['quantity'] for o in self.position['orders']) / total_qty
            self.position['avg_price'] = avg_price
            self.position['total_qty'] = total_qty

            balance = await self.get_balance()
            msg = (f"🟡 УСРЕДНЕНИЕ (шаг {new_step})\n"
                   f"Монета: {symbol}\nЦена: {current_price:.5f}\nСумма: {new_amount:.2f} USDT\n"
                   f"Средняя цена: {avg_price:.5f}\nБаланс: {balance:.2f} USDT")
            await self.send_telegram(msg)
        except Exception as e:
            logger.error(f"Ошибка добавления ордера {symbol}: {e}")

    async def check_take_profit(self, symbol, current_price):
        if not self.position or self.position['symbol'] != symbol:
            return
        avg_price = self.position['avg_price']
        tp_percent = self.config['trade_params']['tp_percent']
        side = self.position['side']
        if side == 'LONG':
            target_price = avg_price * (1 + tp_percent / 100)
            if current_price >= target_price:
                await self.close_all(symbol, current_price, 'take_profit')
        else:
            target_price = avg_price * (1 - tp_percent / 100)
            if current_price <= target_price:
                await self.close_all(symbol, current_price, 'take_profit')

    async def close_all(self, symbol, current_price, reason):
        if not self.position or self.position['symbol'] != symbol:
            return
        total_qty = self.position['total_qty']
        side = self.position['side']
        close_side = 'sell' if side == 'LONG' else 'buy'
        try:
            await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=close_side,
                amount=total_qty,
                params={'positionSide': side}
            )
            logger.info(f"🔴 ЗАКРЫТА ВСЯ ПОЗИЦИЯ {symbol} по {reason}, цена {current_price}")
            balance = await self.get_balance()
            msg = (f"🔴 ПОЗИЦИЯ ЗАКРЫТА\n"
                   f"Монета: {symbol}\nПричина: {reason}\n"
                   f"Цена закрытия: {current_price:.5f}\nБаланс: {balance:.2f} USDT")
            await self.send_telegram(msg)
            self.position = None
        except Exception as e:
            logger.error(f"Ошибка закрытия позиции {symbol}: {e}")

    async def monitor_position(self):
        if not self.position:
            return
        symbol = self.position['symbol']
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            current_price = ticker['last']
            await self.add_martingale_order(symbol, current_price)
            await self.check_take_profit(symbol, current_price)
        except Exception as e:
            logger.error(f"Ошибка мониторинга позиции {symbol}: {e}")

    async def scan_symbols(self):
        while True:
            await self.heartbeat()
            if self.position:
                await self.monitor_position()
                await asyncio.sleep(5)
                continue

            logger.info(f"🔄 Сканирование {len(self.all_symbols)} монет...")
            for symbol in self.all_symbols:
                if symbol in self.blacklist:
                    continue
                try:
                    trend = await self.check_trend(symbol)
                    if trend is None:
                        continue
                    ticker = await self.exchange.fetch_ticker(symbol)
                    price = ticker['last']
                    await self.open_first_order(symbol, price, trend)
                    break
                except Exception as e:
                    logger.error(f"Ошибка сканирования {symbol}: {e}")
                await asyncio.sleep(0.5)
            await asyncio.sleep(10)

    async def run(self):
        await self.load_markets()
        asyncio.create_task(self.scan_symbols())
        balance = await self.get_balance()
        await self.send_telegram(
            f"🚀 БОТ ЗАПУЩЕН (LONG/SHORT, мартингейл до 5 шагов)\n"
            f"Сумма: {self.config['trade_params']['base_amount']} USDT\n"
            f"Множитель: {self.config['trade_params']['martingale_multiplier']}x\n"
            f"Шаг усреднения: {self.config['trade_params']['step_percent']}%\n"
            f"TP: {self.config['trade_params']['tp_percent']}%\n"
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
