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
        self.telegram_bot = Bot(token=config["telegram_token"])
        self.positions = {}
        self.all_symbols = []
        self.blacklist = set()
        self.cooldown = {}
        self.last_heartbeat = datetime.now()
        self.cooldown_hours = self.config.get('cooldown_hours', 1)
        self.risk_per_trade = self.config.get('risk_per_trade_percent', 1.5)
        self.min_volume = self.config.get('min_volume_24h', 0)
        self.max_volatility = self.config.get('volatility_filter_percent', 100)
        self.dynamic_tp = self.config.get('dynamic_tp_enabled', False)
        self.dynamic_tp_step = self.config.get('dynamic_tp_step', 1.0)

        blacklist_from_config = self.config.get('blacklist_symbols', [])
        for sym in blacklist_from_config:
            self.blacklist.add(sym)
        logger.info(f"Загружено {len(blacklist_from_config)} символов в чёрный список")

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
                   f"Открытых позиций: {len(self.positions)}\n"
                   f"Баланс: {balance:.2f} USDT")
            await self.send_telegram(msg)
            self.last_heartbeat = now

    async def is_suitable_symbol(self, symbol):
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            volume_24h = ticker.get('quoteVolume', 0)
            if volume_24h < self.min_volume:
                logger.debug(f"{symbol}: объём {volume_24h} < {self.min_volume}, пропускаем")
                return False
            high = ticker['high']
            low = ticker['low']
            if low > 0:
                volatility = (high - low) / low * 100
                if volatility > self.max_volatility:
                    logger.debug(f"{symbol}: волатильность {volatility:.2f}% > {self.max_volatility}%, пропускаем")
                    return False
            return True
        except Exception as e:
            error_msg = str(e)
            if 'pause currently' in error_msg or 'not found' in error_msg or '109415' in error_msg:
                logger.warning(f"{symbol}: приостановлен или не найден, добавляем в чёрный список")
                self.blacklist.add(symbol)
            else:
                logger.error(f"Ошибка в is_suitable_symbol для {symbol}: {e}")
            return False

    async def load_markets(self):
        await self.exchange.load_markets()
        candidates = [symbol for symbol, market in self.exchange.markets.items()
                      if market['swap'] and market['quote'] == 'USDT' and
                      symbol.count('/') == 1 and not symbol.startswith(('NCFX', 'NCCO', 'NCSI', 'NCSK'))]
        logger.info(f"Найдено {len(candidates)} кандидатов. Применяем фильтры...")
        self.all_symbols = []
        for symbol in candidates:
            if symbol in self.blacklist:
                continue
            if await self.is_suitable_symbol(symbol):
                self.all_symbols.append(symbol)
        logger.info(f"После фильтрации осталось {len(self.all_symbols)} пар")

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

    async def get_market_data(self, symbol, limit=20):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, self.config['timeframe'], limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            if 'pause currently' in str(e) or 'not found' in str(e):
                self.blacklist.add(symbol)
            else:
                logger.error(f"Ошибка данных {symbol}: {e}")
            return None

    async def check_signal(self, symbol):
        df = await self.get_market_data(symbol, limit=10)
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

    async def get_min_amount(self, symbol):
        market = self.exchange.market(symbol)
        return market['limits']['amount']['min'] if 'limits' in market and 'amount' in market['limits'] else 0.0001

    async def get_position_size(self, symbol, price):
        balance = await self.get_balance()
        trade_amount = balance * self.risk_per_trade / 100
        trade_amount = max(trade_amount, 10.0)
        trade_amount = min(trade_amount, balance * 0.3)
        return trade_amount

    async def open_first_order(self, symbol, price, side):
        trade_amount = await self.get_position_size(symbol, price)
        order_side = 'buy' if side == 'LONG' else 'sell'
        try:
            quantity = trade_amount / price
            min_amount = await self.get_min_amount(symbol)
            if quantity < min_amount:
                logger.warning(f"{symbol}: количество {quantity} < {min_amount}, пропускаем")
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
            logger.info(f"🟢 ОТКРЫТ ПЕРВЫЙ ОРДЕР {side} {symbol}: {quantity} по {price}, сумма {trade_amount:.2f} USDT")
            pos = {
                'side': side,
                'orders': [{'price': price, 'amount': trade_amount, 'quantity': quantity}],
                'step': 1,
                'avg_price': price,
                'total_qty': quantity,
                'open_time': datetime.now()
            }
            tp_percent = self.config['trade_params']['tp_percent']
            if self.dynamic_tp:
                pos['tp_percent'] = tp_percent
            else:
                pos['tp_percent'] = tp_percent
            self.positions[symbol] = pos
            balance = await self.get_balance()
            msg = (f"🟢 ПЕРВЫЙ ОРДЕР {side}\n"
                   f"Монета: {symbol}\nЦена: {price:.5f}\nСумма: {trade_amount:.2f} USDT\n"
                   f"Баланс: {balance:.2f} USDT")
            await self.send_telegram(msg)
            return True
        except Exception as e:
            logger.error(f"Ошибка открытия первого ордера {symbol}: {e}")
            if 'minimum amount' in str(e).lower():
                self.blacklist.add(symbol)
            return False

    async def add_martingale_order(self, symbol, current_price):
        if symbol not in self.positions:
            return
        pos = self.positions[symbol]
        if pos['step'] >= self.config['trade_params']['max_steps']:
            return
        last_order = pos['orders'][-1]
        step_percent = self.config['trade_params']['step_percent']
        side = pos['side']
        if side == 'LONG':
            if current_price >= last_order['price'] * (1 - step_percent / 100):
                return
        else:
            if current_price <= last_order['price'] * (1 + step_percent / 100):
                return
        new_step = pos['step'] + 1
        multiplier = self.config['trade_params']['martingale_multiplier']
        prev_amount = last_order['amount']
        new_amount = prev_amount * multiplier
        order_side = 'buy' if side == 'LONG' else 'sell'
        try:
            quantity = new_amount / current_price
            min_amount = await self.get_min_amount(symbol)
            if quantity < min_amount:
                logger.warning(f"{symbol}: количество {quantity} < {min_amount}, не добавляем ордер")
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
            logger.info(f"🟢 ДОБАВЛЕН ОРДЕР {side} (шаг {new_step}): {quantity} по {current_price}, сумма {new_amount:.2f} USDT")
            pos['orders'].append({'price': current_price, 'amount': new_amount, 'quantity': quantity})
            pos['step'] = new_step
            total_qty = sum(o['quantity'] for o in pos['orders'])
            avg_price = sum(o['price'] * o['quantity'] for o in pos['orders']) / total_qty
            pos['avg_price'] = avg_price
            pos['total_qty'] = total_qty
            if self.dynamic_tp:
                pos['tp_percent'] = self.config['trade_params']['tp_percent'] + (new_step - 1) * self.dynamic_tp_step
                logger.info(f"{symbol}: TP увеличен до {pos['tp_percent']:.1f}%")
            balance = await self.get_balance()
            msg = (f"🟡 УСРЕДНЕНИЕ (шаг {new_step})\n"
                   f"Монета: {symbol}\nЦена: {current_price:.5f}\nСумма: {new_amount:.2f} USDT\n"
                   f"Средняя цена: {avg_price:.5f}\n"
                   f"Тейк-профит: {pos['tp_percent']:.1f}%\n"
                   f"Баланс: {balance:.2f} USDT")
            await self.send_telegram(msg)
        except Exception as e:
            logger.error(f"Ошибка добавления ордера {symbol}: {e}")

    async def check_take_profit(self, symbol, current_price):
        if symbol not in self.positions:
            return
        pos = self.positions[symbol]
        avg_price = pos['avg_price']
        tp_percent = pos['tp_percent']
        side = pos['side']
        if side == 'LONG':
            target_price = avg_price * (1 + tp_percent / 100)
            if current_price >= target_price:
                await self.close_all(symbol, current_price, 'take_profit')
        else:
            target_price = avg_price * (1 - tp_percent / 100)
            if current_price <= target_price:
                await self.close_all(symbol, current_price, 'take_profit')

    async def close_all(self, symbol, current_price, reason):
        if symbol not in self.positions:
            return
        pos = self.positions[symbol]
        total_qty = pos['total_qty']
        side = pos['side']
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
            del self.positions[symbol]
            if reason == 'take_profit':
                self.cooldown[symbol] = datetime.now() + timedelta(hours=self.cooldown_hours)
                logger.info(f"{symbol}: заблокирована на {self.cooldown_hours} час(ов) после тейк-профита")
                await self.send_telegram(f"🔒 {symbol}: блокировка на {self.cooldown_hours} час(ов) (тейк-профит)")
        except Exception as e:
            logger.error(f"Ошибка закрытия позиции {symbol}: {e}")

    async def monitor_position(self, symbol):
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
            for symbol in list(self.positions.keys()):
                await self.monitor_position(symbol)

            if len(self.positions) >= self.config['max_positions']:
                await asyncio.sleep(5)
                continue

            logger.info(f"🔄 Сканирование {len(self.all_symbols)} монет...")
            for symbol in self.all_symbols:
                if symbol in self.blacklist:
                    continue
                if symbol in self.positions:
                    continue
                if symbol in self.cooldown and datetime.now() < self.cooldown[symbol]:
                    continue
                try:
                    signal = await self.check_signal(symbol)
                    if signal is None:
                        continue
                    ticker = await self.exchange.fetch_ticker(symbol)
                    price = ticker['last']
                    await self.open_first_order(symbol, price, signal)
                    if len(self.positions) >= self.config['max_positions']:
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
            f"🚀 БОТ ЗАПУЩЕН (Heiken Ashi, таймфрейм {self.config['timeframe']})\n"
            f"Риск на сделку: {self.risk_per_trade}% от баланса\n"
            f"Макс. позиций: {self.config['max_positions']}\n"
            f"Множитель: {self.config['trade_params']['martingale_multiplier']}x\n"
            f"Шаг усреднения: {self.config['trade_params']['step_percent']}%\n"
            f"TP: {self.config['trade_params']['tp_percent']}% (динамический: {self.dynamic_tp})\n"
            f"Блокировка монеты после TP: {self.cooldown_hours} час(ов)\n"
            f"Фильтры: объём > {self.min_volume}, волатильность < {self.max_volatility}%\n"
            f"Чёрный список: {len(self.blacklist)} монет\n"
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
