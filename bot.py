import asyncio
import logging
import ccxt.async_support as ccxt
import pandas as pd
import json
import csv
import os
from datetime import datetime, timedelta
from telegram import Bot

CONFIG_FILE = "config.json"
STATS_FILE = "trades.csv"

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
        self.signal_block = {}
        self.cooldown_hours = self.config.get('cooldown_hours', 3)
        self.min_volume = self.config.get('min_volume_24h', 50000)
        self.max_volatility = self.config.get('volatility_filter_percent', 5)

        # Фиксированная сумма сделки (без мартингейла)
        self.fixed_trade_amount = self.config.get('fixed_trade_amount', 600.0)

        # Статистика
        self.stats = self.load_stats()
        self.total_pnl = self.stats.get('total_pnl', 0.0)
        self.total_trades = self.stats.get('total_trades', 0)
        self.winning_trades = self.stats.get('winning_trades', 0)
        self.losing_trades = self.stats.get('losing_trades', 0)
        self.max_consecutive_losses = self.stats.get('max_consecutive_losses', 0)
        self.current_loss_streak = 0

        blacklist_from_config = self.config.get('blacklist_symbols', [])
        for sym in blacklist_from_config:
            self.blacklist.add(sym)
        logger.info(f"Загружено {len(blacklist_from_config)} символов в чёрный список")

    def load_stats(self):
        if not os.path.exists(STATS_FILE):
            return {
                'total_pnl': 0.0,
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'max_consecutive_losses': 0
            }
        df = pd.read_csv(STATS_FILE)
        if df.empty:
            return {
                'total_pnl': 0.0,
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'max_consecutive_losses': 0
            }
        total_pnl = df['pnl'].sum()
        total_trades = len(df)
        winning_trades = len(df[df['pnl'] > 0])
        losing_trades = len(df[df['pnl'] < 0])
        max_streak = 0
        current = 0
        for pnl in df['pnl']:
            if pnl < 0:
                current += 1
                if current > max_streak:
                    max_streak = current
            else:
                current = 0
        return {
            'total_pnl': total_pnl,
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'max_consecutive_losses': max_streak
        }

    def save_trade(self, symbol, side, entry_price, exit_price, pnl, reason):
        with open(STATS_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            if os.path.getsize(STATS_FILE) == 0:
                writer.writerow(['timestamp', 'symbol', 'side', 'entry_price', 'exit_price', 'pnl', 'reason'])
            writer.writerow([datetime.now().isoformat(), symbol, side, entry_price, exit_price, pnl, reason])

        self.total_trades += 1
        self.total_pnl += pnl
        if pnl > 0:
            self.winning_trades += 1
            self.current_loss_streak = 0
        else:
            self.losing_trades += 1
            self.current_loss_streak += 1
            if self.current_loss_streak > self.max_consecutive_losses:
                self.max_consecutive_losses = self.current_loss_streak

    async def send_stats(self):
        balance = await self.get_balance()
        winrate = (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0
        msg = (f"📊 СТАТИСТИКА\n"
               f"Баланс: {balance:.2f} USDT\n"
               f"Всего сделок: {self.total_trades}\n"
               f"Прибыльных: {self.winning_trades}\n"
               f"Убыточных: {self.losing_trades}\n"
               f"Winrate: {winrate:.1f}%\n"
               f"Общая P&L: {self.total_pnl:.2f} USDT\n"
               f"Макс. серия убытков: {self.max_consecutive_losses}")
        await self.send_telegram(msg)

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

    async def is_suitable_symbol(self, symbol):
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            volume_24h = ticker.get('quoteVolume', 0)
            if volume_24h < self.min_volume:
                return False
            high = ticker.get('high', 0)
            low = ticker.get('low', 0)
            if low > 0:
                volatility = (high - low) / low * 100
                if volatility > self.max_volatility:
                    return False
            return True
        except Exception as e:
            if 'pause currently' in str(e) or 'not found' in str(e):
                self.blacklist.add(symbol)
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

    def period_hours(self, timeframe):
        mapping = {'1m': 1/60, '3m': 3/60, '5m': 5/60, '15m': 15/60, '1h': 1,
                   '4h': 4, '6h': 6, '12h': 12, '1d': 24}
        return mapping.get(timeframe, 6)

    def is_mid_candle(self, df, timeframe, snooze_percent=0.3):
        if len(df) < 1:
            return False
        now = pd.Timestamp.now('UTC').tz_localize(None)
        last_ts = df['timestamp'].iloc[-1]
        if last_ts.tzinfo is not None:
            last_ts = last_ts.tz_localize(None)
        freq_hours = self.period_hours(timeframe)
        elapsed = (now - last_ts).total_seconds() / 3600
        remaining = freq_hours - elapsed
        half = freq_hours / 2
        return remaining > half * snooze_percent

    def count_consecutive_ha(self, ha_df, color):
        arr = ha_df['ha_color'].values
        cnt = 0
        for i in range(len(arr)-3, -1, -1):
            if arr[i] == color:
                cnt += 1
            else:
                break
        return cnt

    async def check_signal(self, symbol):
        timeframe = self.config['timeframe']
        df = await self.get_market_data(symbol, limit=30)
        if df is None or len(df) < 6:
            return None
        if not self.is_mid_candle(df, timeframe):
            return None

        ha_df = self.calculate_heiken_ashi(df)
        if len(ha_df) < 4:
            return None

        sig = ha_df.iloc[-2]
        pull = ha_df.iloc[-1]

        sig_color = sig['ha_color']
        sig_ha_close = sig['ha_close']
        pull_low = pull['low']
        pull_high = pull['high']
        min_pullback = self.config.get('signal_params', {}).get('min_pullback_percent', 0.5) / 100.0

        if sig_color == 'green':
            red_cnt = self.count_consecutive_ha(ha_df, 'red')
            if red_cnt >= 3:
                if pull_low <= sig_ha_close * (1 - min_pullback) and pull_high < sig_ha_close:
                    return 'LONG'
        elif sig_color == 'red':
            green_cnt = self.count_consecutive_ha(ha_df, 'green')
            if green_cnt >= 3:
                if pull_high >= sig_ha_close * (1 + min_pullback) and pull_low > sig_ha_close:
                    return 'SHORT'
        return None

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

    async def get_min_amount(self, symbol):
        market = self.exchange.market(symbol)
        return market['limits']['amount']['min'] if 'limits' in market and 'amount' in market['limits'] else 0.0001

    def get_trade_amount(self):
        return self.fixed_trade_amount

    async def open_position(self, symbol, price, side):
        trade_amount = self.get_trade_amount()
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
            logger.info(f"🟢 ОТКРЫТА {side} {symbol}: {quantity} по {price}, сумма {trade_amount:.2f} USDT")

            sl_percent = self.config['trade_params'].get('sl_percent', 2.0) / 100.0
            tp_percent = self.config['trade_params'].get('tp_percent', 2.0) / 100.0
            if side == 'LONG':
                stop_price = price * (1 - sl_percent)
                take_price = price * (1 + tp_percent)
            else:
                stop_price = price * (1 + sl_percent)
                take_price = price * (1 - tp_percent)

            self.positions[symbol] = {
                'side': side,
                'entry_price': price,
                'quantity': quantity,
                'stop_price': stop_price,
                'take_price': take_price,
                'trade_amount': trade_amount,
                'open_time': datetime.now()
            }

            balance = await self.get_balance()
            msg = (f"🟢 ОТКРЫТА СДЕЛКА {side}\n"
                   f"Монета: {symbol}\nЦена: {price:.5f}\nСумма: {trade_amount:.2f} USDT\n"
                   f"SL: {stop_price:.5f} ({sl_percent*100:.1f}%)\n"
                   f"TP: {take_price:.5f} ({tp_percent*100:.1f}%)\n"
                   f"Баланс: {balance:.2f} USDT")
            await self.send_telegram(msg)
            return True
        except Exception as e:
            logger.error(f"Ошибка открытия позиции {symbol}: {e}")
            if 'minimum amount' in str(e).lower():
                self.blacklist.add(symbol)
            return False

    async def close_position(self, symbol, reason, current_price):
        if symbol not in self.positions:
            return
        pos = self.positions[symbol]
        try:
            close_side = 'sell' if pos['side'] == 'LONG' else 'buy'
            await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=close_side,
                amount=pos['quantity'],
                params={'positionSide': pos['side']}
            )
            logger.info(f"🔴 ЗАКРЫТА {symbol} по {reason}, цена {current_price}")

            if pos['side'] == 'LONG':
                pnl = (current_price - pos['entry_price']) * pos['quantity']
            else:
                pnl = (pos['entry_price'] - current_price) * pos['quantity']

            self.save_trade(symbol, pos['side'], pos['entry_price'], current_price, pnl, reason)

            # Мартингейла нет – ничего не обновляем

            del self.positions[symbol]

            balance = await self.get_balance()
            emoji = "🔴" if reason == 'stop_loss' else "🟢"
            msg = f"{emoji} СДЕЛКА ЗАКРЫТА\nМонета: {symbol}\nПричина: {reason}\nЦена: {current_price:.5f}\nP&L: {pnl:.2f} USDT\nБаланс: {balance:.2f} USDT"
            await self.send_telegram(msg)

            if reason == 'take_profit':
                self.cooldown[symbol] = datetime.now() + timedelta(hours=self.cooldown_hours)
                logger.info(f"{symbol}: заблокирована на {self.cooldown_hours} час(ов) после тейк-профита")
                await self.send_telegram(f"🔒 {symbol}: блокировка на {self.cooldown_hours} час(ов) (тейк-профит)")

            await self.send_stats()
        except Exception as e:
            logger.error(f"Ошибка закрытия {symbol}: {e}")

    async def monitor_position(self, symbol):
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            current_price = ticker['last']
            pos = self.positions.get(symbol)
            if not pos:
                return
            if pos['side'] == 'LONG':
                if current_price <= pos['stop_price']:
                    await self.close_position(symbol, 'stop_loss', current_price)
                elif current_price >= pos['take_price']:
                    await self.close_position(symbol, 'take_profit', current_price)
            else:
                if current_price >= pos['stop_price']:
                    await self.close_position(symbol, 'stop_loss', current_price)
                elif current_price <= pos['take_price']:
                    await self.close_position(symbol, 'take_profit', current_price)
        except Exception as e:
            logger.error(f"Ошибка мониторинга позиции {symbol}: {e}")

    async def scan_symbols(self):
        while True:
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
                if symbol in self.signal_block and datetime.now() < self.signal_block[symbol]:
                    continue
                try:
                    signal = await self.check_signal(symbol)
                    if signal is None:
                        continue
                    self.signal_block[symbol] = datetime.now() + timedelta(minutes=5)
                    ticker = await self.exchange.fetch_ticker(symbol)
                    price = ticker['last']
                    await self.open_position(symbol, price, signal)
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
            f"🚀 ТОРГОВЫЙ БОТ ЗАПУЩЕН (Heiken Ashi, таймфрейм {self.config['timeframe']})\n"
            f"Фиксированная сумма сделки: {self.fixed_trade_amount} USDT (без мартингейла)\n"
            f"SL/TP: {self.config['trade_params'].get('sl_percent', 2.0)}% / {self.config['trade_params'].get('tp_percent', 2.0)}%\n"
            f"Макс. позиций: {self.config['max_positions']}\n"
            f"Баланс: {balance:.2f} USDT"
        )
        await self.send_stats()
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
