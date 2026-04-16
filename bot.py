import asyncio
import logging
import ccxt.async_support as ccxt
import pandas as pd
import json
from datetime import datetime, timedelta
from telegram import Bot
import os

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
        self.signal_block = {}
        self.consecutive_losses = 0
        self.base_trade_amount = config.get('base_trade_amount', 100.0)
        self.martingale_multiplier = config.get('martingale_multiplier', 2.0)
        self.max_martingale_steps = config.get('max_martingale_steps', 3)
        self.min_volume = config.get('min_volume_24h', 50000)
        self.max_volatility = config.get('volatility_filter_percent', 5)
        self.cooldown_hours = config.get('cooldown_hours', 3)

        # Статистика
        self.stats = {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'total_pnl': 0.0,
            'max_drawdown': 0.0,
            'current_drawdown': 0.0,
            'peak_balance': 0.0,
            'history': []   # список словарей с деталями каждой сделки
        }
        self.start_balance = 0.0

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

    async def update_stats(self, trade_result):
        """Обновляет статистику после закрытия сделки"""
        self.stats['total_trades'] += 1
        pnl = trade_result['pnl']
        self.stats['total_pnl'] += pnl
        if pnl > 0:
            self.stats['winning_trades'] += 1
        else:
            self.stats['losing_trades'] += 1
        self.stats['history'].append(trade_result)

        # Текущая просадка
        current_balance = await self.get_balance()
        if current_balance > self.stats['peak_balance']:
            self.stats['peak_balance'] = current_balance
        drawdown = (self.stats['peak_balance'] - current_balance) / self.stats['peak_balance'] * 100 if self.stats['peak_balance'] > 0 else 0
        self.stats['current_drawdown'] = drawdown
        if drawdown > self.stats['max_drawdown']:
            self.stats['max_drawdown'] = drawdown

        # Сохраняем статистику в файл
        with open('statistics.json', 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=4, ensure_ascii=False)

    async def send_stats_report(self):
        """Отправляет сводку статистики в Telegram"""
        win_rate = (self.stats['winning_trades'] / self.stats['total_trades'] * 100) if self.stats['total_trades'] > 0 else 0
        avg_win = 0
        avg_loss = 0
        if self.stats['winning_trades'] > 0:
            avg_win = sum(t['pnl'] for t in self.stats['history'] if t['pnl'] > 0) / self.stats['winning_trades']
        if self.stats['losing_trades'] > 0:
            avg_loss = sum(t['pnl'] for t in self.stats['history'] if t['pnl'] < 0) / self.stats['losing_trades']
        msg = (f"📊 СТАТИСТИКА ТОРГОВЛИ\n"
               f"Всего сделок: {self.stats['total_trades']}\n"
               f"Прибыльных: {self.stats['winning_trades']} ({win_rate:.1f}%)\n"
               f"Убыточных: {self.stats['losing_trades']}\n"
               f"Общий PnL: {self.stats['total_pnl']:.2f} USDT\n"
               f"Средняя прибыль: {avg_win:.2f}\n"
               f"Средний убыток: {avg_loss:.2f}\n"
               f"Макс. просадка: {self.stats['max_drawdown']:.2f}%\n"
               f"Текущая серия убытков: {self.consecutive_losses}")
        await self.send_telegram(msg)

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
        if self.consecutive_losses >= self.max_martingale_steps:
            return self.base_trade_amount
        return self.base_trade_amount * (self.martingale_multiplier ** self.consecutive_losses)

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
            # Расчёт PnL
            if pos['side'] == 'LONG':
                pnl = (current_price - pos['entry_price']) * pos['quantity']
            else:
                pnl = (pos['entry_price'] - current_price) * pos['quantity']
            pnl_percent = (pnl / pos['trade_amount']) * 100

            trade_record = {
                'symbol': symbol,
                'direction': pos['side'],
                'entry_price': pos['entry_price'],
                'exit_price': current_price,
                'amount': pos['trade_amount'],
                'pnl': pnl,
                'pnl_percent': pnl_percent,
                'exit_reason': reason,
                'time': datetime.now().isoformat()
            }
            await self.update_stats(trade_record)

            logger.info(f"🔴 ЗАКРЫТА {symbol} по {reason}, цена {current_price}, PnL: {pnl:.2f} USDT")
            del self.positions[symbol]

            if reason == 'stop_loss':
                self.consecutive_losses += 1
                logger.info(f"Стоп-лосс, серия убытков: {self.consecutive_losses}")
            else:
                self.consecutive_losses = 0
                logger.info(f"Тейк-профит, мартингейл сброшен")

            if self.consecutive_losses > self.max_martingale_steps:
                self.consecutive_losses = 0

            balance = await self.get_balance()
            emoji = "🔴" if reason == 'stop_loss' else "🟢"
            msg = f"{emoji} СДЕЛКА ЗАКРЫТА\nМонета: {symbol}\nПричина: {reason}\nЦена: {current_price:.5f}\nPnL: {pnl:.2f} USDT ({pnl_percent:.1f}%)\nБаланс: {balance:.2f} USDT"
            await self.send_telegram(msg)

            if reason == 'take_profit':
                self.cooldown[symbol] = datetime.now() + timedelta(hours=self.cooldown_hours)
                logger.info(f"{symbol}: заблокирована на {self.cooldown_hours} час(ов) после тейк-профита")
                await self.send_telegram(f"🔒 {symbol}: блокировка на {self.cooldown_hours} час(ов) (тейк-профит)")

            # Периодически отправляем статистику (каждые 10 сделок)
            if self.stats['total_trades'] % 10 == 0 and self.stats['total_trades'] > 0:
                await self.send_stats_report()

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
            # Мониторим открытые позиции
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
        # Сохраняем начальный баланс
        self.start_balance = await self.get_balance()
        self.stats['peak_balance'] = self.start_balance
        await self.load_markets()
        asyncio.create_task(self.scan_symbols())
        balance = await self.get_balance()
        await self.send_telegram(
            f"🚀 ТОРГОВЫЙ БОТ ЗАПУЩЕН (Heiken Ashi, таймфрейм {self.config['timeframe']})\n"
            f"Базовая сумма сделки: {self.base_trade_amount} USDT\n"
            f"Мартингейл: {self.martingale_multiplier}x, до {self.max_martingale_steps} шагов\n"
            f"SL/TP: {self.config['trade_params'].get('sl_percent', 2.0)}% / {self.config['trade_params'].get('tp_percent', 2.0)}%\n"
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
