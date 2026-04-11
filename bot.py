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
        self.next_trade_doubled = False
        self.open_positions = set()
        self.pos_data = {}
        self.all_symbols = []
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
        all_swap = [s for s, m in self.exchange.markets.items() if m['swap'] and m['quote'] == 'USDT']
        self.all_symbols = all_swap[:50]
        logger.info(f"Загружено {len(self.all_symbols)} пар, таймфрейм {self.config['timeframe']}")

    def get_trade_amount(self):
        base = self.config['trade_params']['fixed_trade_amount']
        return base * 2 if self.next_trade_doubled else base

    async def check_higher_trend(self, symbol):
        higher_tf = self.config.get('higher_timeframe')
        if not higher_tf:
            return None
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, higher_tf, limit=3)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
            ha_open = [df['open'].iloc[0]]
            for i in range(1, len(df)):
                ha_open.append((ha_open[i-1] + df['ha_close'].iloc[i-1]) / 2)
            df['ha_open'] = ha_open
            last_color = 'green' if df['ha_close'].iloc[-1] >= df['ha_open'].iloc[-1] else 'red'
            return last_color
        except Exception as e:
            logger.error(f"Ошибка высшего ТФ {symbol}: {e}")
            return None

    async def check_atr(self, symbol):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, self.config['timeframe'], limit=20)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            high, low, close = df['high'], df['low'], df['close'].shift(1)
            tr = pd.concat([high - low, (high - close).abs(), (low - close).abs()], axis=1).max(axis=1)
            atr = tr.rolling(window=14).mean().iloc[-1]
            min_atr = self.config['trade_params'].get('min_atr', 0)
            max_atr = self.config['trade_params'].get('max_atr', 100)
            return min_atr <= atr <= max_atr
        except Exception as e:
            logger.error(f"Ошибка ATR {symbol}: {e}")
            return True

    async def check_volume(self, symbol, current_volume):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, self.config['timeframe'], limit=20)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            avg_volume = df['volume'].iloc[-10:-1].mean()
            return current_volume >= avg_volume * self.config['trade_params'].get('volume_multiplier', 1.0)
        except Exception as e:
            logger.error(f"Ошибка объёма {symbol}: {e}")
            return True

    async def open_position(self, symbol, direction, price, volume):
        if len(self.open_positions) >= self.config['max_positions']:
            logger.warning(f"Лимит позиций достигнут")
            return

        higher_trend = await self.check_higher_trend(symbol)
        if higher_trend:
            if direction == 'LONG' and higher_trend != 'green':
                logger.info(f"{symbol}: пропуск LONG, тренд 4H {higher_trend}")
                return
            if direction == 'SHORT' and higher_trend != 'red':
                logger.info(f"{symbol}: пропуск SHORT, тренд 4H {higher_trend}")
                return

        if not await self.check_atr(symbol):
            logger.info(f"{symbol}: ATR вне диапазона")
            return

        if not await self.check_volume(symbol, volume):
            logger.info(f"{symbol}: объём отката ниже среднего")
            return

        trade_amount = self.get_trade_amount()
        leverage = self.config['trade_params']['default_leverage']
        side = 'LONG' if direction == 'LONG' else 'SHORT'
        quantity = round((trade_amount * leverage) / price, 5)
        if quantity <= 0:
            return

        try:
            await self.exchange.set_leverage(leverage, symbol, params={'side': side})
            order_side = 'buy' if direction == 'LONG' else 'sell'
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

            self.open_positions.add(symbol)
            self.pos_data[symbol] = {
                'direction': direction,
                'entry_price': price,
                'quantity': quantity,
                'stop_price': stop_price,
                'take_price': take_price,
                'trade_amount': trade_amount,
                'leverage': leverage,
                'closed': False,
                'trailing_activated': False,
                'breakeven_stop': None
            }
            # После открытия сбрасываем флаг удвоения, чтобы следующая сделка была базовой
            self.next_trade_doubled = False

            balance = await self.get_balance()
            msg = (f"{'🟢' if direction=='LONG' else '🔴'} ОТКРЫТА СДЕЛКА {direction}\n"
                   f"Монета: {symbol}\nЦена: {price:.5f}\nСумма: {trade_amount:.2f} USDT\n"
                   f"SL: {stop_price:.5f} ({sl_percent*100:.0f}%), TP: {take_price:.5f} ({tp_percent*100:.0f}%)\n"
                   f"Баланс: {balance:.2f} USDT")
            await self.send_telegram(msg)

            if symbol in self.signal_state:
                self.signal_state[symbol]['waiting_for_pullback'] = False
        except Exception as e:
            logger.error(f"Ошибка открытия {symbol}: {e}")

    async def close_position(self, symbol, reason, current_price):
        pos = self.pos_data.get(symbol)
        if not pos or pos.get('closed'):
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
            self.pos_data[symbol]['closed'] = True

            balance = await self.get_balance()
            emoji = "🔴" if reason == 'stop_loss' else "🟢"
            msg = f"{emoji} СДЕЛКА ЗАКРЫТА\nМонета: {symbol}\nПричина: {reason}\nЦена: {current_price:.5f}\nБаланс: {balance:.2f} USDT"
            await self.send_telegram(msg)

            if reason == 'stop_loss':
                self.next_trade_doubled = True
                await self.send_telegram(f"⚠️ СТОП-ЛОСС\nСледующая сделка будет удвоена")
            else:
                self.next_trade_doubled = False

            self.open_positions.discard(symbol)
            asyncio.create_task(self.delayed_cleanup(symbol))
        except Exception as e:
            logger.error(f"Ошибка закрытия {symbol}: {e}")

    async def delayed_cleanup(self, symbol):
        await asyncio.sleep(10)
        if symbol in self.pos_data:
            del self.pos_data[symbol]

    async def monitor_positions(self):
        while True:
            for symbol, pos in list(self.pos_data.items()):
                if pos.get('closed'):
                    continue
                try:
                    ticker = await self.exchange.fetch_ticker(symbol)
                    current_price = ticker['last']
                    should_close = False
                    reason = None

                    # Трейлинг-стоп (безубыток после 50% TP)
                    tp_percent = self.config['trade_params']['tp_percent']
                    activation = self.config['trade_params'].get('trailing_stop_activation', 0.5)
                    if not pos.get('trailing_activated'):
                        if pos['direction'] == 'LONG':
                            profit_percent = (current_price - pos['entry_price']) / pos['entry_price']
                        else:
                            profit_percent = (pos['entry_price'] - current_price) / pos['entry_price']
                        if profit_percent >= tp_percent * activation:
                            pos['trailing_activated'] = True
                            pos['breakeven_stop'] = pos['entry_price']
                            logger.info(f"{symbol}: активирован безубыток на {pos['entry_price']}")

                    if pos.get('trailing_activated') and pos['breakeven_stop']:
                        if pos['direction'] == 'LONG' and current_price <= pos['breakeven_stop']:
                            should_close = True
                            reason = 'trailing_stop'
                        elif pos['direction'] == 'SHORT' and current_price >= pos['breakeven_stop']:
                            should_close = True
                            reason = 'trailing_stop'

                    if not should_close:
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
            await asyncio.sleep(2)

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
        if symbol in self.open_positions:
            return
        logger.info(f"🔍 Проверяю монету: {symbol}")
        df = await self.get_market_data(symbol, limit=50)
        if df is None or len(df) < 20:
            return
        df = self.calculate_heiken_ashi(df)
        current_ts = df['timestamp'].iloc[-1]

        if symbol not in self.signal_state:
            self.signal_state[symbol] = {
                'last_candle_ts': None,
                'waiting_for_pullback': False,
                'signal_candle_close': None,
                'signal_direction': None,
                'signal_volume': 0
            }
        state = self.signal_state[symbol]

        if current_ts != state['last_candle_ts']:
            state['last_candle_ts'] = current_ts
            prev2 = df['ha_color'].iloc[-3]
            prev1 = df['ha_color'].iloc[-2]
            signal_candle = df.iloc[-2]
            if prev2 == 'red' and prev1 == 'green':
                state['waiting_for_pullback'] = True
                state['signal_direction'] = 'LONG'
                state['signal_candle_close'] = signal_candle['close']
                state['signal_volume'] = signal_candle['volume']
                logger.info(f"{symbol}: сигнал LONG, ждём отката вниз")
            elif prev2 == 'green' and prev1 == 'red':
                state['waiting_for_pullback'] = True
                state['signal_direction'] = 'SHORT'
                state['signal_candle_close'] = signal_candle['close']
                state['signal_volume'] = signal_candle['volume']
                logger.info(f"{symbol}: сигнал SHORT, ждём отката вверх")
            else:
                state['waiting_for_pullback'] = False
                state['signal_direction'] = None

        if state['waiting_for_pullback']:
            current_candle = df.iloc[-1]
            current_ha_open = df['ha_open'].iloc[-1]
            min_pullback = self.config['trade_params']['min_pullback_percent'] / 100.0
            if state['signal_direction'] == 'LONG':
                target_low = min(current_ha_open, state['signal_candle_close']) * (1 - min_pullback)
                if current_candle['low'] <= target_low:
                    await self.open_position(symbol, 'LONG', current_candle['close'], current_candle['volume'])
                    state['waiting_for_pullback'] = False
            elif state['signal_direction'] == 'SHORT':
                target_high = max(current_ha_open, state['signal_candle_close']) * (1 + min_pullback)
                if current_candle['high'] >= target_high:
                    await self.open_position(symbol, 'SHORT', current_candle['close'], current_candle['volume'])
                    state['waiting_for_pullback'] = False

    async def run(self):
        await self.load_markets()
        asyncio.create_task(self.monitor_positions())
        balance = await self.get_balance()
        await self.send_telegram(f"🚀 Бот запущен (улучшенная стратегия)\nТаймфрейм: 30m\nФильтры: тренд 4H, ATR, объём\nОткат: {self.config['trade_params']['min_pullback_percent']}%\nБазовый лот: 7 USDT, мартингейл (удвоение после стоп-лосса)\nБаланс: {balance:.2f} USDT")
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
