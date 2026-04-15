import asyncio
import logging
import json
import ccxt.async_support as ccxt
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

# Проверяем, установлен ли telegram для уведомлений
TELEGRAM_ENABLED = True
try:
    from telegram import Bot
except ImportError:
    TELEGRAM_ENABLED = False
    Bot = None


CONFIG_FILE = "config.json"

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SignalBot:
    def __init__(self, config):
        self.config = config
        self.exchange = getattr(ccxt, config["exchange"])({
            'enableRateLimit': True,
            'apiKey': config.get('api_key', ''),
            'secret': config.get('api_secret', ''),
            'options': {
                'defaultType': 'swap',
                'adjustForTimeDifference': True
            }
        })

        self.telegram_bot = None
        if TELEGRAM_ENABLED:
            self.telegram_bot = Bot(token=config["telegram_token"])

        self.all_symbols = []
        self.blacklist = set()
        self.sent_signals: Dict[str, Dict[str, Any]] = {}

        blacklist_from_config = self.config.get('blacklist_symbols', [])
        for sym in blacklist_from_config:
            self.blacklist.add(sym)
        logger.info(f"Загружено {len(blacklist_from_config)} символов в чёрный список")

    async def send_telegram_signal(self, symbol, signal_type, timeframe, price, reason=""):
        if not self.telegram_bot:
            return
        msg = (f"🎯 ТОЧНЫЙ СИГНАЛ {signal_type} ({timeframe})\n"
               f"Монета: {symbol}\n"
               f"Цена входа: {price:.5f}\n"
               f"Причина: {reason}\n"
               f"Время: {datetime.now().strftime('%H:%M:%S')}")

        for i in range(self.config.get("telegram_retry_count", 3)):
            try:
                chat_id = self.config.get("telegram_chat_id")
                if not chat_id:
                    break
                await self.telegram_bot.send_message(
                    chat_id=chat_id,
                    text=msg
                )
                return
            except Exception as e:
                if i < self.config.get("telegram_retry_count", 3) - 1:
                    logger.warning(f"Повторная попытка Telegram {i+1}: {e}")
                    await asyncio.sleep(self.config.get("telegram_retry_delay", 5))
                else:
                    logger.error(f"Ошибка Telegram после попыток: {e}")

    async def get_market_data(self, symbol, timeframe, limit=20):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            return df
        except Exception as e:
            if 'pause currently' not in str(e) and 'not found' not in str(e):
                logger.error(f"Ошибка данных {symbol} ({timeframe}): {e}")
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
        df['ha_color'] = df.apply(
            lambda row: 'green' if row['ha_close'] >= row['ha_open'] else 'red',
            axis=1
        )
        return df

    async def fetch_all_tickers(self):
        try:
            tickers = await self.exchange.fetch_tickers()
            return tickers
        except Exception as e:
            logger.error(f"Ошибка fetch_tickers: {e}")
            return {}

    async def is_suitable_symbol(self, symbol, tickers):
        try:
            ticker = tickers.get(symbol)
            if not ticker:
                return False
            volume_24h = ticker.get('quoteVolume', 0)
            if volume_24h < self.config.get('min_volume_24h', 0):
                return False
            high = ticker.get('high', 0)
            low = ticker.get('low', 0)
            if low > 0:
                volatility = (high - low) / low * 100
                if volatility > self.config.get('volatility_filter_percent', 100):
                    return False
            return True
        except Exception as e:
            logger.error(f"Ошибка в is_suitable_symbol для {symbol}: {e}")
            return False

    async def load_market_list(self):
        await self.exchange.load_markets()
        candidates = [
            symbol
            for symbol, market in self.exchange.markets.items()
            if market['swap']
               and market['quote'] == 'USDT'
               and '/' in market['symbol']
               and not market['symbol'].startswith(('NCFX', 'NCCO', 'NCSI', 'NCSK'))
        ]
        logger.info(f"Найдено {len(candidates)} кандидатов для сигнального бота")

        tickers = await self.fetch_all_tickers()
        self.all_symbols = []
        for symbol in candidates:
            if symbol in self.blacklist:
                continue
            if await self.is_suitable_symbol(symbol, tickers):
                self.all_symbols.append(symbol)
        logger.info(f"Осталось {len(self.all_symbols)} монет после фильтра")

    def period_hours(self, timeframe):
        mapping = {'1m': 1/60, '3m': 3/60, '5m': 5/60, '15m': 15/60, '1h': 1,
                   '4h': 4, '6h': 6, '12h': 12, '1d': 24}
        return mapping.get(timeframe, 6)

    def is_mid_candle(self, df, timeframe, snooze_percent=0.3):
        if len(df) < 1:
            return False
        now = pd.Timestamp.utcnow()
        last_ts = df['timestamp'].iloc[-1]
        freq_hours = self.period_hours(timeframe)
        elapsed = (now - last_ts).total_seconds() / 3600
        remaining = freq_hours - elapsed
        half = freq_hours / 2
        return remaining > half * snooze_percent

    def min_pullback_percent(self):
        return self.config['signal_params'].get('min_pullback_percent', 0.2)

    async def generate_signal(self, symbol, timeframe, limit=20):
        df = await self.get_market_data(symbol, timeframe, limit=limit)
        if df is None or len(df) < 3:
            return None
        if not self.is_mid_candle(df, timeframe):
            return None

        ha_df = self.calculate_heiken_ashi(df)
        if len(ha_df) < 3:
            return None

        # Сигнальная свеча (предпоследняя)
        sig = ha_df.iloc[-2]
        sig_color = sig['ha_color']
        sig_ha_close = sig['ha_close']
        sig_range = sig['high'] - sig['low']

        # Откатная свеча (последняя, ещё не закрыта)
        pull = ha_df.iloc[-1]
        pull_low = pull['low']
        pull_high = pull['high']

        signal_type = None
        reason = ""
        price = None

        # LONG: после 3+ красных → зелёная → откат вниз ~30% от закрытия зелёной
        if sig_color == 'green':
            level_down = sig_ha_close * 0.7  # 30% вниз от закрытия сигнальной
            if pull_low <= sig_ha_close and pull_low >= level_down:
                pullback_percent = (sig_ha_close - pull_low) / sig_ha_close * 100
                if pullback_percent >= self.min_pullback_percent():
                    signal_type = 'LONG'
                    price = sig_ha_close
                    reason = f"Heiken Ashi LONG: после 3+ красных свечей → зелёная, откат вниз 30% от закрытия, откат {pullback_percent:.2f}%"

        # SHORT: после 3+ зелёных → красная → откат вверх ~30% от закрытия красной
        elif sig_color == 'red':
            level_up = sig_ha_close * 1.3  # 30% вверх от закрытия сигнальной
            if pull_high >= sig_ha_close and pull_high <= level_up:
                pullback_percent = (pull_high - sig_ha_close) / sig_ha_close * 100
                if pullback_percent >= self.min_pullback_percent():
                    signal_type = 'SHORT'
                    price = sig_ha_close
                    reason = f"Heiken Ashi SHORT: после 3+ зелёных свечей → красная, откат вверх 30% от закрытия, откат {pullback_percent:.2f}%"

        if signal_type and price:
            return {
                'type': signal_type,
                'symbol': symbol,
                'timeframe': timeframe,
                'price': price,
                'reason': reason
            }

        return None

    async def scan_for_signals(self):
        await self.load_market_list()
        timeframes = self.config.get('timeframes', ['6h'])

        while True:
            logger.info(f"🔄 Сканирую {len(self.all_symbols)} монет по таймфреймам: {timeframes}")
            for symbol in self.all_symbols:
                if symbol in self.blacklist:
                    continue
                for tf in timeframes:
                    key = f"{symbol}_{tf}"
                    try:
                        signal = await self.generate_signal(symbol, tf)
                        if not signal:
                            continue

                        last_signal = self.sent_signals.get(key)
                        if last_signal and last_signal['type'] == signal['type']:
                            time_diff = (datetime.utcnow() - last_signal['ts']).total_seconds()
                            if time_diff < 7200:  # 2 часа без дублей
                                continue

                        self.sent_signals[key] = {
                            'type': signal['type'],
                            'ts': datetime.utcnow()
                        }

                        logger.info(f"Сигнал {signal['type']} на {symbol} {tf}: {signal['price']:.5f}")
                        await self.send_telegram_signal(
                            symbol=signal['symbol'],
                            signal_type=signal['type'],
                            timeframe=signal['timeframe'],
                            price=signal['price'],
                            reason=signal['reason']
                        )
                    except Exception as e:
                        logger.error(f"Ошибка при генерации сигнала {symbol} {tf}: {e}")

            # очистка старых сигналов (старше 24 часов)
            now = datetime.utcnow()
            to_remove = [k for k, v in self.sent_signals.items() if (now - v['ts']).total_seconds() > 86400]
            for k in to_remove:
                del self.sent_signals[k]

            logger.info("Жду 30 минут до следующего цикла...")
            await asyncio.sleep(1800)

    async def run(self):
        try:
            balance = await self.exchange.fetch_balance()
            usdt_free = balance.get('USDT', {}).get('free', 0)
            logger.info(f"Баланс: {usdt_free:.2f} USDT (только сигналы)")
            await self.send_telegram_signal(
                symbol="BOT",
                signal_type="СТАРТ",
                timeframe="LOG",
                price=0.0,
                reason="Старт сигнального бота Heiken Ashi (6h, откат 30% от закрытия)"
            )
        except Exception as e:
            logger.error(f"Ошибка баланса: {e}")
            await self.send_telegram_signal(
                symbol="BOT",
                signal_type="СТАРТ",
                timeframe="LOG",
                price=0.0,
                reason="Сигнальный бот запущен (без баланса)"
            )

        await self.scan_for_signals()

    async def close(self):
        await self.exchange.close()


async def main():
    config = load_config()
    bot = SignalBot(config)
    try:
        await bot.run()
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
