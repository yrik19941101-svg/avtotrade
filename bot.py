# bot.py

import time
import logging
from typing import List, Dict, Any
from decimal import Decimal
import requests

from config import *
from bingx import Client as BingXClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HeikenAshiBot:
    def __init__(self):
        self.bingx = BingXClient(BINGX_API_KEY, BINGX_API_SECRET)
        self.open_positions = {}  # {symbol: {direction, size, avg_price, level, sl, tp}}
        self.telegram_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    def send_telegram(self, text):
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
        requests.post(self.telegram_url, data=data)

    def calculate_heiken_ashi(self, candles):
        ha = []
        for i, c in enumerate(candles):
            close, open, high, low, volume = (
                c["close"],
                c["open"],
                c["high"],
                c["low"],
                c["volume"],
            )
            if i == 0:
                ha_close = (open + high + low + close) / 4
                ha_open = open
            else:
                ha_close = (ha[i - 1]["open"] + ha[i - 1]["high"] + ha[i - 1]["low"] + close) / 4
                ha_open = (ha[i - 1]["open"] + ha[i - 1]["close"]) / 2
            ha_high = max(ha_open, ha_close, high)
            ha_low = min(ha_open, ha_close, low)
            ha.append({
                "open": ha_open,
                "high": ha_high,
                "low": ha_low,
                "close": ha_close,
                "volume": volume
            })
        return ha

    def get_atr(self, candles, period=ATR_PERIOD):
        tr = []
        for i in range(1, len(candles)):
            c, p = candles[i], candles[i - 1]
            tr.append(max(
                c["high"] - c["low"],
                abs(c["high"] - p["close"]),
                abs(c["low"] - p["close"])
            ))
        if len(tr) < period:
            return 0
        return sum(tr[-period:]) / period

    def filter_by_atr(self, candles, symbol):
        atr = self.get_atr(candles)
        price = Decimal(candles[-1]["close"])
        if atr == 0:
            return False
        if atr < price * ATR_MIN_FACTOR:
            logger.info(f"{symbol}: слишком низкая волатильность")
            return False
        # ATR_MAX_FACTOR — опционально отфильтровать очень высокую волу
        return atr < 3 * atr

    def check_long_signal(self, ha_bars, atr):
        # 3+ красных → закрылась зелёная
        if len(ha_bars) < 4:
            return None
        # 3 последних красных
        if not all(ha_bars[i]["close"] < ha_bars[i]["open"] for i in [-4, -3, -2]):
            return None
        # [-1] — зелёная, сигнальная
        if ha_bars[-1]["close"] < ha_bars[-1]["open"]:
            return None

        signal_bar = ha_bars[-1]
        signal_open = signal_bar["open"]
        signal_high = signal_bar["high"]
        signal_low = signal_bar["low"]
        signal_close = signal_bar["close"]
        signal_range = signal_high - signal_low

        # Длина отката свечи (0)
        if len(ha_bars) < 2:
            return None
        pullback_bar = ha_bars[0]  # текущая свеча (откат)
        pullback_high = pullback_bar["high"]
        pullback_low = pullback_bar["low"]
        pullback_range = pullback_high - pullback_low

        # Фильтр по длине отката
        if pullback_range > 0.5 * signal_range:
            return None

        # 30% отката вниз от закрытия зелёной
        level_down = Decimal(signal_close) - RETRACEMENT_RATIO * signal_range

        current_price = Decimal(pullback_bar["close"])
        if current_price <= level_down * 1.001 and current_price >= level_down * 0.999:
            logger.info(f"Лонг сигнал для: {signal_close}, уровень отката: {level_down}")
            return {
                "direction": "BUY",
                "level": level_down,
                "signal_close": signal_close,
                "signal_range": signal_range,
                "atr": atr
            }
        return None

    def check_short_signal(self, ha_bars, atr):
        # 3+ зелёных → закрылась красная
        if len(ha_bars) < 4:
            return None
        if not all(ha_bars[i]["close"] > ha_bars[i]["open"] for i in [-4, -3, -2]):
            return None
        if ha_bars[-1]["close"] > ha_bars[-1]["open"]:
            return None

        signal_bar = ha_bars[-1]
        signal_open = signal_bar["open"]
        signal_high = signal_bar["high"]
        signal_low = signal_bar["low"]
        signal_close = signal_bar["close"]
        signal_range = signal_high - signal_low

        if len(ha_bars) < 2:
            return None
        pullback_bar = ha_bars[0]
        pullback_high = pullback_bar["high"]
        pullback_low = pullback_bar["low"]
        pullback_range = pullback_high - pullback_low

        if pullback_range > 0.5 * signal_range:
            return None

        level_up = Decimal(signal_close) + RETRACEMENT_RATIO * signal_range
        current_price = Decimal(pullback_bar["close"])
        if current_price >= level_up * 0.999 and current_price <= level_up * 1.001:
            logger.info(f"Шорт сигнал для: {signal_close}, уровень отката: {level_up}")
            return {
                "direction": "SELL",
                "level": level_up,
                "signal_close": signal_close,
                "signal_range": signal_range,
                "atr": atr
            }
        return None

    def open_position(self, symbol, direction, size, price, sl, tp, martingale_level=0):
        size = int(size)  # размер в контрактах (или необходимый формат биржи)
        if direction == "BUY":
            # Открываем лонг лимит на уровне price
            self.bingx.trade().futures_long_limit(symbol, size, price, sl, tp, LEVERAGE)
        elif direction == "SELL":
            self.bingx.trade().futures_short_limit(symbol, size, price, sl, tp, LEVERAGE)
        position = {
            "direction": direction,
            "size": size,
            "avg_price": price,
            "level": martingale_level,
            "sl": sl,
            "tp": tp,
        }
        self.open_positions[symbol] = position
        msg = f"[{symbol}] {direction} позиция уровня {martingale_level} открыта {price:.5f}"
        self.send_telegram(msg)

    def martingale_up(self, symbol, direction):
        pos = self.open_positions.get(symbol)
        if not pos or pos["level"] >= 3:
            return
        atr = pos["atr"]
        size = int(pos["size"] * 1.6)
        if direction == "BUY":
            price = pos["avg_price"] * 0.95  # чуть ниже
        else:
            price = pos["avg_price"] * 1.05  # чуть выше
        sl = price * 0.985  # 1.5% ниже
        tp = price * 1.015 * RR_SHORT
        self.open_position(symbol, direction, size, price, sl, tp, pos["level"] + 1)

    def run(self):
        self.send_telegram("Бот запущен: стратегия Heiken Ashi 6H, маринвелин 1.6 до 3 колен, 5 позиций максимум.")
        while True:
            try:
                positions = self.bingx.trade().futures_positions()
                symbol_to_position = {p["symbol"]: p for p in positions}
                self.open_positions = symbol_to_position

                # Получаем все фьючерсные пары BingX
                markets = self.bingx.market().get_symbols()
                futures_symbols = [m["symbol"] for m in markets if m["symbol"].endswith("USDT")]

                for symbol in futures_symbols:
                    candles = self.bingx.market().candles(symbol, TIMEFRAME, limit=HEIKEN_ASHI_CANDLES)
                    if len(candles) < 20:
                        continue
                    ha_bars = self.calculate_heiken_ashi(candles)
                    atr = self.get_atr(candles, ATR_PERIOD)
                    if not self.filter_by_atr(candles, symbol):
                        continue

                    # Проверяем лонг и шорт
                    long_signal = self.check_long_signal(ha_bars, atr)
                    short_signal = self.check_short_signal(ha_bars, atr)

                    if long_signal and len(self.open_positions) < MAX_SIMULTANEOUS_POSITIONS:
                        self.open_position(
                            symbol,
                            long_signal["direction"],
                            RISK_PER_POSITION,
                            long_signal["level"],
                            long_signal["level"] - atr * 1.5,
                            long_signal["level"] + atr * RR_LONG,
                        )
                    elif short_signal and len(self.open_positions) < MAX_SIMULTANEOUS_POSITIONS:
                        self.open_position(
                            symbol,
                            short_signal["direction"],
                            RISK_PER_POSITION,
                            short_signal["level"],
                            short_signal["level"] + atr * 1.5,
                            short_signal["level"] - atr * RR_SHORT,
                        )
                    
                    # Проверяем TP/SL и мартингейл
                    for sym, pos in self.open_positions.items():
                        current_price = Decimal(candles[-1]["close"])
                        if current_price >= pos["tp"]:
                            self.bingx.trade().futures_close_position(sym, 0)  # закрыть
                            self.send_telegram(f"[{sym}] Позиция закрыта в прибыли (TP).")
                        elif current_price <= pos["sl"]:
                            if pos["level"] < 3:
                                self.martingale_up(sym, pos["direction"])
                            else:
                                self.bingx.trade().futures_close_position(sym, 0)
                                self.send_telegram(f"[{sym}] Позиция закрыта в убытке (SL), уровень 3.")
                time.sleep(60)  # 1 минута между циклами
            except Exception as e:
                logger.error(f"Ошибка: {e}")
