"""
╔══════════════════════════════════════════════════════════════════════════════╗
║     POLYMARKET BTC 5M × BINANCE — CROSS DATA COLLECTOR v2.0               ║
║                                                                              ║
║  Todo lo del collector original +                                            ║
║  Datos cruzados de Binance en cada tick:                                    ║
║                                                                              ║
║  BINANCE:                                                                    ║
║    btc_price, btc_open (strike), distance_to_strike, distance_pct,          ║
║    rsi_14, macd_line, macd_signal, macd_hist,                               ║
║    bollinger_upper, bollinger_mid, bollinger_lower, bb_position,            ║
║    volatility_1m, volatility_5m, momentum_30s, momentum_60s,               ║
║    volume_1m, vwap_5m, bid_ask_imbalance_binance,                           ║
║    ema_9, ema_21, atr_14                                                    ║
║                                                                              ║
║  POLYMARKET (sin cambios):                                                   ║
║    YES/NO: best_ask, best_bid, mid_price, spread, ask_liq, bid_liq         ║
║                                                                              ║
║  DERIVADOS (calculados por tick):                                            ║
║    edge_teorico  = prob_teorica - yes_best_ask (si apostamos UP)            ║
║    prob_teorica  = estimación basada en distancia + vol + tiempo            ║
║    implied_prob  = yes_mid_price (probabilidad implícita de Poly)           ║
║                                                                              ║
║  Ejecución real:  comprar YES → yes_best_ask                                ║
║                   comprar NO  → no_best_ask                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

INSTALACIÓN:
    pip install aiohttp aiosqlite pandas loguru numpy

USO:
    python polymarket_binance_collector.py                  <- recolección continua
    python polymarket_binance_collector.py --debug          <- diagnóstico completo
    python polymarket_binance_collector.py --stats          <- estadísticas de la DB
    python polymarket_binance_collector.py --export-split   <- CSVs ≤25MB (solo resueltos)
    python polymarket_binance_collector.py --export-all     <- CSVs ≤25MB (todo, incl. pendientes)
    python polymarket_binance_collector.py --export-summary <- 1 fila por vela (ultra compacto)
"""

import asyncio
import aiosqlite
import aiohttp
import pandas as pd
import numpy as np
import json
import time
import signal
import sys
import math
from datetime import datetime, timezone
from loguru import logger
from pathlib import Path
from collections import deque

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

CONFIG = {
    # Polymarket
    "GAMMA_API":         "https://gamma-api.polymarket.com",
    "CLOB_API":          "https://clob.polymarket.com",
    "MARKET_SLUG_KEY":   "btc-updown-5m",

    # Binance
    "BINANCE_API":       "https://api.binance.com",
    "BINANCE_SYMBOL":    "BTCUSDT",
    "KLINE_INTERVAL":    "1m",
    "KLINE_LIMIT":       50,       # velas de 1m para calcular indicadores (MACD necesita 35+)

    # Timing
    "TICK_INTERVAL":     3,        # segundos entre capturas
    "MIN_SECS_TO_TRACK": 15,       # mínimo de segundos para iniciar captura

    # Storage
    "DB_PATH":           "btc_5m_cross_data.db",
    "CSV_PATH":          "btc_5m_cross_data.csv",
    "CSV_EXPORT_EVERY":  10,

    # HTTP
    "MAX_RETRIES":       4,
    "RETRY_DELAY":       6,

    # Resolución en background
    "RESOLVE_INTERVAL":  20,
    "RESOLVE_MAX_WAIT":  900,

    # Indicadores
    "RSI_PERIOD":        14,
    "MACD_FAST":         12,
    "MACD_SLOW":         26,
    "MACD_SIGNAL":       9,
    "BB_PERIOD":         20,
    "BB_STD":            2.0,
    "ATR_PERIOD":        14,
    "EMA_FAST":          9,
    "EMA_SLOW":          21,

    "LOG_LEVEL":         "INFO",
}

logger.remove()
logger.add(sys.stderr, level=CONFIG["LOG_LEVEL"],
           format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}")
logger.add("collector_cross.log", rotation="50 MB", retention="7 days", level="DEBUG")


# ─────────────────────────────────────────────────────────────────────────────
# BASE DE DATOS — SCHEMA EXTENDIDO
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Identificación del mercado Poly
    market_id               TEXT NOT NULL,
    market_slug             TEXT,
    token_id_yes            TEXT,
    token_id_no             TEXT,
    timestamp_utc           TEXT NOT NULL,
    unix_ts                 REAL NOT NULL,
    segundos_restantes      REAL,

    -- Polymarket YES
    yes_best_ask            REAL,
    yes_best_bid            REAL,
    yes_mid_price           REAL,
    yes_spread              REAL,
    yes_ask_liquidity       REAL,
    yes_bid_liquidity       REAL,

    -- Polymarket NO
    no_best_ask             REAL,
    no_best_bid             REAL,
    no_mid_price            REAL,
    no_spread               REAL,
    no_ask_liquidity        REAL,
    no_bid_liquidity        REAL,

    -- Binance — Precio
    btc_price               REAL,
    btc_open                REAL,
    distance_to_strike      REAL,
    distance_pct            REAL,

    -- Binance — RSI
    rsi_14                  REAL,

    -- Binance — MACD
    macd_line               REAL,
    macd_signal             REAL,
    macd_hist               REAL,

    -- Binance — Bollinger Bands
    bollinger_upper         REAL,
    bollinger_mid           REAL,
    bollinger_lower         REAL,
    bb_position             REAL,

    -- Binance — Volatilidad y Momentum
    volatility_1m           REAL,
    volatility_5m           REAL,
    momentum_30s            REAL,
    momentum_60s            REAL,

    -- Binance — Volumen y Presión
    volume_1m               REAL,
    vwap_5m                 REAL,
    buy_volume_ratio        REAL,

    -- Binance — EMAs y ATR
    ema_9                   REAL,
    ema_21                  REAL,
    atr_14                  REAL,

    -- Derivados cruzados
    implied_prob_up         REAL,
    prob_teorica_up         REAL,
    edge_teorico            REAL,

    -- Resultado
    resultado_final         INTEGER DEFAULT NULL
);
CREATE INDEX IF NOT EXISTS idx_market_id ON ticks(market_id);
CREATE INDEX IF NOT EXISTS idx_resultado  ON ticks(resultado_final);
CREATE INDEX IF NOT EXISTS idx_unix       ON ticks(unix_ts);
CREATE INDEX IF NOT EXISTS idx_segs       ON ticks(segundos_restantes);
CREATE INDEX IF NOT EXISTS idx_dist       ON ticks(distance_pct);

CREATE TABLE IF NOT EXISTS markets_log (
    market_id       TEXT PRIMARY KEY,
    slug            TEXT,
    end_unix        REAL,
    btc_open        REAL,
    resolved        INTEGER DEFAULT 0,
    resultado_final INTEGER DEFAULT NULL,
    created_at      TEXT
);
"""


async def init_db(db_path: str) -> aiosqlite.Connection:
    db = await aiosqlite.connect(db_path)
    await db.executescript(SCHEMA)
    await db.commit()
    logger.success(f"Base de datos lista: {db_path}")
    return db


# ─────────────────────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_json(session: aiohttp.ClientSession, url: str,
                     params: dict = None, retries: int = None):
    retries = retries or CONFIG["MAX_RETRIES"]
    for attempt in range(retries):
        try:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                elif resp.status == 404:
                    return None
                elif resp.status == 429:
                    wait = 2 ** attempt * 5
                    logger.warning(f"Rate limit. Esperando {wait}s...")
                    await asyncio.sleep(wait)
                else:
                    logger.warning(f"HTTP {resp.status} | {url}")
                    await asyncio.sleep(CONFIG["RETRY_DELAY"])
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            wait = 2 ** attempt * CONFIG["RETRY_DELAY"]
            logger.error(f"Red intento {attempt+1}/{retries}: {e}. Esperando {wait}s...")
            await asyncio.sleep(wait)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# TIMESTAMPS (sin cambios)
# ─────────────────────────────────────────────────────────────────────────────

def current_candle_start() -> int:
    return (int(time.time()) // 300) * 300


def candle_slugs_to_search() -> list[tuple[int, str]]:
    start = CONFIG["MARKET_SLUG_KEY"]
    ts    = current_candle_start()
    key   = CONFIG["MARKET_SLUG_KEY"]
    return [(ts, f"{key}-{ts}"), (ts - 300, f"{key}-{ts - 300}")]


# ─────────────────────────────────────────────────────────────────────────────
# PARSEO DE EVENT → MARKET DICT (sin cambios)
# ─────────────────────────────────────────────────────────────────────────────

def parse_clob_ids(raw) -> list:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def extract_market_from_event(event: dict, slug: str, end_unix: float) -> dict | None:
    markets = event.get("markets", [])
    if not markets:
        markets = [event]

    for mkt in markets:
        if not isinstance(mkt, dict):
            continue

        yes_id = no_id = None

        clob_ids = parse_clob_ids(mkt.get("clobTokenIds"))
        if len(clob_ids) >= 2:
            yes_id, no_id = str(clob_ids[0]), str(clob_ids[1])

        if not (yes_id and no_id):
            for o in mkt.get("outcomes", []):
                if not isinstance(o, dict):
                    continue
                label = o.get("title", o.get("name", "")).upper()
                tid   = o.get("clobTokenId") or o.get("token_id")
                if not tid:
                    continue
                if "YES" in label or label == "UP":
                    yes_id = str(tid)
                elif "NO" in label or label == "DOWN":
                    no_id = str(tid)

        if not (yes_id and no_id):
            for t in mkt.get("tokens", []):
                if not isinstance(t, dict):
                    continue
                out = t.get("outcome", "").upper()
                tid = t.get("token_id") or t.get("id")
                if not tid:
                    continue
                if out in ("YES", "UP"):
                    yes_id = str(tid)
                elif out in ("NO", "DOWN"):
                    no_id = str(tid)

        if yes_id and no_id:
            return {
                "yes_id":    yes_id,
                "no_id":     no_id,
                "market_id": str(mkt.get("conditionId") or mkt.get("id") or ""),
                "slug":      slug,
                "end_unix":  end_unix,
            }
    return None


# ─────────────────────────────────────────────────────────────────────────────
# BÚSQUEDA DE MERCADO ACTIVO (sin cambios)
# ─────────────────────────────────────────────────────────────────────────────

async def find_active_market(session: aiohttp.ClientSession) -> dict | None:
    url    = f"{CONFIG['GAMMA_API']}/events"
    now_ts = time.time()
    candidates = []

    for start_ts, slug in candle_slugs_to_search():
        data = await fetch_json(session, url, params={"slug": slug})
        if not data:
            continue
        events = data if isinstance(data, list) else [data]
        for event in events:
            if not isinstance(event, dict):
                continue
            end_unix = float(start_ts) + 300
            market = extract_market_from_event(event, slug, end_unix)
            if market:
                secs_left = end_unix - now_ts
                candidates.append((secs_left, market))
                break

    active = [(s, m) for s, m in candidates if 0 < s <= 300]
    if not active:
        return None

    active.sort(key=lambda x: x[0])
    _, best = active[0]
    logger.info(f"Mercado activo: {best['slug']} | {best['end_unix'] - now_ts:.0f}s restantes")
    return best


# ─────────────────────────────────────────────────────────────────────────────
# ORDER BOOK POLYMARKET (sin cambios)
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_orderbook(session: aiohttp.ClientSession, token_id: str) -> dict | None:
    data = await fetch_json(session, f"{CONFIG['CLOB_API']}/book",
                            params={"token_id": token_id})
    if not data:
        return None
    asks = data.get("asks") or []
    bids = data.get("bids") or []
    if not asks or not bids:
        return None

    asks_s = sorted(asks, key=lambda x: float(x.get("price", 999)))
    bids_s = sorted(bids, key=lambda x: float(x.get("price", 0)), reverse=True)
    best_ask = float(asks_s[0]["price"])
    best_bid = float(bids_s[0]["price"])

    return {
        "best_ask":      best_ask,
        "best_bid":      best_bid,
        "mid_price":     round((best_ask + best_bid) / 2, 6),
        "spread":        round(best_ask - best_bid, 6),
        "ask_liquidity": float(asks_s[0].get("size", 0)),
        "bid_liquidity": float(bids_s[0].get("size", 0)),
    }


# ─────────────────────────────────────────────────────────────────────────────
# ██████  BINANCE FEEDER  ██████
# ─────────────────────────────────────────────────────────────────────────────

class BinanceFeeder:
    """
    Recoge precio spot + klines de Binance y calcula indicadores técnicos.

    Endpoints usados (públicos, sin auth):
      - GET /api/v3/ticker/price       → precio actual
      - GET /api/v3/klines             → velas 1m para indicadores
      - GET /api/v3/ticker/bookTicker  → best bid/ask de Binance
      - GET /api/v3/aggTrades          → trades recientes para momentum granular

    Rate limit Binance: 1200 req/min (más que suficiente a 3s/tick)
    """

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.base    = CONFIG["BINANCE_API"]
        self.symbol  = CONFIG["BINANCE_SYMBOL"]

        # Cache de precios recientes para momentum sub-minuto
        self._price_history: deque = deque(maxlen=120)  # ~6 min a 3s/tick

    async def fetch_all(self) -> dict | None:
        """
        Hace todas las llamadas a Binance en paralelo y devuelve
        un dict con precio + indicadores, o None si falla.
        """
        try:
            price_data, klines, book_ticker = await asyncio.gather(
                self._fetch_price(),
                self._fetch_klines(),
                self._fetch_book_ticker(),
                return_exceptions=True,
            )

            # Si el precio falla, no podemos hacer nada
            if isinstance(price_data, Exception) or price_data is None:
                logger.warning(f"Binance precio falló: {price_data}")
                return None

            btc_price = price_data
            self._price_history.append((time.time(), btc_price))

            result = {"btc_price": btc_price}

            # Indicadores desde klines
            if not isinstance(klines, Exception) and klines is not None:
                indicators = self._calculate_indicators(klines)
                result.update(indicators)

            # Bid/ask imbalance de Binance
            if not isinstance(book_ticker, Exception) and book_ticker is not None:
                result["buy_volume_ratio"] = book_ticker.get("imbalance")

            # Momentum sub-minuto desde price_history
            momentum = self._calculate_momentum()
            result.update(momentum)

            return result

        except Exception as e:
            logger.error(f"BinanceFeeder error: {e}")
            return None

    # ── Fetches individuales ──────────────────────────────────────────────

    async def _fetch_price(self) -> float | None:
        data = await fetch_json(
            self.session,
            f"{self.base}/api/v3/ticker/price",
            params={"symbol": self.symbol},
            retries=2,
        )
        if data and "price" in data:
            return float(data["price"])
        return None

    async def _fetch_klines(self) -> list | None:
        """Devuelve lista de klines 1m recientes."""
        data = await fetch_json(
            self.session,
            f"{self.base}/api/v3/klines",
            params={
                "symbol":   self.symbol,
                "interval": CONFIG["KLINE_INTERVAL"],
                "limit":    CONFIG["KLINE_LIMIT"],
            },
            retries=2,
        )
        if data and isinstance(data, list) and len(data) >= 5:
            return data
        return None

    async def _fetch_book_ticker(self) -> dict | None:
        """Best bid/ask de Binance para calcular imbalance."""
        data = await fetch_json(
            self.session,
            f"{self.base}/api/v3/ticker/bookTicker",
            params={"symbol": self.symbol},
            retries=2,
        )
        if data:
            bid_qty = float(data.get("bidQty", 0))
            ask_qty = float(data.get("askQty", 0))
            total   = bid_qty + ask_qty
            imbalance = bid_qty / total if total > 0 else 0.5
            return {"imbalance": round(imbalance, 6)}
        return None

    # ── Cálculo de indicadores ────────────────────────────────────────────

    def _calculate_indicators(self, klines: list) -> dict:
        """
        Calcula todos los indicadores técnicos desde klines de 1m.

        Formato kline de Binance:
        [open_time, open, high, low, close, volume, close_time,
         quote_volume, trades, taker_buy_vol, taker_buy_quote_vol, ignore]
        """
        closes  = np.array([float(k[4]) for k in klines])
        highs   = np.array([float(k[2]) for k in klines])
        lows    = np.array([float(k[3]) for k in klines])
        volumes = np.array([float(k[5]) for k in klines])
        taker_buy_vols = np.array([float(k[9]) for k in klines])

        result = {}

        # ── RSI 14 ────────────────────────────────────────────────────────
        rsi = self._rsi(closes, CONFIG["RSI_PERIOD"])
        if rsi is not None:
            result["rsi_14"] = round(rsi, 4)

        # ── MACD (12, 26, 9) ─────────────────────────────────────────────
        macd_line, macd_signal, macd_hist = self._macd(
            closes, CONFIG["MACD_FAST"], CONFIG["MACD_SLOW"], CONFIG["MACD_SIGNAL"]
        )
        if macd_line is not None:
            result["macd_line"]   = round(macd_line, 4)
            result["macd_signal"] = round(macd_signal, 4)
            result["macd_hist"]   = round(macd_hist, 4)

        # ── Bollinger Bands (20, 2) ───────────────────────────────────────
        bb = self._bollinger(closes, CONFIG["BB_PERIOD"], CONFIG["BB_STD"])
        if bb:
            result["bollinger_upper"] = round(bb["upper"], 2)
            result["bollinger_mid"]   = round(bb["mid"], 2)
            result["bollinger_lower"] = round(bb["lower"], 2)
            result["bb_position"]     = round(bb["position"], 6)

        # ── Volatilidad ──────────────────────────────────────────────────
        if len(closes) >= 2:
            returns = np.diff(closes) / closes[:-1]
            result["volatility_1m"] = round(float(np.std(returns[-1:])) if len(returns) >= 1 else 0, 8)
            vol_5 = returns[-5:] if len(returns) >= 5 else returns
            result["volatility_5m"] = round(float(np.std(vol_5)), 8)

        # ── Volumen 1m ────────────────────────────────────────────────────
        if len(volumes) >= 1:
            result["volume_1m"] = round(float(volumes[-1]), 4)

        # ── VWAP 5m ──────────────────────────────────────────────────────
        if len(closes) >= 5 and len(volumes) >= 5:
            typical = (highs[-5:] + lows[-5:] + closes[-5:]) / 3
            vol5    = volumes[-5:]
            vwap    = np.sum(typical * vol5) / np.sum(vol5) if np.sum(vol5) > 0 else closes[-1]
            result["vwap_5m"] = round(float(vwap), 2)

        # ── Buy volume ratio (último minuto) ─────────────────────────────
        if len(volumes) >= 1 and len(taker_buy_vols) >= 1:
            total_vol = float(volumes[-1])
            if total_vol > 0:
                result["buy_volume_ratio"] = round(float(taker_buy_vols[-1]) / total_vol, 6)

        # ── EMAs ──────────────────────────────────────────────────────────
        ema9 = self._ema(closes, CONFIG["EMA_FAST"])
        ema21 = self._ema(closes, CONFIG["EMA_SLOW"])
        if ema9 is not None:
            result["ema_9"] = round(float(ema9), 2)
        if ema21 is not None:
            result["ema_21"] = round(float(ema21), 2)

        # ── ATR 14 ────────────────────────────────────────────────────────
        atr = self._atr(highs, lows, closes, CONFIG["ATR_PERIOD"])
        if atr is not None:
            result["atr_14"] = round(float(atr), 4)

        return result

    def _calculate_momentum(self) -> dict:
        """Momentum sub-minuto usando el historial de precios del feeder."""
        result = {}
        now = time.time()
        prices = list(self._price_history)

        if len(prices) < 2:
            return result

        current_price = prices[-1][1]

        # Momentum 30s: buscar precio más cercano a 30s atrás
        for ts, px in reversed(prices):
            if now - ts >= 28:   # tolerancia ±2s
                result["momentum_30s"] = round((current_price - px) / px * 100, 6)
                break

        # Momentum 60s
        for ts, px in reversed(prices):
            if now - ts >= 58:
                result["momentum_60s"] = round((current_price - px) / px * 100, 6)
                break

        return result

    # ── Indicadores puros (funciones estáticas) ───────────────────────────

    @staticmethod
    def _rsi(closes: np.ndarray, period: int) -> float | None:
        if len(closes) < period + 1:
            return None
        deltas = np.diff(closes)
        gains  = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)

        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])

        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def _ema(data: np.ndarray, period: int) -> float | None:
        if len(data) < period:
            return None
        multiplier = 2.0 / (period + 1)
        ema_val = float(np.mean(data[:period]))
        for price in data[period:]:
            ema_val = (float(price) - ema_val) * multiplier + ema_val
        return ema_val

    @staticmethod
    def _macd(closes: np.ndarray, fast: int, slow: int, signal_p: int):
        if len(closes) < slow + signal_p:
            return None, None, None

        def ema_series(data, period):
            mult = 2.0 / (period + 1)
            ema  = [float(np.mean(data[:period]))]
            for px in data[period:]:
                ema.append((float(px) - ema[-1]) * mult + ema[-1])
            return ema

        ema_fast = ema_series(closes, fast)
        ema_slow = ema_series(closes, slow)

        min_len   = min(len(ema_fast), len(ema_slow))
        macd_line = [ema_fast[-(min_len - i)] - ema_slow[-(min_len - i)]
                     for i in range(min_len)]

        if len(macd_line) < signal_p:
            return None, None, None

        signal_line = ema_series(np.array(macd_line), signal_p)

        ml = macd_line[-1]
        sl = signal_line[-1]
        return ml, sl, ml - sl

    @staticmethod
    def _bollinger(closes: np.ndarray, period: int, num_std: float) -> dict | None:
        if len(closes) < period:
            return None
        window = closes[-period:]
        mid    = float(np.mean(window))
        std    = float(np.std(window))
        upper  = mid + num_std * std
        lower  = mid - num_std * std
        bw     = upper - lower
        pos    = (float(closes[-1]) - lower) / bw if bw > 0 else 0.5
        return {"upper": upper, "mid": mid, "lower": lower, "position": pos}

    @staticmethod
    def _atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
             period: int) -> float | None:
        if len(closes) < period + 1:
            return None
        tr_list = []
        for i in range(1, len(closes)):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i - 1])
            lc = abs(lows[i] - closes[i - 1])
            tr_list.append(max(hl, hc, lc))
        if len(tr_list) < period:
            return None
        atr_val = float(np.mean(tr_list[:period]))
        for tr in tr_list[period:]:
            atr_val = (atr_val * (period - 1) + float(tr)) / period
        return atr_val


# ─────────────────────────────────────────────────────────────────────────────
# ██████  PROBABILIDAD TEÓRICA  ██████
# ─────────────────────────────────────────────────────────────────────────────

def theoretical_prob_up(distance_pct: float, seconds_left: float,
                        volatility_5m: float) -> float:
    """
    Estimación simplificada de P(cierra arriba del strike).

    Usa un modelo tipo digital-option con distribución normal:
      z = distance / (vol_anualizada * sqrt(T))

    donde:
      - distance = (price - strike) / strike  (ya es distance_pct/100)
      - vol = volatility_5m anualizada (aprox)
      - T = seconds_left / (365.25 * 24 * 3600)

    Devuelve P(S_T > strike) ≈ Φ(z)
    """
    if seconds_left <= 0 or volatility_5m is None or volatility_5m <= 0:
        return 1.0 if distance_pct > 0 else (0.5 if distance_pct == 0 else 0.0)

    # Convertir vol de returns 1m a vol por segundo (aprox)
    # vol_5m es std de returns de 1m → vol por segundo ≈ vol_1m / sqrt(60)
    vol_per_sec = volatility_5m / math.sqrt(60)

    # Desviación esperada en seconds_left
    expected_std = vol_per_sec * math.sqrt(seconds_left)

    if expected_std <= 0:
        return 1.0 if distance_pct > 0 else 0.0

    # z-score: cuántas desviaciones estándar está del strike
    z = (distance_pct / 100.0) / expected_std

    # CDF normal estándar
    prob = 0.5 * (1.0 + math.erf(z / math.sqrt(2)))

    return round(max(0.001, min(0.999, prob)), 6)


# ─────────────────────────────────────────────────────────────────────────────
# RESOLUCIÓN EN BACKGROUND (sin cambios)
# ─────────────────────────────────────────────────────────────────────────────

async def resolve_via_polymarket(session: aiohttp.ClientSession,
                                  slug: str) -> int | None:
    data = await fetch_json(session, f"{CONFIG['GAMMA_API']}/events",
                            params={"slug": slug})
    if not data:
        return None

    events = data if isinstance(data, list) else [data]
    for ev in events:
        if not isinstance(ev, dict):
            continue
        for mkt in ev.get("markets", [ev]):
            if not isinstance(mkt, dict):
                continue
            if not mkt.get("resolved") and not mkt.get("closed"):
                continue
            op = mkt.get("outcomePrices") or []
            if isinstance(op, str):
                try:
                    op = json.loads(op)
                except Exception:
                    continue
            if len(op) >= 2:
                resultado = 1 if float(op[0]) >= 0.5 else 0
                logger.debug(f"[BG] Polymarket: UP={float(op[0]):.4f} DOWN={float(op[1]):.4f} "
                             f"-> {'UP' if resultado == 1 else 'DOWN'}")
                return resultado
    return None


async def resolve_in_background(session: aiohttp.ClientSession,
                                 db: aiosqlite.Connection,
                                 market_id: str,
                                 slug: str,
                                 end_unix: float):
    wait = max(end_unix - time.time() + CONFIG["RESOLVE_INTERVAL"], 15)
    await asyncio.sleep(wait)

    logger.debug(f"[BG] Resolviendo {slug} via Polymarket/Chainlink...")

    max_attempts = CONFIG["RESOLVE_MAX_WAIT"] // CONFIG["RESOLVE_INTERVAL"]
    for attempt in range(int(max_attempts)):
        try:
            resultado = await resolve_via_polymarket(session, slug)
            if resultado is not None:
                await db.execute(
                    "UPDATE ticks SET resultado_final=? WHERE market_id=?",
                    (resultado, market_id)
                )
                await db.execute(
                    "UPDATE markets_log SET resolved=1, resultado_final=? WHERE market_id=?",
                    (resultado, market_id)
                )
                await db.commit()
                label = "UP  ↑" if resultado == 1 else "DOWN ↓"
                logger.success(f"[BG] Resuelto (Polymarket): {label} | {slug}")
                return
            logger.debug(f"[BG] {slug} sin resolver aún, reintento {attempt+1} "
                         f"en {CONFIG['RESOLVE_INTERVAL']}s...")
        except Exception as e:
            logger.warning(f"[BG] Error resolución intento {attempt+1}: {e}")

        await asyncio.sleep(CONFIG["RESOLVE_INTERVAL"])

    logger.warning(f"[BG] No se pudo resolver {slug} tras {CONFIG['RESOLVE_MAX_WAIT']}s — ticks quedan con NULL")


# ─────────────────────────────────────────────────────────────────────────────
# ██████  TRACKER PRINCIPAL (EXTENDIDO)  ██████
# ─────────────────────────────────────────────────────────────────────────────

class BTCMarketTracker:

    def __init__(self, db: aiosqlite.Connection, session: aiohttp.ClientSession):
        self.db                = db
        self.session           = session
        self.buffer            = []
        self.markets_completed = 0
        self._seen_slugs       = set()
        self.binance           = BinanceFeeder(session)

    async def run_forever(self):
        logger.info("=" * 70)
        logger.info("  POLYMARKET × BINANCE CROSS COLLECTOR v2.0 — INICIADO")
        logger.info("  Poly: orderbook YES/NO via CLOB API")
        logger.info("  Binance: precio, RSI, MACD, BB, vol, momentum, EMAs, ATR")
        logger.info("  Resolución: Polymarket/Chainlink (Gamma API)")
        logger.info("  Ctrl+C para parar limpiamente")
        logger.info("=" * 70)

        while True:
            try:
                await self._run_one_market_cycle()
            except asyncio.CancelledError:
                logger.info("Shutdown. Guardando buffer...")
                await self._flush_buffer()
                break
            except Exception as e:
                logger.exception(f"Error en ciclo: {e}")
                await asyncio.sleep(10)

    async def _run_one_market_cycle(self):
        # ── 1. Buscar mercado activo ──────────────────────────────────────
        market = None
        while market is None:
            market = await find_active_market(self.session)
            if market is None:
                await asyncio.sleep(3)
                continue
            if market["slug"] in self._seen_slugs:
                secs_left = market["end_unix"] - time.time()
                if secs_left > 0:
                    logger.debug(f"  Ya trackeada {market['slug']}, esperando {secs_left:.0f}s...")
                    await asyncio.sleep(min(secs_left + 1, 10))
                market = None

        market_id = market["market_id"]
        slug      = market["slug"]
        yes_id    = market["yes_id"]
        no_id     = market["no_id"]
        end_unix  = market["end_unix"]
        secs_left = end_unix - time.time()

        if secs_left < CONFIG["MIN_SECS_TO_TRACK"]:
            logger.info(f"  {slug} solo tiene {secs_left:.0f}s — esperando siguiente vela")
            self._seen_slugs.add(slug)
            await asyncio.sleep(secs_left + 2)
            return

        self._seen_slugs.add(slug)

        # ── 2. Capturar precio de apertura (strike) desde Binance ─────────
        btc_open = await self._get_strike_price()
        await self._log_market(market_id, slug, end_unix, btc_open)

        logger.info(f"  Strike (btc_open): {btc_open:.2f}" if btc_open else "  Strike: no disponible")

        # ── 3. Lanzar resolución en background ────────────────────────────
        asyncio.create_task(
            resolve_in_background(self.session, self.db, market_id, slug, end_unix)
        )

        # ── 4. Trackear ticks hasta cierre ────────────────────────────────
        logger.info(f"  Capturando {slug} | {secs_left:.0f}s restantes")
        await self._track_market(market_id, slug, yes_id, no_id, end_unix, btc_open)

        # ── 5. Siguiente vela ─────────────────────────────────────────────
        self.markets_completed += 1
        if self.markets_completed % CONFIG["CSV_EXPORT_EVERY"] == 0:
            await self._export_csv()

        if len(self._seen_slugs) > 100:
            self._seen_slugs = set(list(self._seen_slugs)[-50:])

    async def _get_strike_price(self) -> float | None:
        """
        Obtiene el precio de BTC al inicio de la vela de 5m.
        Usa la kline de 5m actual de Binance para obtener el open exacto.
        """
        try:
            data = await fetch_json(
                self.session,
                f"{CONFIG['BINANCE_API']}/api/v3/klines",
                params={"symbol": CONFIG["BINANCE_SYMBOL"], "interval": "5m", "limit": 1},
                retries=3,
            )
            if data and isinstance(data, list) and len(data) >= 1:
                return float(data[0][1])  # open price de la vela actual de 5m
        except Exception as e:
            logger.warning(f"Error obteniendo strike: {e}")

        # Fallback: precio actual
        try:
            data = await fetch_json(
                self.session,
                f"{CONFIG['BINANCE_API']}/api/v3/ticker/price",
                params={"symbol": CONFIG["BINANCE_SYMBOL"]},
                retries=2,
            )
            if data:
                return float(data["price"])
        except Exception:
            pass
        return None

    async def _track_market(self, market_id: str, slug: str,
                             yes_id: str, no_id: str, end_unix: float,
                             btc_open: float | None):
        tick_count = 0

        while True:
            now       = time.time()
            secs_left = end_unix - now

            if secs_left <= 0:
                logger.info(f"  Vela cerrada. {tick_count} ticks capturados → siguiente vela")
                break

            # ── Fetch paralelo: Poly YES + Poly NO + Binance ─────────────
            yes_ob, no_ob, binance_data = await asyncio.gather(
                fetch_orderbook(self.session, yes_id),
                fetch_orderbook(self.session, no_id),
                self.binance.fetch_all(),
            )

            if yes_ob and no_ob:
                # Datos base de Binance
                btc_price    = binance_data.get("btc_price") if binance_data else None
                dist_abs     = None
                dist_pct     = None
                prob_teorica = None
                edge         = None

                if btc_price and btc_open:
                    dist_abs = round(btc_price - btc_open, 2)
                    dist_pct = round((btc_price - btc_open) / btc_open * 100, 6)

                    vol_5m = binance_data.get("volatility_5m") if binance_data else None
                    prob_teorica = theoretical_prob_up(dist_pct, secs_left, vol_5m)
                    edge = round(prob_teorica - yes_ob["best_ask"], 6)

                tick = {
                    # Poly base
                    "market_id":          market_id,
                    "market_slug":        slug,
                    "token_id_yes":       yes_id,
                    "token_id_no":        no_id,
                    "timestamp_utc":      datetime.now(timezone.utc).isoformat(),
                    "unix_ts":            now,
                    "segundos_restantes": round(secs_left, 2),
                    # Poly YES
                    "yes_best_ask":       yes_ob["best_ask"],
                    "yes_best_bid":       yes_ob["best_bid"],
                    "yes_mid_price":      yes_ob["mid_price"],
                    "yes_spread":         yes_ob["spread"],
                    "yes_ask_liquidity":  yes_ob["ask_liquidity"],
                    "yes_bid_liquidity":  yes_ob["bid_liquidity"],
                    # Poly NO
                    "no_best_ask":        no_ob["best_ask"],
                    "no_best_bid":        no_ob["best_bid"],
                    "no_mid_price":       no_ob["mid_price"],
                    "no_spread":          no_ob["spread"],
                    "no_ask_liquidity":   no_ob["ask_liquidity"],
                    "no_bid_liquidity":   no_ob["bid_liquidity"],
                    # Binance precio
                    "btc_price":          btc_price,
                    "btc_open":           btc_open,
                    "distance_to_strike": dist_abs,
                    "distance_pct":       dist_pct,
                    # Binance indicadores
                    "rsi_14":             binance_data.get("rsi_14") if binance_data else None,
                    "macd_line":          binance_data.get("macd_line") if binance_data else None,
                    "macd_signal":        binance_data.get("macd_signal") if binance_data else None,
                    "macd_hist":          binance_data.get("macd_hist") if binance_data else None,
                    "bollinger_upper":    binance_data.get("bollinger_upper") if binance_data else None,
                    "bollinger_mid":      binance_data.get("bollinger_mid") if binance_data else None,
                    "bollinger_lower":    binance_data.get("bollinger_lower") if binance_data else None,
                    "bb_position":        binance_data.get("bb_position") if binance_data else None,
                    "volatility_1m":      binance_data.get("volatility_1m") if binance_data else None,
                    "volatility_5m":      binance_data.get("volatility_5m") if binance_data else None,
                    "momentum_30s":       binance_data.get("momentum_30s") if binance_data else None,
                    "momentum_60s":       binance_data.get("momentum_60s") if binance_data else None,
                    "volume_1m":          binance_data.get("volume_1m") if binance_data else None,
                    "vwap_5m":            binance_data.get("vwap_5m") if binance_data else None,
                    "buy_volume_ratio":   binance_data.get("buy_volume_ratio") if binance_data else None,
                    "ema_9":              binance_data.get("ema_9") if binance_data else None,
                    "ema_21":             binance_data.get("ema_21") if binance_data else None,
                    "atr_14":             binance_data.get("atr_14") if binance_data else None,
                    # Derivados cruzados
                    "implied_prob_up":    yes_ob["mid_price"],
                    "prob_teorica_up":    prob_teorica,
                    "edge_teorico":       edge,
                    # Resultado
                    "resultado_final":    None,
                }

                self.buffer.append(tick)
                tick_count += 1

                # Log compacto con info cruzada
                dist_str = f"dist={dist_pct:+.4f}%" if dist_pct is not None else "dist=N/A"
                rsi_str  = f"RSI={binance_data.get('rsi_14', 'N/A')}" if binance_data else "RSI=N/A"
                edge_str = f"edge={edge:+.4f}" if edge is not None else "edge=N/A"

                logger.info(
                    f"  tick#{tick_count:3d} [{secs_left:5.0f}s] "
                    f"YES={yes_ob['best_ask']:.3f} NO={no_ob['best_ask']:.3f} "
                    f"| BTC={btc_price:,.0f} {dist_str} {rsi_str} {edge_str} "
                    f"| buf={len(self.buffer)}"
                )
            else:
                logger.warning(f"  [{secs_left:5.0f}s] order books vacíos o incompletos")

            if len(self.buffer) >= 30:
                await self._flush_buffer()

            await asyncio.sleep(CONFIG["TICK_INTERVAL"])

        await self._flush_buffer()

    # ── DB ────────────────────────────────────────────────────────────────

    async def _flush_buffer(self):
        if not self.buffer:
            return
        try:
            cols = [
                "market_id", "market_slug", "token_id_yes", "token_id_no",
                "timestamp_utc", "unix_ts", "segundos_restantes",
                "yes_best_ask", "yes_best_bid", "yes_mid_price", "yes_spread",
                "yes_ask_liquidity", "yes_bid_liquidity",
                "no_best_ask", "no_best_bid", "no_mid_price", "no_spread",
                "no_ask_liquidity", "no_bid_liquidity",
                "btc_price", "btc_open", "distance_to_strike", "distance_pct",
                "rsi_14", "macd_line", "macd_signal", "macd_hist",
                "bollinger_upper", "bollinger_mid", "bollinger_lower", "bb_position",
                "volatility_1m", "volatility_5m", "momentum_30s", "momentum_60s",
                "volume_1m", "vwap_5m", "buy_volume_ratio",
                "ema_9", "ema_21", "atr_14",
                "implied_prob_up", "prob_teorica_up", "edge_teorico",
                "resultado_final",
            ]
            placeholders = ", ".join([f":{c}" for c in cols])
            col_names    = ", ".join(cols)
            sql = f"INSERT INTO ticks ({col_names}) VALUES ({placeholders})"

            await self.db.executemany(sql, self.buffer)
            await self.db.commit()
            logger.debug(f"  Flush: {len(self.buffer)} ticks -> DB")
            self.buffer.clear()
        except Exception as e:
            logger.error(f"Error flush: {e}")

    async def _log_market(self, market_id: str, slug: str,
                           end_unix: float, btc_open: float | None):
        await self.db.execute("""
            INSERT OR IGNORE INTO markets_log (market_id, slug, end_unix, btc_open, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (market_id, slug, end_unix, btc_open,
              datetime.now(timezone.utc).isoformat()))
        await self.db.commit()

    async def _export_csv(self):
        try:
            async with self.db.execute(
                "SELECT * FROM ticks WHERE resultado_final IS NOT NULL"
            ) as cursor:
                rows = await cursor.fetchall()
                cols = [d[0] for d in cursor.description]
            df = pd.DataFrame(rows, columns=cols)
            df.to_csv(CONFIG["CSV_PATH"], index=False)
            logger.success(f"CSV exportado: {len(df)} filas -> {CONFIG['CSV_PATH']}")
        except Exception as e:
            logger.error(f"Error CSV: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# MODO DEBUG (EXTENDIDO)
# ─────────────────────────────────────────────────────────────────────────────

async def debug_api():
    headers = {"Accept": "application/json", "User-Agent": "btc5m-cross-collector/2.0"}
    async with aiohttp.ClientSession(headers=headers) as session:

        now   = time.time()
        start = current_candle_start()
        end   = start + 300

        print(f"\n{'='*70}")
        print(f"  POLYMARKET × BINANCE CROSS COLLECTOR — DEBUG")
        print(f"{'='*70}")

        print(f"\n[1] Timestamps")
        print(f"    Ahora:              {int(now)}")
        print(f"    Inicio vela actual: {start}  (empezó hace {now-start:.0f}s)")
        print(f"    Cierre vela actual: {end}  ({end-now:.0f}s restantes)")

        # ── Binance ───────────────────────────────────────────────────────
        print(f"\n[2] Binance BTCUSDT")
        feeder = BinanceFeeder(session)
        binance = await feeder.fetch_all()
        if binance:
            print(f"    Precio:     ${binance.get('btc_price', 'N/A'):,.2f}")
            print(f"    RSI(14):    {binance.get('rsi_14', 'N/A')}")
            print(f"    MACD:       line={binance.get('macd_line', 'N/A')} "
                  f"signal={binance.get('macd_signal', 'N/A')} "
                  f"hist={binance.get('macd_hist', 'N/A')}")
            print(f"    Bollinger:  U={binance.get('bollinger_upper', 'N/A')} "
                  f"M={binance.get('bollinger_mid', 'N/A')} "
                  f"L={binance.get('bollinger_lower', 'N/A')}")
            print(f"    BB pos:     {binance.get('bb_position', 'N/A')}")
            print(f"    Vol 1m:     {binance.get('volatility_1m', 'N/A')}")
            print(f"    Vol 5m:     {binance.get('volatility_5m', 'N/A')}")
            print(f"    EMA 9/21:   {binance.get('ema_9', 'N/A')} / {binance.get('ema_21', 'N/A')}")
            print(f"    ATR(14):    {binance.get('atr_14', 'N/A')}")
            print(f"    Vol 1m BTC: {binance.get('volume_1m', 'N/A')}")
            print(f"    VWAP 5m:    {binance.get('vwap_5m', 'N/A')}")
            print(f"    Buy ratio:  {binance.get('buy_volume_ratio', 'N/A')}")
        else:
            print(f"    ERROR: No se pudo obtener datos de Binance")

        # ── Polymarket ────────────────────────────────────────────────────
        print(f"\n[3] Polymarket — Búsqueda de mercado activo")
        market = await find_active_market(session)
        if not market:
            print("    No encontrado. Transición de vela, reintenta en unos segundos.")
            return

        secs = market["end_unix"] - now
        print(f"    slug:      {market['slug']}")
        print(f"    market_id: {market['market_id']}")
        print(f"    expira en: {secs:.0f}s")

        print(f"\n[4] Order books")
        yes_ob = await fetch_orderbook(session, market["yes_id"])
        no_ob  = await fetch_orderbook(session, market["no_id"])

        if yes_ob:
            print(f"    YES: ask={yes_ob['best_ask']:.4f}  bid={yes_ob['best_bid']:.4f}  "
                  f"spread={yes_ob['spread']:.4f}")
        if no_ob:
            print(f"    NO:  ask={no_ob['best_ask']:.4f}  bid={no_ob['best_bid']:.4f}  "
                  f"spread={no_ob['spread']:.4f}")

        # ── Cruce ─────────────────────────────────────────────────────────
        if binance and yes_ob and no_ob:
            btc_price = binance["btc_price"]
            # Obtener strike
            strike_data = await fetch_json(
                session,
                f"{CONFIG['BINANCE_API']}/api/v3/klines",
                params={"symbol": CONFIG["BINANCE_SYMBOL"], "interval": "5m", "limit": 1},
            )
            btc_open = float(strike_data[0][1]) if strike_data else btc_price

            dist_pct = (btc_price - btc_open) / btc_open * 100
            vol_5m   = binance.get("volatility_5m", 0)
            prob     = theoretical_prob_up(dist_pct, secs, vol_5m)
            edge     = prob - yes_ob["best_ask"]

            print(f"\n[5] Análisis cruzado")
            print(f"    Strike (open 5m): ${btc_open:,.2f}")
            print(f"    Precio actual:    ${btc_price:,.2f}")
            print(f"    Distancia:        {dist_pct:+.4f}%  (${btc_price - btc_open:+.2f})")
            print(f"    Volatilidad 5m:   {vol_5m}")
            print(f"    Prob teórica UP:  {prob:.4f}  ({prob*100:.1f}%)")
            print(f"    Poly implied UP:  {yes_ob['mid_price']:.4f}  ({yes_ob['mid_price']*100:.1f}%)")
            print(f"    Edge teórico:     {edge:+.4f}  ({edge*100:+.1f}%)")
            print(f"    Comprar YES ask:  {yes_ob['best_ask']:.4f}")
            print(f"    Comprar NO  ask:  {no_ob['best_ask']:.4f}")

            if abs(edge) > 0.05:
                side = "YES (UP)" if edge > 0 else "NO (DOWN)"
                print(f"\n    ⚡ Edge > 5% detectado → {side}")
            else:
                print(f"\n    → Edge bajo, sin señal clara")

        print(f"\n{'='*70}")
        print(f"  DEBUG COMPLETO")
        print(f"{'='*70}\n")


# ─────────────────────────────────────────────────────────────────────────────
# MODO STATS (EXTENDIDO)
# ─────────────────────────────────────────────────────────────────────────────

async def quick_stats(db_path: str):
    if not Path(db_path).exists():
        print(f"No existe {db_path}.")
        return
    db = await aiosqlite.connect(db_path)

    async def q(sql):
        async with db.execute(sql) as c:
            return await c.fetchone()

    total      = (await q("SELECT COUNT(*) FROM ticks"))[0]
    resolved   = (await q("SELECT COUNT(*) FROM ticks WHERE resultado_final IS NOT NULL"))[0]
    mkts_done  = (await q("SELECT COUNT(DISTINCT market_id) FROM markets_log WHERE resolved=1"))[0]
    wr         = (await q("SELECT AVG(resultado_final) FROM ticks WHERE resultado_final IS NOT NULL"))[0]
    avg_edge   = (await q("SELECT AVG(edge_teorico) FROM ticks WHERE resultado_final IS NOT NULL AND edge_teorico IS NOT NULL"))[0]
    avg_dist   = (await q("SELECT AVG(ABS(distance_pct)) FROM ticks WHERE distance_pct IS NOT NULL"))[0]
    avg_vol    = (await q("SELECT AVG(volatility_5m) FROM ticks WHERE volatility_5m IS NOT NULL"))[0]
    t_range    = await q("SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM ticks")
    binance_ok = (await q("SELECT COUNT(*) FROM ticks WHERE btc_price IS NOT NULL"))[0]

    print(f"\n{'='*60}")
    print(f"  ESTADÍSTICAS — CROSS COLLECTOR v2.0")
    print(f"{'='*60}")
    print(f"  Total ticks:              {total:,}")
    print(f"  Ticks con resultado:      {resolved:,}")
    print(f"  Ticks con Binance data:   {binance_ok:,}")
    print(f"  Mercados completos:       {mkts_done:,}")
    if wr is not None:
        print(f"  Win rate UP (YES):        {wr:.4%}")
        print(f"  Win rate DOWN (NO):       {1 - wr:.4%}")
    if avg_edge is not None:
        print(f"  Edge teórico medio:       {avg_edge:+.4f}")
    if avg_dist is not None:
        print(f"  |Distancia al strike| μ:  {avg_dist:.4f}%")
    if avg_vol is not None:
        print(f"  Volatilidad 5m media:     {avg_vol:.8f}")
    if t_range[0]:
        print(f"  Primer tick:              {t_range[0][:19]}")
        print(f"  Último tick:              {t_range[1][:19]}")
    print(f"{'='*60}")

    # ── Mini análisis de edge por franja de tiempo ────────────────────
    if resolved > 50:
        print(f"\n  EDGE POR FRANJA DE SEGUNDOS RESTANTES:")
        print(f"  {'Franja':<20} {'Ticks':>8} {'WR':>8} {'Edge μ':>10} {'Prob μ':>10}")
        print(f"  {'-'*56}")
        for lo, hi in [(0, 30), (30, 60), (60, 120), (120, 180), (180, 300)]:
            sql = f"""
                SELECT COUNT(*), AVG(resultado_final), AVG(edge_teorico), AVG(prob_teorica_up)
                FROM ticks
                WHERE resultado_final IS NOT NULL
                  AND segundos_restantes >= {lo} AND segundos_restantes < {hi}
            """
            row = await q(sql)
            if row[0] and row[0] > 0:
                print(f"  {lo:>3}-{hi:<3}s             {row[0]:>8} {row[1]:>8.4f} {row[2]:>+10.4f} {row[3]:>10.4f}")
        print()

    await db.close()


# ─────────────────────────────────────────────────────────────────────────────
# EXPORTACIÓN SPLIT — CSVs de ≤25MB para subir a Claude
# ─────────────────────────────────────────────────────────────────────────────

MAX_FILE_SIZE_MB = 25
EXPORT_DIR       = "exports"


async def export_split(db_path: str, only_resolved: bool = True):
    """
    Exporta la DB a múltiples CSVs de ≤25MB.
    - --export-split  → solo ticks resueltos (los útiles para análisis)
    - --export-all    → todos los ticks (incluyendo pendientes de resolución)
    """
    if not Path(db_path).exists():
        print(f"No existe {db_path}.")
        return

    Path(EXPORT_DIR).mkdir(exist_ok=True)

    db = await aiosqlite.connect(db_path)

    where = "WHERE resultado_final IS NOT NULL" if only_resolved else ""
    label = "resueltos" if only_resolved else "todos"

    async with db.execute(f"SELECT COUNT(*) FROM ticks {where}") as c:
        total = (await c.fetchone())[0]

    if total == 0:
        print(f"No hay ticks {label} en la DB.")
        await db.close()
        return

    print(f"\n  Exportando {total:,} ticks ({label})...")

    # Leer todo en pandas
    async with db.execute(f"SELECT * FROM ticks {where} ORDER BY unix_ts") as cursor:
        rows = await cursor.fetchall()
        cols = [d[0] for d in cursor.description]

    await db.close()

    df = pd.DataFrame(rows, columns=cols)

    # Eliminar columnas pesadas y redundantes para ahorrar espacio
    drop_cols = ["token_id_yes", "token_id_no", "id"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    # Estimar tamaño por fila escribiendo una muestra
    sample = df.head(500)
    sample_csv = sample.to_csv(index=False)
    bytes_per_row = len(sample_csv.encode("utf-8")) / max(len(sample), 1)
    rows_per_file = int((MAX_FILE_SIZE_MB * 1024 * 1024) / bytes_per_row)
    rows_per_file = max(rows_per_file, 1000)  # mínimo 1000 filas por archivo

    n_files = math.ceil(len(df) / rows_per_file)
    prefix  = "cross_resolved" if only_resolved else "cross_all"

    files_created = []
    for i in range(n_files):
        chunk    = df.iloc[i * rows_per_file : (i + 1) * rows_per_file]
        filename = f"{prefix}_part_{i+1:03d}.csv"
        filepath = f"{EXPORT_DIR}/{filename}"
        chunk.to_csv(filepath, index=False)

        size_mb = Path(filepath).stat().st_size / (1024 * 1024)
        files_created.append((filename, len(chunk), size_mb))
        print(f"  ✓ {filename}  ({len(chunk):,} filas, {size_mb:.1f} MB)")

    # Resumen
    total_size = sum(f[2] for f in files_created)
    print(f"\n{'='*55}")
    print(f"  EXPORT COMPLETADO")
    print(f"{'='*55}")
    print(f"  Archivos:    {len(files_created)}")
    print(f"  Total filas: {len(df):,}")
    print(f"  Total size:  {total_size:.1f} MB")
    print(f"  Directorio:  ./{EXPORT_DIR}/")
    print(f"  Cada archivo ≤{MAX_FILE_SIZE_MB}MB → listo para subir a Claude")
    print(f"{'='*55}\n")


async def export_summary(db_path: str):
    """
    Exporta 1 fila por vela (mercado) — ultra compacto.
    Cada fila tiene estadísticas agregadas de todos los ticks de esa vela:
    medias, min, max, valores al inicio y al final, resultado.

    Esto reduce ~20 ticks/vela a 1 fila → cabe MUCHA más historia.
    """
    if not Path(db_path).exists():
        print(f"No existe {db_path}.")
        return

    Path(EXPORT_DIR).mkdir(exist_ok=True)

    db = await aiosqlite.connect(db_path)

    # Solo velas resueltas con data de Binance
    async with db.execute("""
        SELECT * FROM ticks
        WHERE resultado_final IS NOT NULL AND btc_price IS NOT NULL
        ORDER BY unix_ts
    """) as cursor:
        rows = await cursor.fetchall()
        cols = [d[0] for d in cursor.description]

    await db.close()

    if not rows:
        print("No hay ticks resueltos con data de Binance.")
        return

    df = pd.DataFrame(rows, columns=cols)

    print(f"  Procesando {len(df):,} ticks en {df['market_id'].nunique()} velas...")

    summaries = []

    for market_id, group in df.groupby("market_id"):
        g = group.sort_values("unix_ts")

        if len(g) < 2:
            continue

        first = g.iloc[0]
        last  = g.iloc[-1]
        mid   = g.iloc[len(g) // 2]

        summary = {
            # Identificación
            "market_id":             market_id,
            "market_slug":           first["market_slug"],
            "n_ticks":               len(g),
            "timestamp_start":       first["timestamp_utc"],
            "timestamp_end":         last["timestamp_utc"],

            # Resultado
            "resultado_final":       int(first["resultado_final"]),

            # Strike y precio
            "btc_open":              first["btc_open"],
            "btc_price_start":       first["btc_price"],
            "btc_price_mid":         mid["btc_price"],
            "btc_price_end":         last["btc_price"],
            "btc_price_min":         g["btc_price"].min(),
            "btc_price_max":         g["btc_price"].max(),

            # Distancia al strike
            "dist_pct_start":        first["distance_pct"],
            "dist_pct_mid":          mid["distance_pct"],
            "dist_pct_end":          last["distance_pct"],
            "dist_pct_mean":         g["distance_pct"].mean(),
            "dist_pct_min":          g["distance_pct"].min(),
            "dist_pct_max":          g["distance_pct"].max(),
            "dist_pct_std":          g["distance_pct"].std(),

            # Poly YES
            "yes_ask_start":         first["yes_best_ask"],
            "yes_ask_mid":           mid["yes_best_ask"],
            "yes_ask_end":           last["yes_best_ask"],
            "yes_ask_mean":          g["yes_best_ask"].mean(),
            "yes_mid_mean":          g["yes_mid_price"].mean(),
            "yes_spread_mean":       g["yes_spread"].mean(),
            "yes_liq_ask_mean":      g["yes_ask_liquidity"].mean(),

            # Poly NO
            "no_ask_start":          first["no_best_ask"],
            "no_ask_mid":            mid["no_best_ask"],
            "no_ask_end":            last["no_best_ask"],
            "no_ask_mean":           g["no_best_ask"].mean(),
            "no_mid_mean":           g["no_mid_price"].mean(),
            "no_spread_mean":        g["no_spread"].mean(),
            "no_liq_ask_mean":       g["no_ask_liquidity"].mean(),

            # RSI
            "rsi_start":             first.get("rsi_14"),
            "rsi_mid":               mid.get("rsi_14"),
            "rsi_end":               last.get("rsi_14"),
            "rsi_mean":              g["rsi_14"].mean(),

            # MACD
            "macd_hist_start":       first.get("macd_hist"),
            "macd_hist_end":         last.get("macd_hist"),
            "macd_hist_mean":        g["macd_hist"].mean(),

            # Bollinger
            "bb_position_start":     first.get("bb_position"),
            "bb_position_end":       last.get("bb_position"),
            "bb_position_mean":      g["bb_position"].mean(),

            # Volatilidad
            "vol_1m_mean":           g["volatility_1m"].mean(),
            "vol_5m_mean":           g["volatility_5m"].mean(),
            "vol_5m_start":          first.get("volatility_5m"),
            "vol_5m_end":            last.get("volatility_5m"),

            # Momentum
            "momentum_30s_start":    first.get("momentum_30s"),
            "momentum_30s_end":      last.get("momentum_30s"),
            "momentum_60s_mean":     g["momentum_60s"].mean(),

            # Volumen
            "volume_1m_mean":        g["volume_1m"].mean(),
            "vwap_5m_mean":          g["vwap_5m"].mean(),
            "buy_vol_ratio_mean":    g["buy_volume_ratio"].mean(),

            # EMAs
            "ema_9_start":           first.get("ema_9"),
            "ema_21_start":          first.get("ema_21"),
            "ema_9_end":             last.get("ema_9"),
            "ema_21_end":            last.get("ema_21"),
            "ema_cross":             1 if (first.get("ema_9") or 0) < (first.get("ema_21") or 0) and
                                          (last.get("ema_9") or 0) > (last.get("ema_21") or 0) else
                                    -1 if (first.get("ema_9") or 0) > (first.get("ema_21") or 0) and
                                          (last.get("ema_9") or 0) < (last.get("ema_21") or 0) else 0,

            # ATR
            "atr_mean":              g["atr_14"].mean(),

            # Edge teórico
            "edge_start":            first.get("edge_teorico"),
            "edge_mid":              mid.get("edge_teorico"),
            "edge_end":              last.get("edge_teorico"),
            "edge_mean":             g["edge_teorico"].mean(),
            "edge_max":              g["edge_teorico"].max(),
            "edge_min":              g["edge_teorico"].min(),
            "prob_teorica_mean":     g["prob_teorica_up"].mean(),
            "implied_prob_mean":     g["implied_prob_up"].mean(),

            # Segundos capturados
            "secs_first_tick":       first["segundos_restantes"],
            "secs_last_tick":        last["segundos_restantes"],
        }

        summaries.append(summary)

    df_summary = pd.DataFrame(summaries)
    filepath   = f"{EXPORT_DIR}/cross_summary_by_candle.csv"
    df_summary.to_csv(filepath, index=False)

    size_mb = Path(filepath).stat().st_size / (1024 * 1024)
    print(f"\n{'='*55}")
    print(f"  EXPORT SUMMARY COMPLETADO")
    print(f"{'='*55}")
    print(f"  Velas:      {len(summaries):,}")
    print(f"  Columnas:   {len(df_summary.columns)}")
    print(f"  Tamaño:     {size_mb:.2f} MB")
    print(f"  Archivo:    {filepath}")
    print(f"  (1 fila = 1 vela de 5min con stats agregadas)")
    print(f"{'='*55}\n")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    if "--debug" in sys.argv:
        await debug_api()
        return
    if "--stats" in sys.argv:
        await quick_stats(CONFIG["DB_PATH"])
        return
    if "--export-split" in sys.argv:
        await export_split(CONFIG["DB_PATH"], only_resolved=True)
        return
    if "--export-all" in sys.argv:
        await export_split(CONFIG["DB_PATH"], only_resolved=False)
        return
    if "--export-summary" in sys.argv:
        await export_summary(CONFIG["DB_PATH"])
        return

    loop     = asyncio.get_event_loop()
    shutdown = asyncio.Event()

    def handle_signal():
        logger.info("Ctrl+C. Finalizando...")
        shutdown.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    db      = await init_db(CONFIG["DB_PATH"])
    headers = {"Accept": "application/json", "User-Agent": "btc5m-cross-collector/2.0"}

    async with aiohttp.ClientSession(headers=headers) as session:
        tracker      = BTCMarketTracker(db, session)
        tracker_task = asyncio.create_task(tracker.run_forever())
        await shutdown.wait()
        tracker_task.cancel()
        try:
            await tracker_task
        except asyncio.CancelledError:
            pass

    await db.close()
    logger.info("Cross Collector parado.")


if __name__ == "__main__":
    asyncio.run(main())
