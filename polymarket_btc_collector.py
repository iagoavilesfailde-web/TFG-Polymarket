"""
╔══════════════════════════════════════════════════════════════════════════════╗
║         POLYMARKET BTC 5M — DATA COLLECTOR FOR CALIBRATION STUDY           ║
║                                                                              ║
║  Tracking y resolución son CONCURRENTES:                                    ║
║  - Al cerrar una vela → resolución corre en background                      ║
║  - El tracker empieza la siguiente vela desde el segundo 1                  ║
║  - Resolución: Polymarket/Chainlink (outcomePrices via Gamma API)           ║
║                                                                              ║
║  Ambos lados grabados por tick:                                              ║
║  YES: best_ask, best_bid, mid_price, spread, ask_liq, bid_liq               ║
║  NO:  best_ask, best_bid, mid_price, spread, ask_liq, bid_liq               ║
║                                                                              ║
║  Ejecución real:  comprar YES → yes_best_ask                                ║
║                   comprar NO  → no_best_ask                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

INSTALACIÓN:
    pip install aiohttp aiosqlite pandas loguru

USO:
    python polymarket_btc_collector.py            <- recolección continua
    python polymarket_btc_collector.py --debug    <- diagnóstico completo
    python polymarket_btc_collector.py --stats    <- estadísticas de la DB
"""

import asyncio
import aiosqlite
import aiohttp
import pandas as pd
import json
import time
import signal
import sys
from datetime import datetime, timezone
from loguru import logger
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

CONFIG = {
    "GAMMA_API":         "https://gamma-api.polymarket.com",
    "CLOB_API":          "https://clob.polymarket.com",
    "MARKET_SLUG_KEY":   "btc-updown-5m",
    "TICK_INTERVAL":     3,      # segundos entre capturas
    "MIN_SECS_TO_TRACK": 15,     # mínimo de segundos para iniciar captura
    "DB_PATH":           "btc_5m_data.db",
    "CSV_PATH":          "btc_5m_data.csv",
    "CSV_EXPORT_EVERY":  10,
    "MAX_RETRIES":       4,
    "RETRY_DELAY":       6,
    # Resolución en background: reintentar cada N segundos durante max M minutos
    "RESOLVE_INTERVAL":  20,     # segundos entre intentos de resolución
    "RESOLVE_MAX_WAIT":  900,    # 15 minutos máximo esperando resolución
    "LOG_LEVEL":         "INFO",
}

logger.remove()
logger.add(sys.stderr, level=CONFIG["LOG_LEVEL"],
           format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}")
logger.add("collector.log", rotation="50 MB", retention="7 days", level="DEBUG")


# ─────────────────────────────────────────────────────────────────────────────
# BASE DE DATOS
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id           TEXT NOT NULL,
    market_slug         TEXT,
    token_id_yes        TEXT,
    token_id_no         TEXT,
    timestamp_utc       TEXT NOT NULL,
    unix_ts             REAL NOT NULL,
    segundos_restantes  REAL,
    yes_best_ask        REAL,
    yes_best_bid        REAL,
    yes_mid_price       REAL,
    yes_spread          REAL,
    yes_ask_liquidity   REAL,
    yes_bid_liquidity   REAL,
    no_best_ask         REAL,
    no_best_bid         REAL,
    no_mid_price        REAL,
    no_spread           REAL,
    no_ask_liquidity    REAL,
    no_bid_liquidity    REAL,
    resultado_final     INTEGER DEFAULT NULL
);
CREATE INDEX IF NOT EXISTS idx_market_id ON ticks(market_id);
CREATE INDEX IF NOT EXISTS idx_resultado  ON ticks(resultado_final);
CREATE INDEX IF NOT EXISTS idx_unix       ON ticks(unix_ts);

CREATE TABLE IF NOT EXISTS markets_log (
    market_id       TEXT PRIMARY KEY,
    slug            TEXT,
    end_unix        REAL,
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
# TIMESTAMPS
# Slug = timestamp de INICIO de la vela. End = start + 300.
# ─────────────────────────────────────────────────────────────────────────────

def current_candle_start() -> int:
    return (int(time.time()) // 300) * 300


def candle_slugs_to_search() -> list[tuple[int, str]]:
    """[(start_ts, slug), ...] — solo vela activa y anterior como fallback."""
    start = current_candle_start()
    key   = CONFIG["MARKET_SLUG_KEY"]
    return [(start, f"{key}-{start}"), (start - 300, f"{key}-{start - 300}")]


# ─────────────────────────────────────────────────────────────────────────────
# PARSEO DE EVENT → MARKET DICT
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

        # Método 1: clobTokenIds
        clob_ids = parse_clob_ids(mkt.get("clobTokenIds"))
        if len(clob_ids) >= 2:
            yes_id, no_id = str(clob_ids[0]), str(clob_ids[1])

        # Método 2: outcomes
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

        # Método 3: tokens
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
# BÚSQUEDA DE MERCADO ACTIVO
# ─────────────────────────────────────────────────────────────────────────────

async def find_active_market(session: aiohttp.ClientSession) -> dict | None:
    """
    Devuelve el mercado BTC 5m con 0 < segundos_restantes <= 300.
    Si ninguno cumple (transición entre velas) devuelve None.
    """
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

    # Solo velas activas (ya empezadas y aún no cerradas)
    active = [(s, m) for s, m in candidates if 0 < s <= 300]
    if not active:
        return None

    active.sort(key=lambda x: x[0])  # la que expira antes
    _, best = active[0]
    logger.info(f"Mercado activo: {best['slug']} | {best['end_unix'] - now_ts:.0f}s restantes")
    return best


# ─────────────────────────────────────────────────────────────────────────────
# ORDER BOOK
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
# RESOLUCIÓN EN BACKGROUND
#
# Lee outcomePrices del evento via /events?slug=
# Polymarket lo publica cuando Chainlink confirma el resultado.
# Esta función corre como tarea asyncio separada — NO bloquea el tracker.
# ─────────────────────────────────────────────────────────────────────────────

async def resolve_via_polymarket(session: aiohttp.ClientSession,
                                  slug: str) -> int | None:
    """
    Consulta Polymarket (Gamma API / Chainlink) para saber si la vela cerró UP o DOWN.
    Lee outcomePrices del mercado ya resuelto.
    Returns: 1 (UP), 0 (DOWN), None (sin resolver aún o error)
    """
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
                # op[0] = precio final UP, op[1] = precio final DOWN
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
    """
    Resuelve la vela usando la resolución real de Polymarket (Chainlink).
    Corre como asyncio.create_task() — no bloquea el tracker.

    Fuente: Gamma API /events?slug= → outcomePrices (publicado tras confirmación Chainlink).
    """
    # Esperar cierre + margen inicial para que Polymarket publique la resolución
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
# TRACKER PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

class BTCMarketTracker:

    def __init__(self, db: aiosqlite.Connection, session: aiohttp.ClientSession):
        self.db                = db
        self.session           = session
        self.buffer            = []
        self.markets_completed = 0
        self._seen_slugs       = set()

    async def run_forever(self):
        logger.info("=" * 60)
        logger.info("  POLYMARKET BTC 5M COLLECTOR — INICIADO")
        logger.info("  Resolucion: Polymarket/Chainlink (Gamma API)")
        logger.info("  Ctrl+C para parar limpiamente")
        logger.info("=" * 60)

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
        # ── 1. Buscar mercado activo ──────────────────────────────────────────
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
        await self._log_market(market_id, slug, end_unix)

        # ── 2. Lanzar resolución en background (no bloquea) ───────────────────
        asyncio.create_task(
            resolve_in_background(self.session, self.db, market_id, slug, end_unix)
        )
        logger.debug(f"  [BG] Resolución de {slug} lanzada en background")

        # ── 3. Trackear ticks hasta cierre de la vela ─────────────────────────
        logger.info(f"  Capturando {slug} | {secs_left:.0f}s restantes")
        await self._track_market(market_id, slug, yes_id, no_id, end_unix)

        # ── 4. Al cerrar: pasar directamente a la siguiente vela ─────────────
        # La resolución sigue corriendo en background sin bloquear nada
        self.markets_completed += 1
        if self.markets_completed % CONFIG["CSV_EXPORT_EVERY"] == 0:
            await self._export_csv()

        # Limpiar slugs viejos
        if len(self._seen_slugs) > 100:
            self._seen_slugs = set(list(self._seen_slugs)[-50:])

    async def _track_market(self, market_id: str, slug: str,
                             yes_id: str, no_id: str, end_unix: float):
        tick_count = 0

        while True:
            now       = time.time()
            secs_left = end_unix - now

            if secs_left <= 0:
                logger.info(f"  Vela cerrada. {tick_count} ticks capturados → siguiente vela")
                break

            yes_ob, no_ob = await asyncio.gather(
                fetch_orderbook(self.session, yes_id),
                fetch_orderbook(self.session, no_id),
            )

            if yes_ob and no_ob:
                self.buffer.append({
                    "market_id":          market_id,
                    "market_slug":        slug,
                    "token_id_yes":       yes_id,
                    "token_id_no":        no_id,
                    "timestamp_utc":      datetime.now(timezone.utc).isoformat(),
                    "unix_ts":            now,
                    "segundos_restantes": round(secs_left, 2),
                    "yes_best_ask":       yes_ob["best_ask"],
                    "yes_best_bid":       yes_ob["best_bid"],
                    "yes_mid_price":      yes_ob["mid_price"],
                    "yes_spread":         yes_ob["spread"],
                    "yes_ask_liquidity":  yes_ob["ask_liquidity"],
                    "yes_bid_liquidity":  yes_ob["bid_liquidity"],
                    "no_best_ask":        no_ob["best_ask"],
                    "no_best_bid":        no_ob["best_bid"],
                    "no_mid_price":       no_ob["mid_price"],
                    "no_spread":          no_ob["spread"],
                    "no_ask_liquidity":   no_ob["ask_liquidity"],
                    "no_bid_liquidity":   no_ob["bid_liquidity"],
                    "resultado_final":    None,
                })
                tick_count += 1
                logger.info(
                    f"  tick#{tick_count:3d} [{secs_left:5.0f}s] "
                    f"YES ask={yes_ob['best_ask']:.3f} bid={yes_ob['best_bid']:.3f} mid={yes_ob['mid_price']:.3f} "
                    f"| NO  ask={no_ob['best_ask']:.3f} bid={no_ob['best_bid']:.3f} mid={no_ob['mid_price']:.3f} "
                    f"| buf={len(self.buffer)}"
                )
            elif yes_ob and not no_ob:
                logger.warning(f"  [{secs_left:5.0f}s] order book NO vacío — tick descartado")
            else:
                logger.warning(f"  [{secs_left:5.0f}s] order books vacíos")

            if len(self.buffer) >= 30:
                await self._flush_buffer()

            await asyncio.sleep(CONFIG["TICK_INTERVAL"])

        await self._flush_buffer()

    # ── DB ────────────────────────────────────────────────────────────────────

    async def _flush_buffer(self):
        if not self.buffer:
            return
        try:
            await self.db.executemany("""
                INSERT INTO ticks (
                    market_id, market_slug, token_id_yes, token_id_no,
                    timestamp_utc, unix_ts, segundos_restantes,
                    yes_best_ask, yes_best_bid, yes_mid_price, yes_spread,
                    yes_ask_liquidity, yes_bid_liquidity,
                    no_best_ask, no_best_bid, no_mid_price, no_spread,
                    no_ask_liquidity, no_bid_liquidity,
                    resultado_final
                ) VALUES (
                    :market_id, :market_slug, :token_id_yes, :token_id_no,
                    :timestamp_utc, :unix_ts, :segundos_restantes,
                    :yes_best_ask, :yes_best_bid, :yes_mid_price, :yes_spread,
                    :yes_ask_liquidity, :yes_bid_liquidity,
                    :no_best_ask, :no_best_bid, :no_mid_price, :no_spread,
                    :no_ask_liquidity, :no_bid_liquidity,
                    :resultado_final
                )
            """, self.buffer)
            await self.db.commit()
            logger.debug(f"  Flush: {len(self.buffer)} ticks -> DB")
            self.buffer.clear()
        except Exception as e:
            logger.error(f"Error flush: {e}")

    async def _log_market(self, market_id: str, slug: str, end_unix: float):
        await self.db.execute("""
            INSERT OR IGNORE INTO markets_log (market_id, slug, end_unix, created_at)
            VALUES (?, ?, ?, ?)
        """, (market_id, slug, end_unix, datetime.now(timezone.utc).isoformat()))
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
# MODO DEBUG
# ─────────────────────────────────────────────────────────────────────────────

async def debug_api():
    headers = {"Accept": "application/json", "User-Agent": "btc5m-collector/1.0"}
    async with aiohttp.ClientSession(headers=headers) as session:

        now   = time.time()
        start = current_candle_start()
        end   = start + 300

        print(f"\n[1] Timestamps")
        print(f"    Ahora:              {int(now)}")
        print(f"    Inicio vela actual: {start}  (empezó hace {now-start:.0f}s)")
        print(f"    Cierre vela actual: {end}  ({end-now:.0f}s restantes)")
        print(f"    Slugs: {[s for _, s in candle_slugs_to_search()]}")

        print(f"\n[2] Búsqueda de slugs:")
        for start_ts, slug in candle_slugs_to_search():
            end_ts   = start_ts + 300
            secs     = end_ts - now
            data     = await fetch_json(session, f"{CONFIG['GAMMA_API']}/events",
                                        params={"slug": slug})
            if data:
                events = data if isinstance(data, list) else [data]
                mkt = None
                for ev in events:
                    mkt = extract_market_from_event(ev, slug, float(end_ts))
                    if mkt:
                        break
                status = f"OK — yes={mkt['yes_id'][:16]}..." if mkt else "sin tokens"
            else:
                status = "404"
            flag = "<-- ACTIVA" if 0 < secs <= 300 else ("FUTURA" if secs > 300 else "EXPIRADA")
            print(f"    {slug}  ({secs:+.0f}s)  {flag}  ->  {status}")

        print(f"\n[3] find_active_market():")
        market = await find_active_market(session)
        if not market:
            print("    No encontrado. Transición de vela, reintenta en unos segundos.")
            return

        secs = market["end_unix"] - now
        print(f"    slug:      {market['slug']}")
        print(f"    market_id: {market['market_id']}")
        print(f"    yes_id:    {market['yes_id'][:40]}...")
        print(f"    expira en: {secs:.0f}s")

        print(f"\n[4] Order book YES:")
        yes_ob = await fetch_orderbook(session, market["yes_id"])
        if yes_ob:
            print(f"    ask={yes_ob['best_ask']:.4f}  bid={yes_ob['best_bid']:.4f}  "
                  f"mid={yes_ob['mid_price']:.4f}  spread={yes_ob['spread']:.4f}")

        print(f"\n[5] Order book NO:")
        no_ob = await fetch_orderbook(session, market["no_id"])
        if no_ob:
            print(f"    ask={no_ob['best_ask']:.4f}  bid={no_ob['best_bid']:.4f}  "
                  f"mid={no_ob['mid_price']:.4f}  spread={no_ob['spread']:.4f}")

        if yes_ob and no_ob:
            yes_exec = yes_ob['best_ask']
            no_exec  = no_ob['best_ask']
            print(f"\n    Precio ejecución real:")
            print(f"    Comprar YES → pagas {yes_exec:.4f}  (yes_ask)")
            print(f"    Comprar NO  → pagas {no_exec:.4f}  (no_ask)")
            print(f"    Spread YES: {yes_ob['spread']:.4f}  |  Spread NO: {no_ob['spread']:.4f}")

        print(f"\n[6] Estado resolución (vela activa = None es normal):")
        # Simular una consulta de resolución sin espera
        data = await fetch_json(session, f"{CONFIG['GAMMA_API']}/events",
                                params={"slug": market["slug"]})
        resolved_val = None
        if data:
            events = data if isinstance(data, list) else [data]
            for ev in events:
                for mkt in ev.get("markets", [ev]):
                    if isinstance(mkt, dict) and mkt.get("resolved"):
                        op = mkt.get("outcomePrices") or []
                        if isinstance(op, str):
                            op = json.loads(op)
                        if len(op) >= 2:
                            resolved_val = 1 if float(op[0]) >= 0.5 else 0
        print(f"    resultado: {resolved_val}  (None = sin resolver, es normal)")

        if yes_ob and no_ob:
            print(f"\n    TODO OK — listo para recolectar")


# ─────────────────────────────────────────────────────────────────────────────
# MODO STATS
# ─────────────────────────────────────────────────────────────────────────────

async def quick_stats(db_path: str):
    if not Path(db_path).exists():
        print(f"No existe {db_path}.")
        return
    db = await aiosqlite.connect(db_path)
    async with db.execute("SELECT COUNT(*) FROM ticks") as c:
        total = (await c.fetchone())[0]
    async with db.execute(
        "SELECT COUNT(*) FROM ticks WHERE resultado_final IS NOT NULL") as c:
        resolved = (await c.fetchone())[0]
    async with db.execute(
        "SELECT COUNT(DISTINCT market_id) FROM markets_log WHERE resolved=1") as c:
        markets_done = (await c.fetchone())[0]
    async with db.execute(
        "SELECT AVG(resultado_final) FROM ticks WHERE resultado_final IS NOT NULL") as c:
        wr = (await c.fetchone())[0]
    async with db.execute(
        "SELECT AVG(no_mid_price) FROM ticks WHERE resultado_final IS NOT NULL AND no_mid_price IS NOT NULL") as c:
        avg_no_mid = (await c.fetchone())[0]
    async with db.execute(
        "SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM ticks") as c:
        t_min, t_max = await c.fetchone()

    print("\n" + "=" * 50)
    print("  ESTADISTICAS")
    print("=" * 50)
    print(f"  Total ticks:         {total:,}")
    print(f"  Ticks con resultado: {resolved:,}")
    print(f"  Mercados completos:  {markets_done:,}")
    if wr is not None:
        print(f"  Win rate UP (YES):    {wr:.4%}")
        print(f"  Win rate DOWN (NO):   {1-wr:.4%}")
    if avg_no_mid is not None:
        print(f"  NO mid price medio:  {avg_no_mid:.4f}")
    if t_min:
        print(f"  Primer tick:         {t_min[:19]}")
        print(f"  Ultimo tick:         {t_max[:19]}")
    print("=" * 50)
    await db.close()


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

    loop     = asyncio.get_event_loop()
    shutdown = asyncio.Event()

    def handle_signal():
        logger.info("Ctrl+C. Finalizando...")
        shutdown.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass  # Windows

    db      = await init_db(CONFIG["DB_PATH"])
    headers = {"Accept": "application/json", "User-Agent": "btc5m-collector/1.0"}

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
    logger.info("Collector parado.")


if __name__ == "__main__":
    asyncio.run(main())
