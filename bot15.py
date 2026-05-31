import os 
from requests.exceptions import RequestException
from py_clob_client.exceptions import PolyApiException
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, BalanceAllowanceParams, AssetType
from py_clob_client.order_builder.constants import BUY
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
import json

from dotenv import load_dotenv
load_dotenv()

# --- CONFIGURACIÓN BALLENA (Bot Rentable) ---
# shares = 64  <-- ELIMINADO
STAKE_PCT = 0.024  # 2.4% del Bank total
USDC_DECIMALS = 1_000_000

prob_acertar = 0.89
tiempo_antes_cerrar = 601  # Espera 5 min (Clave del éxito)

HOST = "https://clob.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet
PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY")
FUNDER = os.getenv("POLY_FUNDER")

client = ClobClient(
    HOST,  # The CLOB API endpoint
    key=PRIVATE_KEY,  # Your wallet's private key
    chain_id=CHAIN_ID,  # Polygon chain ID (137)
    signature_type=2, 
    funder=FUNDER  # Address that holds your funds
)
client.set_api_creds(client.create_or_derive_api_creds())

open_orders: dict[str, int] = {}

# ==========================
# GESTIÓN DE CAPITAL (NUEVO)
# ==========================
def get_positions_value_usdc(user_addr: str) -> float:
    try:
        resp = requests.get(
            "https://data-api.polymarket.com/value",
            params={"user": user_addr},
            timeout=3
        )
        resp.raise_for_status()
        data = resp.json()
        if not data: return 0.0
        return float(data[0].get("value", 0.0))
    except Exception as e:
        print(f"⚠️ [Stake] Error leyendo portfolio: {e}")
        return 0.0

def get_cash_balance_usdc() -> float:
    try:
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        client.update_balance_allowance(params)
        ba = client.get_balance_allowance(params)
        return int(ba.get("balance", "0")) / USDC_DECIMALS
    except Exception as e:
        print(f"⚠️ [Stake] Error leyendo cash: {e}")
        return 0.0

def get_dynamic_shares(price: float) -> float | None:
    """Calcula cuántas shares comprar para arriesgar exactamente el 2.4% del Equity."""
    positions_v = get_positions_value_usdc(FUNDER)
    cash_v = get_cash_balance_usdc()
    
    if positions_v == 0 and cash_v == 0:
        return None # Error de lectura seguro, mejor no operar
    
    equity = positions_v + cash_v
    stake_usdc = equity * STAKE_PCT
    shares = stake_usdc / price
    
    if shares <= 0: return None
    return round(shares, 2) # Redondeo a 2 decimales

# ==========================
# FUNCIONES ORIGINALES
# ==========================

def get_price(token_id: str, side: str) -> float | None:
    url = f"{HOST}/price"
    params = {"token_id": token_id, "side": side}

    try:
        resp = requests.get(url, params=params, timeout=3)
        resp.raise_for_status()
        data = resp.json()
    except RequestException as e:
        print(f"[Price] Error HTTP para token_id={token_id}: {e}")
        return None

    price = data.get("price")
    if price is None:
        print(f"[Price] Respuesta sin 'price' para token_id={token_id}")
        return None

    try:
        return float(price)
    except (TypeError, ValueError) as e:
        print(f"[Price] Error convirtiendo precio: {e}")
        return None


def fetch_gamma_market(unix_time: int):
    gamma_url = 'https://gamma-api.polymarket.com/markets'
    slug = f'btc-updown-15m-{unix_time}'
    params = {'slug': slug}

    for attempt in range(3):
        try:
            resp = requests.get(gamma_url, params=params, timeout=3)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return None
            return data[0]
        except RequestException:
            time.sleep(1)

    print(f"[Gamma] No se pudo obtener mercado para slug={slug}")
    return None


def get_tokens(unix_time: int) -> tuple[str, str]:
    market = fetch_gamma_market(unix_time)
    if market is None:
        return None, None
    try:
        lista = json.loads(market['clobTokenIds'])
        return (lista[0], lista[1])
    except:
        return None, None


def get_resolution(unix_time: int) -> int:
    market = fetch_gamma_market(unix_time)
    if market is None:
        return 0

    closed = market.get('closed', False)
    if closed:
        try:
            outcome_raw = market['outcomePrices']
            prices = json.loads(outcome_raw)
            first_price = float(prices[0])
            return 1 if int(first_price) == 1 else -1
        except:
            return 0
    else:
        upToken, downToken = get_tokens(unix_time)
        if upToken is None or downToken is None:
            return 0

        up_price = get_price(upToken, "BUY")
        down_price = get_price(downToken, "BUY")

        if up_price is None or down_price is None:
            return 0

        if up_price >= prob_acertar: return 1
        elif down_price >= prob_acertar: return -1
        return 0


def get_signal_for_next_candle(unix_now: int) -> int:
    slot_actual = unix_now - (unix_now % 900)
    slot_anterior = slot_actual - 900

    res_actual = get_resolution(slot_actual)
    res_anterior = get_resolution(slot_anterior)

    if res_actual == -1 and res_anterior == -1: return 1
    if res_actual == 1 and res_anterior == 1: return -1
    return 0


def cancel_expired_orders(unix_now: int) -> None:
    to_cancel = [oid for oid, exp_ts in open_orders.items() if unix_now >= exp_ts]
    for oid in to_cancel:
        try:
            client.cancel(order_id=oid)
            print(f"🗑️ Orden expirada {oid} cancelada.")
        except Exception as e:
            print(f"⚠️ Error cancelando {oid}: {e}")
        finally:
            open_orders.pop(oid, None)


def buy_with_price_cap(token_id: str, max_price: float, max_size: float):
    order = OrderArgs(token_id=token_id, price=max_price, size=max_size, side=BUY)

    MAX_RETRIES = 6
    BASE_SLEEP = 1.0
    CAP_SLEEP = 20.0

    signed = client.create_order(order)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return client.post_order(signed, OrderType.GTC)

        except PolyApiException as e:
            msg_e = str(e)
            retryable = "status_code=500" in msg_e or "could not run the execution" in msg_e
            
            if retryable and attempt < MAX_RETRIES:
                sleep_s = min(CAP_SLEEP, BASE_SLEEP * (2 ** (attempt - 1)))
                print(f"⚠️ Error 500 (Intento {attempt}), reintentando en {sleep_s}s...")
                time.sleep(sleep_s)
                continue
            
            print(f"❌ Error API Polymarket: {e}")
            return None

        except (RequestException, TimeoutError, ConnectionError) as e:
            if attempt < MAX_RETRIES:
                sleep_s = min(CAP_SLEEP, BASE_SLEEP * (2 ** (attempt - 1)))
                print(f"⚠️ Error RED (Intento {attempt}), reintentando en {sleep_s}s...")
                time.sleep(sleep_s)
                continue
            print(f"❌ Error Conexión Fatal: {e}")
            return None
            
        except Exception as e:
            print(f"❌ Error desconocido al comprar: {e}")
            return None


def run_signal_watcher():
    last_alert_slot = None
    print(f"🐋 BOT BTC 15M [DYN STAKE 2.4%] INICIADO")
    # print(f"💰 Stake: {shares} | Prob: {prob_acertar} | Start: Minuto 5 (Wait {900-tiempo_antes_cerrar}s)") # UPDATE LOG

    while True:
        try:
            now_es = datetime.now(ZoneInfo('Europe/Madrid'))
            unix_now = int(now_es.timestamp())

            cancel_expired_orders(unix_now)

            slot_actual = unix_now - (unix_now % 900)
            slot_cierre = slot_actual + 900
            segundos_para_cierre = slot_cierre - unix_now

            # 1) FASE DE SUEÑO (0 a 5 minutos)
            if segundos_para_cierre > tiempo_antes_cerrar:
                dormir = segundos_para_cierre - tiempo_antes_cerrar
                if dormir > 60:
                    print(f"💤 Esperando maduración (Minuto 0-5)... Duermo {dormir}s")
                time.sleep(dormir)
                continue

            # 2) FASE DE OPERACIÓN (Minuto 5 al 15)
            if 0 < segundos_para_cierre <= tiempo_antes_cerrar:
                if last_alert_slot != slot_actual:
                    
                    if segundos_para_cierre % 60 == 0:
                         print(f"🔎 Escaneando señal Ballena... (-{segundos_para_cierre}s)")

                    signal = get_signal_for_next_candle(unix_now)

                    if signal != 0:
                        direccion = "UP" if signal == 1 else "DOWN"
                        slot_siguiente = slot_cierre
                        expiration_ts = slot_siguiente

                        upToken, downToken = get_tokens(slot_siguiente)
                        if upToken and downToken:
                            token = upToken if direccion == 'UP' else downToken
                            
                            # CÁLCULO DINÁMICO DE SHARES
                            shares_dyn = get_dynamic_shares(0.53) # Usamos 0.53 como precio base para el cálculo

                            if shares_dyn:
                                print(f"🚀 SEÑAL CONFIRMADA: BTC -> {direccion}")
                                print(f"💎 Ejecutando {shares_dyn} shares a 0.53 (2.4% Bank)...")
                                
                                resp = buy_with_price_cap(token, 0.53, shares_dyn)

                                if resp and resp.get("orderID"):
                                    open_orders[resp["orderID"]] = expiration_ts
                                    print(f"✅ ORDEN GTC COLOCADA: {resp['orderID']}")
                                else:
                                    print(f"❌ FALLO AL COLOCAR ORDEN")
                            else:
                                print("⚠️ Error calculando Stake Dinámico (Saldo 0 o Error API)")
                        
                        last_alert_slot = slot_actual

                time.sleep(0.5)
                continue

            if segundos_para_cierre <= 0:
                time.sleep(1)
                continue

        except Exception as e:
            print(f"⚠️ Error Loop Principal: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run_signal_watcher()