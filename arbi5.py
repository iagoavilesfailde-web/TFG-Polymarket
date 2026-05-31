import os
import time
import json
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

load_dotenv()

# ==========================================
# ⚙️ CONFIGURACIÓN "RAMBO HEAVY" (CONFIRMACIÓN 0.85)
# ==========================================
FIXED_STAKE = 80.0       # Stake de 80 Shares
ENTRY_PRICE_MAX = 0.51

# --- CONFIGURACIÓN DE FUEGO (AGRESIVA) ---
LADDER_PRICES = [0.54, 0.55] 
BURST_SEQUENCE = [10.0, 10.0, 5.5, 5.5, 5.5, 5.5] # 84 Shares total

# TIEMPOS BTC 5M
TF_SECONDS = 300
LIMITE_ENTRADA_SEG = 260   
INICIO_PURGA_SEG = 261     
MOMENTO_DISPARO_SEG = 265  
FINAL_VELA_SEG = 295

ASSET_SLUGS = {"BTC": "btc"} 
HOST = "https://clob.polymarket.com"
CHAIN_ID = 137
PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY")
FUNDER = os.getenv("POLY_FUNDER")

BINANCE_BASES = ["https://api.binance.com", "https://api1.binance.com", "https://api2.binance.com"]
BINANCE_SYMBOL = "BTCUSDT"
BINANCE_INTERVAL = "5m"

client = ClobClient(HOST, key=PRIVATE_KEY, chain_id=CHAIN_ID, signature_type=2, funder=FUNDER)
client.set_api_creds(client.create_or_derive_api_creds())

status_tracker = {}
active_positions = {}

# ==========================================
# 📊 HELPER: SOLO MIRA K1 Y K2 (BINANCE)
# ==========================================
def check_binance_streak(slot_actual: int):
    # Pedimos historial
    end_time = slot_actual * 1000
    start_time = (slot_actual - (TF_SECONDS * 4)) * 1000 
    params = {"symbol": BINANCE_SYMBOL, "interval": BINANCE_INTERVAL, "startTime": start_time, "endTime": end_time, "limit": 5}
    for base in BINANCE_BASES:
        try:
            r = requests.get(base + "/api/v3/klines", params=params, timeout=1)
            if r.status_code == 200:
                klines = r.json()
                if len(klines) < 2: return 0, 0 
                
                # k1 = T-1 (Última cerrada)
                # k2 = T-2 (Penúltima)
                k1, k2 = klines[-1], klines[-2]
                
                d1 = 1 if float(k1[4]) > float(k1[1]) else -1
                d2 = 1 if float(k2[4]) > float(k2[1]) else -1
                
                # Solo nos importa que estas dos sean iguales
                if d1 == d2: 
                    return d1, 2
        except: continue
    return 0, 0

def get_price(token_id: str, side: str) -> float:
    try:
        r = requests.get(f"{HOST}/price", params={"token_id": token_id, "side": side}, timeout=1)
        return float(r.json().get("price", 0)) if r.status_code == 200 else 0
    except: return 0

def post_order_generic(token_id, price, size, side):
    try:
        safe_size = round(float(size), 2)
        if safe_size <= 0: return None
        order = OrderArgs(token_id=token_id, price=round(float(price), 2), size=safe_size, side=side)
        return client.post_order(client.create_order(order), OrderType.GTC)
    except Exception as e:
        if "not enough balance" in str(e):
            pass 
        else:
            print(f"❌ Error API ({side} @ {price}): {e}")
        return None

def cancel_order_blind(oid):
    try: 
        client.cancel(oid)
        print("✅ Orden cancelada (comando enviado).")
    except Exception as e:
        print(f"⚠️ Cancelación rechazada (¿Ya llena?): {e}")

# ==========================================
# 🧠 LÓGICA RAMBO HYBRID
# ==========================================
def run_rambo_hybrid():
    print(f"🦅 BOT BTC 5M (HYBRID: BINANCE 2 + POLY 0.85) | Stake: {FIXED_STAKE}")
    print(f"🎯 Lógica: k2==k1 (Binance) Y Actual > 0.85 (Poly) -> BUY NEXT")
    
    while True:
        try:
            unix_now = int(datetime.now(ZoneInfo('Europe/Madrid')).timestamp())
            slot_actual = unix_now - (unix_now % TF_SECONDS)
            tiempo_transcurrido = unix_now - slot_actual
            
            key = ("BTC", slot_actual)
            if key not in status_tracker:
                status_tracker[key] = {
                    "bought": False, 
                    "buy_oid": None,
                    "rafagas_lanzadas": False,
                    "target_token": None,
                    "purged": False 
                }

            # 1. ENTRADA (0:00 - 4:20)
            if tiempo_transcurrido <= LIMITE_ENTRADA_SEG:
                if not status_tracker[key]["bought"]:
                    
                    # PASO A: Miramos el pasado en Binance (k1 y k2)
                    streak_dir, streak_len = check_binance_streak(slot_actual)
                    
                    if streak_len >= 2:
                        # PASO B: Miramos el PRESENTE en Polymarket (Vela Actual)
                        slug_t = f"{ASSET_SLUGS['BTC']}-updown-5m-{slot_actual}"
                        try:
                            r_t = requests.get("https://gamma-api.polymarket.com/markets", params={"slug": slug_t}, timeout=1).json()
                            if r_t:
                                tokens_t = json.loads(r_t[0]["clobTokenIds"])
                                
                                # Verificamos FUERZA ACTUAL (> 0.85) en la dirección de la racha
                                # Si streak es UP (1) -> Miramos precio BUY del token UP
                                # Si streak es DOWN (-1) -> Miramos precio BUY del token DOWN
                                
                                price_check = 0.0
                                if streak_dir == 1: # UP
                                    price_check = get_price(tokens_t[0], 'buy')
                                else: # DOWN
                                    price_check = get_price(tokens_t[1], 'buy')
                                
                                # 🔴 LA CONDICIÓN MAESTRA:
                                if price_check >= 0.85:
                                    
                                    signal = streak_dir
                                    slug_next = f"{ASSET_SLUGS['BTC']}-updown-5m-{slot_actual + TF_SECONDS}"
                                    r_next = requests.get("https://gamma-api.polymarket.com/markets", params={"slug": slug_next}, timeout=1).json()
                                    
                                    if r_next:
                                        target_token = json.loads(r_next[0]["clobTokenIds"])[0 if signal == 1 else 1]
                                        dir_str = "UP" if signal == 1 else "DOWN"
                                        
                                        print(f"🚀 SEÑAL FUERTE! (Binance x2 + Poly {price_check}) -> Entrando {FIXED_STAKE} ({dir_str})...")
                                        resp = post_order_generic(target_token, ENTRY_PRICE_MAX, FIXED_STAKE, BUY)
                                        
                                        if resp and resp.get("orderID"):
                                            status_tracker[key]["buy_oid"] = resp["orderID"]
                                            status_tracker[key]["bought"] = True
                                            status_tracker[key]["target_token"] = target_token
                                            active_positions[key] = {"token": target_token}
                                else:
                                    # Info de depuración para que sepas por qué no entra
                                    # print(f"👀 Racha OK, pero actual débil: {price_check} (Min 0.85)")
                                    pass

                        except: pass

            # 2. PURGA CIEGA (4:21)
            elif tiempo_transcurrido >= INICIO_PURGA_SEG and tiempo_transcurrido < MOMENTO_DISPARO_SEG:
                if status_tracker[key]["bought"] and not status_tracker[key]["purged"]:
                    if status_tracker[key]["buy_oid"]:
                        print(f"💀 4:21 - PURGA: Cancelando compra...")
                        cancel_order_blind(status_tracker[key]["buy_oid"])
                        status_tracker[key]["purged"] = True 

            # 3. FUEGO (4:25) - Secuencia Heavy
            if tiempo_transcurrido >= MOMENTO_DISPARO_SEG and tiempo_transcurrido < FINAL_VELA_SEG:
                if status_tracker[key]["bought"] and not status_tracker[key]["rafagas_lanzadas"]:
                    print(f"⏰ 4:25 - ¡RAMBO HEAVY! (Secuencia: {BURST_SEQUENCE})")
                    target_token = status_tracker[key]["target_token"]
                    
                    for i, burst_size in enumerate(BURST_SEQUENCE):
                        print(f"   >>> RÁFAGA {i+1} (Tamaño: {burst_size}) <<<")
                        for price_target in LADDER_PRICES:
                            s_resp = post_order_generic(target_token, price_target, burst_size, SELL)
                            if s_resp and s_resp.get("orderID"):
                                print(f"      -> ✅ Limit colocada: {burst_size} @ {price_target}")
                            time.sleep(0.4) 
                        
                    status_tracker[key]["rafagas_lanzadas"] = True
            
            elif tiempo_transcurrido >= FINAL_VELA_SEG:
                if key in active_positions: del active_positions[key]

            time.sleep(1)
        except Exception as e:
            print(f"⚠️ Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run_rambo_hybrid()