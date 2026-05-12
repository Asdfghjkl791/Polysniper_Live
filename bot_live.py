#!/usr/bin/env python3
# PolySniper LIVE v2.0 - Using py-clob-client-v2 for new deposit wallets
# - signature_type=3 (POLY_1271) for email-signup accounts
# - Diagnostic mode: connects, reads balance, no trading yet
# - All times in EST

import os
import time
import logging
import requests
from datetime import datetime, timezone, timedelta

try:
    from py_clob_client_v2 import ClobClient, OrderArgs, PartialCreateOrderOptions, ApiCreds
    from py_clob_client_v2.order_builder.constants import BUY
    V2_AVAILABLE = True
    IMPORT_ERROR = None
except ImportError as e:
    V2_AVAILABLE = False
    IMPORT_ERROR = str(e)

TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
POLY_PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "").strip()
POLY_FUNDER      = os.environ.get("POLY_FUNDER", "").strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("polysniper-live-v2")

state = {
    "balance_usd": 0.0,
    "connected": False,
    "client_config": None,
}

clob_client = None
update_offset = None


def est_now():
    return datetime.now(timezone.utc) - timedelta(hours=4)


def est_str():
    return est_now().strftime("%H:%M EST")


def tg(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=8
        )
        log.info(f"[TG] {msg[:100]}")
    except Exception as e:
        log.error(f"TG error: {e}")


def try_v2_connection():
    """Use py-clob-client-v2 with signature_type=3 for deposit wallets."""
    global clob_client
    
    host = "https://clob.polymarket.com"
    chain_id = 137  # Polygon
    
    try:
        log.info("STEP 1: Creating temporary client for L1 auth...")
        temp_client = ClobClient(
            host=host,
            chain_id=chain_id,
            key=POLY_PRIVATE_KEY,
        )
        
        log.info("STEP 2: Generating API credentials...")
        creds = temp_client.create_or_derive_api_key()
        log.info(f"DEBUG creds type: {type(creds).__name__}")
        log.info(f"DEBUG creds: {str(creds)[:200]}")
        
        log.info("STEP 3: Creating authenticated client with sig_type=3 (POLY_1271)...")
        clob_client = ClobClient(
            host=host,
            chain_id=chain_id,
            key=POLY_PRIVATE_KEY,
            creds=creds,
            signature_type=3,
            funder=POLY_FUNDER,
        )
        
        log.info("STEP 4: Testing connection - fetching markets...")
        try:
            # Try a simple read call first
            server_time = clob_client.get_server_time()
            log.info(f"DEBUG server_time: {server_time}")
        except Exception as e:
            log.warning(f"get_server_time failed: {e}")
        
        log.info("STEP 5: Fetching balance...")
        try:
            # v2 might have different balance method
            if hasattr(clob_client, 'get_balance_allowance'):
                from py_clob_client_v2 import BalanceAllowanceParams, AssetType
                params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                bal_resp = clob_client.get_balance_allowance(params)
                log.info(f"DEBUG bal_resp type: {type(bal_resp).__name__}")
                log.info(f"DEBUG bal_resp value: {str(bal_resp)[:300]}")
                
                if isinstance(bal_resp, dict):
                    bal_raw = bal_resp.get("balance", 0)
                else:
                    bal_raw = getattr(bal_resp, "balance", 0)
                bal_usd = float(bal_raw) / 1_000_000
            elif hasattr(clob_client, 'get_balance'):
                bal_raw = clob_client.get_balance()
                log.info(f"DEBUG get_balance returned: {bal_raw}")
                bal_usd = float(bal_raw) / 1_000_000
            else:
                # List available methods for debugging
                methods = [m for m in dir(clob_client) if not m.startswith('_')]
                log.info(f"DEBUG available methods: {methods[:20]}")
                bal_usd = 0
        except Exception as e:
            log.error(f"Balance fetch error: {e}")
            bal_usd = 0
        
        log.info(f"FINAL Balance: ${bal_usd:.2f}")
        state["balance_usd"] = bal_usd
        state["connected"] = True
        state["client_config"] = "v2 + sig_type=3 + funder"
        return bal_usd
        
    except Exception as e:
        log.error(f"v2 connection failed: {e}")
        return None


def init_clob():
    log.info("=" * 50)
    log.info("INIT CLOB v2 - DIAGNOSTIC INFO")
    log.info(f"DEBUG poly_key length: {len(POLY_PRIVATE_KEY)}")
    log.info(f"DEBUG poly_key starts: {POLY_PRIVATE_KEY[:6] if POLY_PRIVATE_KEY else 'EMPTY'}")
    log.info(f"DEBUG funder: {POLY_FUNDER}")
    log.info(f"DEBUG v2_available: {V2_AVAILABLE}")
    log.info("=" * 50)
    
    if not V2_AVAILABLE:
        tg(f"❌ <b>py-clob-client-v2 import failed</b>\n<code>{IMPORT_ERROR}</code>\n\nCheck requirements.txt")
        return False
    
    if not POLY_PRIVATE_KEY:
        tg("❌ <b>POLY_PRIVATE_KEY missing</b>")
        return False
    
    if not POLY_FUNDER:
        tg("❌ <b>POLY_FUNDER missing</b>")
        return False
    
    bal = try_v2_connection()
    
    if bal is not None and bal > 0:
        tg(
            f"✅ <b>Connected to Polymarket (v2)!</b>\n\n"
            f"💰 Balance: <b>${bal:.2f}</b>\n"
            f"Config: sig_type=3 (POLY_1271)\n"
            f"🕐 {est_str()}"
        )
        return True
    elif bal is not None:
        tg(
            f"⚠️ <b>Connected but balance is $0</b>\n\n"
            f"Library v2 worked but balance returns 0.\n"
            f"Check Railway logs for DEBUG output.\n"
            f"🕐 {est_str()}"
        )
        return False
    else:
        tg(
            f"❌ <b>v2 connection failed</b>\n\n"
            f"Check Railway logs.\n"
            f"🕐 {est_str()}"
        )
        return False


def handle_commands():
    global update_offset
    try:
        params = {"timeout": 1, "allowed_updates": ["message"]}
        if update_offset:
            params["offset"] = update_offset
        res = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params=params, timeout=5
        )
        updates = res.json().get("result", [])
    except:
        return
    
    for update in updates:
        update_offset = update["update_id"] + 1
        msg  = update.get("message", {})
        text = msg.get("text", "").strip().lower()
        cid  = str(msg.get("chat", {}).get("id", ""))
        if cid != str(TELEGRAM_CHAT_ID):
            continue
        
        if text == "/status":
            tg(
                f"🟢 <b>Live Bot v2 Status</b>\n\n"
                f"Connected: {state['connected']}\n"
                f"💰 Balance: ${state['balance_usd']:.2f}\n"
                f"Config: {state['client_config'] or 'Not set'}\n"
                f"🕐 {est_str()}"
            )
        
        elif text == "/balance":
            if state["connected"]:
                bal = try_v2_connection()
                if bal is not None:
                    tg(f"💰 <b>Balance:</b> ${bal:.2f}\n🕐 {est_str()}")
                else:
                    tg("❌ Balance check failed")
            else:
                tg("❌ Not connected to Polymarket")
        
        elif text == "/help":
            tg(
                "🤖 <b>Live Bot v2 Commands</b>\n\n"
                "/status — current status\n"
                "/balance — refresh balance\n"
                "/help — this menu"
            )


def main():
    tg(
        f"🚀 <b>PolySniper LIVE v2.0</b>\n"
        f"📚 Using py-clob-client-v2\n"
        f"🔧 Signature type 3 (POLY_1271)\n\n"
        f"🕐 {est_str()}\n\n"
        f"Connecting to Polymarket..."
    )
    
    init_clob()
    
    log.info("Entering main loop")
    while True:
        try:
            handle_commands()
        except Exception as e:
            log.error(f"Loop error: {e}")
        time.sleep(2)


if __name__ == "__main__":
    main()
