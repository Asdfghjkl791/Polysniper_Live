#!/usr/bin/env python3
# PolySniper LIVE v1.1 - Clean Edition with Diagnostic Logging
# - Tries multiple wallet configs automatically
# - Heavy debug logging to diagnose Polymarket account type
# - All times in EST

import os
import time
import sqlite3
import logging
import requests
from datetime import datetime, timezone, timedelta

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.constants import POLYGON
    from py_clob_client.clob_types import BalanceAllowanceParams, AssetType, OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY
    POLY_AVAILABLE = True
except ImportError as e:
    POLY_AVAILABLE = False
    IMPORT_ERROR = str(e)

TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
POLY_PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "").strip()
POLY_FUNDER      = os.environ.get("POLY_FUNDER", "").strip()

CONFIG = {
    "bet_size":             float(os.environ.get("BET_SIZE", "1.0")),
    "min_balance":          float(os.environ.get("MIN_BALANCE", "5.0")),
    "max_losses_streak":    int(os.environ.get("MAX_LOSSES_STREAK", "2")),
    "max_api_errors":       int(os.environ.get("MAX_API_ERRORS", "3")),
    "entry_window_seconds": int(os.environ.get("ENTRY_SECS", "10")),
    "max_trades_per_day":   int(os.environ.get("MAX_TRADES_DAY", "20")),
    "daily_loss_limit":     float(os.environ.get("DAILY_LOSS_LIMIT", "5.0")),
    "dry_run":              os.environ.get("DRY_RUN", "false").lower() == "true",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("polysniper-live")

state = {
    "balance_usd":           0.0,
    "paused":                False,
    "clob_config":           None,
    "start_time":            datetime.now(timezone.utc),
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


def try_config(sig_type, funder, name):
    """Try one wallet configuration. Returns (balance, response_data) or (None, None) on fail."""
    global clob_client
    host = "https://clob.polymarket.com"
    
    try:
        log.info(f"Trying: {name}")
        
        if funder:
            clob_client = ClobClient(
                host,
                key=POLY_PRIVATE_KEY,
                chain_id=POLYGON,
                signature_type=sig_type,
                funder=funder,
            )
        else:
            clob_client = ClobClient(host, key=POLY_PRIVATE_KEY, chain_id=POLYGON)
        
        # Get signer address for debugging
        try:
            addr = clob_client.get_address()
            log.info(f"DEBUG signer address: {addr}")
        except Exception as e:
            log.info(f"DEBUG get_address failed: {e}")
        
        # Set API credentials
        clob_client.set_api_creds(clob_client.create_or_derive_api_creds())
        
        # Try to fetch balance
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        bal_resp = clob_client.get_balance_allowance(params)
        
        log.info(f"DEBUG bal_resp type: {type(bal_resp).__name__}")
        log.info(f"DEBUG bal_resp value: {str(bal_resp)[:300]}")
        
        # Try to extract balance from response
        bal_raw = 0
        if isinstance(bal_resp, dict):
            bal_raw = bal_resp.get("balance", 0)
        else:
            bal_raw = getattr(bal_resp, "balance", 0)
        
        bal_usd = float(bal_raw) / 1_000_000
        log.info(f"Result: balance=${bal_usd:.2f}")
        
        return bal_usd, bal_resp
        
    except Exception as e:
        log.warning(f"Config {name} threw exception: {e}")
        return None, None


def init_clob():
    """Try multiple wallet configurations until one returns non-zero balance."""
    
    log.info("=" * 50)
    log.info("INIT CLOB - DIAGNOSTIC INFO")
    log.info(f"DEBUG poly_key length: {len(POLY_PRIVATE_KEY)}")
    log.info(f"DEBUG poly_key starts: {POLY_PRIVATE_KEY[:6] if POLY_PRIVATE_KEY else 'EMPTY'}")
    log.info(f"DEBUG funder length: {len(POLY_FUNDER)}")
    log.info(f"DEBUG funder value: {POLY_FUNDER if POLY_FUNDER else 'EMPTY'}")
    log.info(f"DEBUG dry_run: {CONFIG['dry_run']}")
    log.info("=" * 50)
    
    if not POLY_AVAILABLE:
        tg(f"❌ <b>py-clob-client import failed</b>\n<code>{IMPORT_ERROR}</code>")
        return False
    
    if not POLY_PRIVATE_KEY:
        tg("❌ <b>POLY_PRIVATE_KEY missing</b>")
        return False
    
    # Build configurations to try
    configs = []
    
    if POLY_FUNDER:
        configs.append({"sig": 1, "funder": POLY_FUNDER, "name": "Email wallet (sig=1)"})
        configs.append({"sig": 2, "funder": POLY_FUNDER, "name": "Proxy wallet (sig=2)"})
    
    configs.append({"sig": 0, "funder": None, "name": "Standard EOA (sig=0)"})
    
    # Try each config
    best_balance = 0.0
    best_config = None
    
    for cfg in configs:
        bal, resp = try_config(cfg["sig"], cfg["funder"], cfg["name"])
        if bal is not None and bal > best_balance:
            best_balance = bal
            best_config = cfg
    
    if best_balance > 0:
        # Reconnect with the working config (since clob_client got overwritten)
        try_config(best_config["sig"], best_config["funder"], best_config["name"])
        state["balance_usd"] = best_balance
        state["clob_config"] = best_config["name"]
        tg(
            f"✅ <b>Connected to Polymarket!</b>\n\n"
            f"Config: {best_config['name']}\n"
            f"💰 Balance: <b>${best_balance:.2f}</b>\n"
            f"🕐 {est_str()}"
        )
        return True
    
    tg(
        f"⚠️ <b>Connected but balance is $0</b>\n\n"
        f"All wallet configs tried but none show your balance.\n"
        f"Check Railway logs for DEBUG info.\n\n"
        f"Funder: <code>{POLY_FUNDER[:10]}...</code>\n"
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
                f"🟢 <b>Live Bot Status</b>\n\n"
                f"💰 Balance: ${state['balance_usd']:.2f}\n"
                f"Config: {state['clob_config'] or 'Not connected'}\n"
                f"🕐 {est_str()}"
            )
        
        elif text == "/balance":
            if clob_client:
                try:
                    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                    bal_resp = clob_client.get_balance_allowance(params)
                    if isinstance(bal_resp, dict):
                        bal_raw = bal_resp.get("balance", 0)
                    else:
                        bal_raw = getattr(bal_resp, "balance", 0)
                    bal_usd = float(bal_raw) / 1_000_000
                    state["balance_usd"] = bal_usd
                    tg(f"💰 <b>Balance:</b> ${bal_usd:.2f}\n🕐 {est_str()}")
                except Exception as e:
                    tg(f"❌ Balance check failed: <code>{str(e)[:200]}</code>")
            else:
                tg("❌ Not connected to Polymarket")
        
        elif text == "/help":
            tg(
                "🤖 <b>Live Bot Commands</b>\n\n"
                "/status — quick status\n"
                "/balance — refresh balance\n"
                "/help — this menu"
            )


def main():
    tg(
        f"🚀 <b>PolySniper LIVE v1.1</b>\n"
        f"⚠️ <b>🔴 DIAGNOSTIC MODE</b>\n\n"
        f"🕐 {est_str()}\n\n"
        f"Checking Polymarket connection..."
    )
    
    success = init_clob()
    
    if success:
        log.info("Bot initialized successfully")
    else:
        log.warning("Bot connected but balance shows $0")
    
    # Keep the bot running for command handling
    log.info("Entering main loop (command handler only - no trading yet)")
    while True:
        try:
            handle_commands()
        except Exception as e:
            log.error(f"Loop error: {e}")
        time.sleep(2)


if __name__ == "__main__":
    main()
