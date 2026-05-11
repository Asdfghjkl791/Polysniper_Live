#!/usr/bin/env python3
# PolySniper LIVE v1.0 - Real Polymarket Trading
# - Uses py-clob-client to place real trades on Polymarket
# - All times in EST
# - Conservative safety limits (designed for $10 starting balance)
# - SAME strategy as v3.9 paper bot
#
# REQUIRED ENV VARS:
#   TELEGRAM_TOKEN
#   TELEGRAM_CHAT_ID
#   POLY_PRIVATE_KEY  (your wallet's private key, starts with 0x)
#
# OPTIONAL ENV VARS:
#   BET_SIZE (default 1.0)
#   MIN_BALANCE (default 5.0 - auto-stop if balance drops below)
#   MAX_LOSSES_STREAK (default 2 - stop after N losses in a row)

import os
import time
import sqlite3
import logging
import requests
from datetime import datetime, timezone, timedelta
from collections import deque

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.constants import POLYGON
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY
    POLY_AVAILABLE = True
except ImportError:
    POLY_AVAILABLE = False

TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
POLY_PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "")

CONFIG = {
    "bet_size":             float(os.environ.get("BET_SIZE", "1.0")),
    "min_balance":          float(os.environ.get("MIN_BALANCE", "5.0")),
    "max_losses_streak":    int(os.environ.get("MAX_LOSSES_STREAK", "2")),
    "max_api_errors":       int(os.environ.get("MAX_API_ERRORS", "3")),
    "entry_window_seconds": int(os.environ.get("ENTRY_SECS", "10")),
    "max_trades_per_day":   int(os.environ.get("MAX_TRADES_DAY", "20")),
    "daily_loss_limit":     float(os.environ.get("DAILY_LOSS_LIMIT", "5.0")),
    "price_interval_secs":  5,
    "heartbeat_interval":   1800,
    "dry_run":              os.environ.get("DRY_RUN", "false").lower() == "true",
}

ASSET_THRESHOLDS_5 = {
    "BTC":  float(os.environ.get("THRESHOLD_BTC_5",  "0.08")),
    "ETH":  float(os.environ.get("THRESHOLD_ETH_5",  "0.15")),  # bumped up for safety
    "SOL":  float(os.environ.get("THRESHOLD_SOL_5",  "0.20")),
    "DOGE": float(os.environ.get("THRESHOLD_DOGE_5", "0.25")),
}

ASSET_LIST = ["BTC", "ETH", "SOL", "DOGE"]
TIMEFRAMES = [5]  # Start with just 5min markets

ASSET_EMOJI = {
    "BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡",
}

COINBASE_PRODUCTS = {
    "BTC":  "BTC-USD", "ETH":  "ETH-USD",
    "SOL":  "SOL-USD", "DOGE": "DOGE-USD",
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
    "pause_reason":          None,
    "trades_today":          0,
    "pnl_today":             0.0,
    "consecutive_losses":    0,
    "api_errors":            0,
    "last_message_time":     time.time(),
    "last_balance_check":    0,
    "start_time":            datetime.now(timezone.utc),
}

windows         = {}
prices          = {}
prices_cc       = {}
prices_cb       = {}
last_prices_time = 0
update_offset    = None
last_heartbeat   = time.time()
clob_client      = None


# ─── EST TIME HELPERS ─────────────────────────────────────────────────────────
def est_now():
    return datetime.now(timezone.utc) - timedelta(hours=4)

def est_str(dt=None):
    if dt is None:
        dt = est_now()
    return dt.strftime("%H:%M EST")

def fmt_time_short(iso_str):
    try:
        dt = datetime.fromisoformat(iso_str)
        est = dt - timedelta(hours=4)
        return est.strftime("%H:%M")
    except:
        return "??:??"


# ─── DATABASE ──────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect("trades_live.db")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entered_at TEXT, asset TEXT, timeframe INTEGER, direction TEXT,
            pct_move REAL, entry_price_cents REAL, shares REAL, cost REAL,
            order_id TEXT, status TEXT, settled_at TEXT, result TEXT,
            payout REAL, profit_loss REAL, balance_after REAL
        )
    """)
    conn.commit()
    conn.close()


def db_insert_trade(t):
    conn = sqlite3.connect("trades_live.db")
    c = conn.cursor()
    c.execute("""
        INSERT INTO trades (entered_at,asset,timeframe,direction,pct_move,
            entry_price_cents,shares,cost,order_id,status,payout,profit_loss)
        VALUES (:entered_at,:asset,:timeframe,:direction,:pct_move,
            :entry_price_cents,:shares,:cost,:order_id,:status,:payout,:profit_loss)
    """, t)
    row_id = c.lastrowid
    conn.commit()
    conn.close()
    return row_id


def db_settle(row_id, result, payout, pl, bal):
    conn = sqlite3.connect("trades_live.db")
    conn.execute("""
        UPDATE trades SET settled_at=?,result=?,payout=?,profit_loss=?,balance_after=?
        WHERE id=?
    """, (est_now().isoformat(), result, payout, pl, bal, row_id))
    conn.commit()
    conn.close()


def db_stats():
    conn = sqlite3.connect("trades_live.db")
    c = conn.cursor()
    c.execute("SELECT COUNT(*), SUM(CASE WHEN result='WON' THEN 1 ELSE 0 END), SUM(profit_loss) FROM trades WHERE result IN ('WON','LOST')")
    r = c.fetchone()
    conn.close()
    return r[0] or 0, r[1] or 0, r[2] or 0.0


def db_recent_trades(limit=10, offset=0):
    conn = sqlite3.connect("trades_live.db")
    c = conn.cursor()
    c.execute("""
        SELECT entered_at, asset, timeframe, direction, pct_move, result, profit_loss
        FROM trades
        ORDER BY id DESC LIMIT ? OFFSET ?
    """, (limit, offset))
    rows = c.fetchall()
    conn.close()
    return rows


# ─── TELEGRAM ──────────────────────────────────────────────────────────────────
def tg(msg):
    try:
        state["last_message_time"] = time.time()
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=8
        )
        log.info(f"[TG] {msg[:80]}")
    except Exception as e:
        log.error(f"TG error: {e}")


def get_updates():
    global update_offset
    try:
        params = {"timeout": 1, "allowed_updates": ["message"]}
        if update_offset:
            params["offset"] = update_offset
        res = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params=params, timeout=5
        )
        return res.json().get("result", [])
    except:
        return []


def handle_commands():
    global update_offset
    for update in get_updates():
        update_offset = update["update_id"] + 1
        msg  = update.get("message", {})
        text = msg.get("text", "").strip().lower()
        cid  = str(msg.get("chat", {}).get("id", ""))
        if cid != str(TELEGRAM_CHAT_ID):
            continue

        if text == "/pause":
            state["paused"] = True
            state["pause_reason"] = "Manual"
            tg("⏸ <b>Paused</b>\nSend /resume to restart.")

        elif text == "/resume":
            state["paused"] = False
            state["pause_reason"] = None
            state["consecutive_losses"] = 0
            state["api_errors"] = 0
            tg("▶️ <b>Resumed</b>")

        elif text == "/stop":
            state["paused"] = True
            state["pause_reason"] = "Emergency stop"
            tg("🛑 <b>EMERGENCY STOP</b>\nAll trading halted. Send /resume to restart.")

        elif text == "/status":
            total, wins, pnl = db_stats()
            wr = f"{wins/total*100:.1f}%" if total > 0 else "—"
            status_emoji = "⏸" if state["paused"] else "🟢"
            status_word = "PAUSED" if state["paused"] else "ACTIVE"
            mode = " (DRY RUN)" if CONFIG["dry_run"] else ""
            tg(
                f"{status_emoji} <b>{status_word}{mode}</b>\n\n"
                f"💰 Balance: <b>${state['balance_usd']:.2f}</b>\n"
                f"🎯 Bet: ${CONFIG['bet_size']}\n"
                f"📊 Today: {state['trades_today']} · ${state['pnl_today']:+.2f}\n"
                f"🏆 All-time: {total} · {wr} · ${pnl:+.2f}\n"
                f"🕐 {est_str()}"
            )

        elif text == "/balance":
            check_balance(force=True)
            tg(f"💰 <b>Balance: ${state['balance_usd']:.2f}</b>\n🕐 {est_str()}")

        elif text == "/history" or text.startswith("/history "):
            page = 1
            parts = text.split()
            if len(parts) > 1:
                try: page = max(1, int(parts[1]))
                except: page = 1
            per_page = 10
            offset = (page - 1) * per_page

            conn = sqlite3.connect("trades_live.db")
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM trades")
            total = c.fetchone()[0] or 0
            conn.close()

            trades = db_recent_trades(per_page, offset)
            max_pages = max(1, (total + per_page - 1) // per_page)

            out = f"📜 <b>History · Page {page}/{max_pages}</b>\n\n"
            if not trades:
                out += "(no trades yet)\n"
            else:
                for row in trades:
                    t, a, tf, d, pct, res, pl = row
                    emoji = ASSET_EMOJI.get(a, "")
                    arrow = "🔺" if d == "UP" else "🔻"
                    res_str = "✅" if res == "WON" else ("❌" if res == "LOST" else "⏳")
                    pl_str = f"${pl:+.2f}" if pl is not None else "pending"
                    out += f"  {fmt_time_short(t)} {emoji}{a} {tf}m {arrow} {pct:+.2f}% {res_str} {pl_str}\n"
            if page < max_pages:
                out += f"\n➡️ /history {page + 1}"
            if page > 1:
                out += f"\n⬅️ /history {page - 1}"
            tg(out)

        elif text == "/help":
            tg(
                "🤖 <b>LIVE Bot Commands</b>\n\n"
                "/status — quick status\n"
                "/balance — refresh real balance\n"
                "/history — recent trades\n"
                "/pause — pause trading\n"
                "/resume — resume\n"
                "/stop — emergency stop"
            )


# ─── POLYMARKET CLIENT ─────────────────────────────────────────────────────────
def init_clob():
    global clob_client
    if not POLY_AVAILABLE:
        tg("❌ <b>py-clob-client not installed!</b>\nAdd to requirements.txt")
        return False
    if not POLY_PRIVATE_KEY:
        tg("❌ <b>POLY_PRIVATE_KEY missing!</b>\nAdd to Railway env vars")
        return False
    try:
        host = "https://clob.polymarket.com"
        chain_id = POLYGON
        clob_client = ClobClient(host, key=POLY_PRIVATE_KEY, chain_id=chain_id)
        # Generate API credentials (cached after first call)
        clob_client.set_api_creds(clob_client.create_or_derive_api_creds())
        log.info("CLOB client initialized")
        return True
    except Exception as e:
        log.error(f"CLOB init failed: {e}")
        tg(f"❌ <b>Polymarket connection failed</b>\n<code>{str(e)[:200]}</code>")
        return False


def check_balance(force=False):
    """Fetch USDC balance from Polymarket."""
    global clob_client
    now = time.time()
    if not force and now - state["last_balance_check"] < 60:
        return state["balance_usd"]
    if not clob_client:
        return 0.0
    try:
        # The actual API call depends on py-clob-client version
        # Trying common methods
        balance_response = clob_client.get_balance_allowance({
            "asset_type": "COLLATERAL",
        })
        # Response is in raw USDC units (6 decimals)
        if isinstance(balance_response, dict) and "balance" in balance_response:
            state["balance_usd"] = float(balance_response["balance"]) / 1_000_000
        elif hasattr(balance_response, "balance"):
            state["balance_usd"] = float(balance_response.balance) / 1_000_000
        state["last_balance_check"] = now
        log.info(f"Balance: ${state['balance_usd']:.2f}")
        return state["balance_usd"]
    except Exception as e:
        log.error(f"Balance check failed: {e}")
        state["api_errors"] += 1
        return state["balance_usd"]


# ─── PRICE FEEDS ───────────────────────────────────────────────────────────────
def fetch_cryptocompare():
    try:
        symbols = ",".join(ASSET_LIST)
        url = f"https://min-api.cryptocompare.com/data/pricemulti?fsyms={symbols}&tsyms=USD"
        res = requests.get(url, timeout=10)
        data = res.json()
        return {sym: float(data[sym]["USD"]) for sym in ASSET_LIST if sym in data and "USD" in data[sym]}
    except Exception as e:
        log.warning(f"CryptoCompare failed: {e}")
        return {}


def fetch_coinbase():
    result = {}
    for asset, product in COINBASE_PRODUCTS.items():
        try:
            url = f"https://api.coinbase.com/v2/prices/{product}/spot"
            res = requests.get(url, timeout=5)
            data = res.json()
            if "data" in data and "amount" in data["data"]:
                result[asset] = float(data["data"]["amount"])
        except:
            pass
    return result


def fetch_validated_prices():
    global prices_cc, prices_cb
    cc = fetch_cryptocompare()
    cb = fetch_coinbase()
    if cc: prices_cc = cc
    if cb: prices_cb = cb
    validated = {}
    for asset in ASSET_LIST:
        c = prices_cc.get(asset)
        b = prices_cb.get(asset)
        if c and b:
            avg = (c + b) / 2
            diff = abs(c - b) / avg * 100
            if diff <= 0.20:
                validated[asset] = avg
        elif c:
            validated[asset] = c
        elif b:
            validated[asset] = b
    return validated


# ─── WINDOW LOGIC ──────────────────────────────────────────────────────────────
def get_window_times(tf):
    now = datetime.now(timezone.utc)
    slot = (now.minute // tf) * tf
    open_ = now.replace(minute=slot, second=0, microsecond=0)
    cm = slot + tf
    close_ = open_.replace(hour=(open_.hour + 1) % 24, minute=0) if cm >= 60 else open_.replace(minute=cm)
    return open_, close_, max(0, int((close_ - now).total_seconds()))


def wkey(asset, tf, open_time):
    return f"{asset}_{tf}_{open_time.strftime('%Y%m%d%H%M')}"


# ─── SAFETY CHECKS ─────────────────────────────────────────────────────────────
def safety_checks():
    """Returns (ok, reason) tuple."""
    if state["balance_usd"] < CONFIG["min_balance"]:
        return False, f"Balance ${state['balance_usd']:.2f} below limit ${CONFIG['min_balance']}"
    if state["consecutive_losses"] >= CONFIG["max_losses_streak"]:
        return False, f"{state['consecutive_losses']} losses in a row"
    if state["api_errors"] >= CONFIG["max_api_errors"]:
        return False, f"{state['api_errors']} API errors"
    if state["trades_today"] >= CONFIG["max_trades_per_day"]:
        return False, f"Daily trade limit ({CONFIG['max_trades_per_day']})"
    if state["pnl_today"] <= -CONFIG["daily_loss_limit"]:
        return False, f"Daily loss limit -${CONFIG['daily_loss_limit']}"
    return True, None


# ─── POLYMARKET TRADE PLACEMENT ────────────────────────────────────────────────
def find_market_token(asset, tf, open_time, direction):
    """Find Polymarket condition_id and token_id for a given crypto market."""
    # NOTE: This is the part that needs the most work
    # Polymarket markets have URL patterns like "ethereum-up-or-down-may-10-3pm-est"
    # We need to query the Gamma API to find the right one
    try:
        slug_date = (open_time - timedelta(hours=4)).strftime("%B-%-d").lower()  # EST
        slug_time = (open_time - timedelta(hours=4)).strftime("%-I%p").lower()
        asset_name = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "DOGE": "dogecoin"}.get(asset)
        # Try Gamma API
        url = f"https://gamma-api.polymarket.com/markets?active=true&closed=false"
        res = requests.get(url, timeout=10)
        # This needs more work - placeholder
        return None, None
    except Exception as e:
        log.error(f"Market lookup failed: {e}")
        return None, None


def place_real_trade(asset, tf, direction, bet_size, pct, open_time):
    """Place actual trade on Polymarket."""
    if CONFIG["dry_run"]:
        log.info(f"DRY RUN: would trade {asset} {direction} ${bet_size}")
        return {"status": "dry_run", "shares": bet_size / 0.95, "entry": 95.0, "order_id": "DRY_RUN"}

    condition_id, token_id = find_market_token(asset, tf, open_time, direction)
    if not token_id:
        return {"status": "failed", "error": "Market not found"}

    try:
        # Get current best ask price for the YES side (if we want UP, buy YES; if DOWN, buy NO)
        order_book = clob_client.get_order_book(token_id)
        if not order_book or not order_book.asks:
            return {"status": "failed", "error": "No asks in order book"}

        best_ask = float(order_book.asks[0].price)
        shares = bet_size / best_ask

        # Place market buy order
        order_args = OrderArgs(
            price=best_ask,
            size=shares,
            side=BUY,
            token_id=token_id,
        )
        signed = clob_client.create_order(order_args)
        resp = clob_client.post_order(signed, OrderType.FOK)  # Fill-or-kill for immediate fills

        return {
            "status": "filled" if resp.get("success") else "failed",
            "shares": shares,
            "entry": best_ask * 100,  # convert to cents
            "order_id": resp.get("orderID", "unknown"),
            "raw_response": resp,
        }
    except Exception as e:
        log.error(f"Trade placement failed: {e}")
        state["api_errors"] += 1
        return {"status": "failed", "error": str(e)}


# ─── MAIN TRADING LOOP ─────────────────────────────────────────────────────────
def process_tick():
    for tf in TIMEFRAMES:
        open_time, close_time, secs_left = get_window_times(tf)
        thresh_map = ASSET_THRESHOLDS_5

        for asset in ASSET_LIST:
            price = prices.get(asset)
            if not price:
                continue

            threshold = thresh_map.get(asset, 0.15)
            wk_key = wkey(asset, tf, open_time)
            ws_key = (asset, tf)

            if ws_key not in windows or windows[ws_key]["key"] != wk_key:
                windows[ws_key] = {
                    "key": wk_key, "open_time": open_time, "close_time": close_time,
                    "open_price": price, "traded": False,
                }

            w = windows[ws_key]
            pct = (price - w["open_price"]) / w["open_price"] * 100
            absp = abs(pct)
            dirn = "UP" if pct >= 0 else "DOWN"

            if w["traded"]: continue
            if secs_left <= 0 or secs_left > CONFIG["entry_window_seconds"]: continue
            if absp < threshold: continue
            if state["paused"]: continue

            ok, reason = safety_checks()
            if not ok:
                state["paused"] = True
                state["pause_reason"] = reason
                tg(f"🛑 <b>Auto-paused</b>\n{reason}")
                continue

            # Place the trade
            w["traded"] = True
            result = place_real_trade(asset, tf, dirn, CONFIG["bet_size"], pct, open_time)

            emoji = ASSET_EMOJI.get(asset, "")
            arrow = "🔺" if dirn == "UP" else "🔻"

            if result["status"] in ("filled", "dry_run"):
                trade = {
                    "entered_at": est_now().isoformat(),
                    "asset": asset, "timeframe": tf, "direction": dirn,
                    "pct_move": round(pct, 4),
                    "entry_price_cents": result.get("entry", 0),
                    "shares": result.get("shares", 0),
                    "cost": CONFIG["bet_size"],
                    "order_id": result.get("order_id", ""),
                    "status": "PENDING",
                    "payout": None, "profit_loss": None,
                }
                db_insert_trade(trade)
                state["trades_today"] += 1
                state["balance_usd"] -= CONFIG["bet_size"]
                tg(
                    f"{arrow} <b>{emoji} {asset} · {tf}m · {dirn}</b>\n\n"
                    f"📈 Move: {pct:+.3f}%\n"
                    f"⏱ {secs_left}s left\n"
                    f"💵 ${CONFIG['bet_size']} @ {result['entry']:.1f}¢\n"
                    f"📦 {result['shares']:.2f} shares\n"
                    f"🕐 {est_str()}"
                )
            else:
                err = result.get("error", "unknown")
                tg(f"❌ <b>Trade failed</b>\n{asset} {dirn}\n<code>{err[:150]}</code>")


# ─── MAIN LOOP ─────────────────────────────────────────────────────────────────
def main():
    global prices, last_prices_time
    init_db()

    mode_str = "DRY RUN" if CONFIG["dry_run"] else "🔴 LIVE TRADING"
    tg(
        f"🚀 <b>PolySniper LIVE v1.0</b>\n"
        f"⚠️ <b>{mode_str}</b>\n\n"
        f"🟠 BTC · 🔷 ETH · 🟣 SOL · 🟡 DOGE\n"
        f"⏱ 5min markets only (for now)\n"
        f"💵 Bet: ${CONFIG['bet_size']}\n"
        f"🛑 Auto-stop: <${CONFIG['min_balance']} bal, "
        f"{CONFIG['max_losses_streak']} losses, "
        f"-${CONFIG['daily_loss_limit']} daily\n"
        f"🕐 {est_str()}\n\n"
        f"Send /help for commands"
    )

    if not init_clob():
        log.error("Failed to init Polymarket client")
        return

    # Initial balance check
    bal = check_balance(force=True)
    if bal < CONFIG["min_balance"]:
        tg(f"⚠️ <b>Starting balance ${bal:.2f} below limit ${CONFIG['min_balance']}</b>\nDeposit more or lower MIN_BALANCE")
        state["paused"] = True
        state["pause_reason"] = "Low balance at start"
    else:
        tg(f"✅ <b>Connected to Polymarket</b>\n💰 Balance: ${bal:.2f}")

    last_price_fetch = 0
    while True:
        now = time.time()

        # Check balance every 5 min
        if now - state["last_balance_check"] >= 300:
            check_balance()

        if now - last_price_fetch >= CONFIG["price_interval_secs"]:
            new_p = fetch_validated_prices()
            if new_p:
                prices = new_p
                last_prices_time = now
            last_price_fetch = now

        handle_commands()

        if prices and not state["paused"]:
            process_tick()

        time.sleep(1)


if __name__ == "__main__":
    main()
