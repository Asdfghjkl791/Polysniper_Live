#!/usr/bin/env python3
# PolySniper LIVE v3.8 - SIMPLIFIED TEST (NO FILTERS)
# Pure price-range test: enter ANY side (UP or DOWN) if Polymarket ask is
# between 65-85¢ during the entry window. No move thresholds, no reversal
# filters, no conviction check. Just price + timing.
# 5m: last 180s (3 min) of window
# 15m: last 300s (5 min) of window
# Goal: collect data on natural win rate at 65-85¢ entries.

import os
import time
import sqlite3
import logging
import requests
import json
import threading
from datetime import datetime, timezone, timedelta
from collections import deque

try:
    import websocket  # websocket-client library
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

try:
    from py_clob_client_v2 import (
        ClobClient, OrderArgs, PartialCreateOrderOptions,
        BalanceAllowanceParams, AssetType, OrderType
    )
    from py_clob_client_v2.order_builder.constants import BUY
    V2_AVAILABLE = True
    IMPORT_ERROR = None
except ImportError as e:
    V2_AVAILABLE = False
    IMPORT_ERROR = str(e)

# ─── ENV VARS ─────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
POLY_PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "").strip()
POLY_FUNDER      = os.environ.get("POLY_FUNDER", "").strip()

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ASSET_LIST = ["BTC", "ETH", "SOL", "DOGE", "BNB"]
TIMEFRAMES = [5, 15]

ASSET_THRESHOLDS_5 = {
    "BTC":  float(os.environ.get("THRESHOLD_BTC_5",  "0.07")),
    "ETH":  float(os.environ.get("THRESHOLD_ETH_5",  "0.08")),
    "SOL":  float(os.environ.get("THRESHOLD_SOL_5",  "0.12")),
    "DOGE": float(os.environ.get("THRESHOLD_DOGE_5", "0.15")),
    "BNB":  float(os.environ.get("THRESHOLD_BNB_5",  "0.10")),
}
ASSET_THRESHOLDS_15 = {
    "BTC":  float(os.environ.get("THRESHOLD_BTC_15", "0.15")),
    "ETH":  float(os.environ.get("THRESHOLD_ETH_15", "0.20")),
    "SOL":  float(os.environ.get("THRESHOLD_SOL_15", "0.30")),
    "DOGE": float(os.environ.get("THRESHOLD_DOGE_15", "0.40")),
    "BNB":  float(os.environ.get("THRESHOLD_BNB_15", "0.25")),
}

MAX_REVERSALS_5  = int(os.environ.get("MAX_REVERSALS_5",  "30"))
MAX_REVERSALS_15 = int(os.environ.get("MAX_REVERSALS_15", "60"))

CONFIG = {
    "bet_size":             float(os.environ.get("BET_SIZE", "1.0")),
    "entry_window_seconds": int(os.environ.get("ENTRY_SECS", "180")),
    "entry_min_seconds":    int(os.environ.get("ENTRY_MIN_SECS", "30")),
    "retry_interval_secs":  int(os.environ.get("RETRY_INTERVAL", "5")),
    "momentum_checks":      int(os.environ.get("MOMENTUM_CHECKS", "1")),
    "price_agreement_pct":  float(os.environ.get("PRICE_AGREEMENT_PCT", "0.20")),
    "price_interval_secs":  5,
    "min_balance":          float(os.environ.get("MIN_BALANCE", "1.0")),
    "min_entry_cents":      float(os.environ.get("MIN_ENTRY_CENTS", "1.0")),
    "max_entry_cents":      float(os.environ.get("MAX_ENTRY_CENTS", "3.0")),
    "choppy_threshold":     int(os.environ.get("CHOPPY_THRESHOLD", "25")),
}

ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡", "BNB": "🟨"}

COINBASE_PRODUCTS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD", "DOGE": "DOGE-USD", "BNB": "BNB-USD"}
COINGECKO_IDS     = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "DOGE": "dogecoin", "BNB": "binancecoin"}

# ─── STATE ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("polysniper-live")

state = {
    "balance_usd":  0.0,
    "paused":       False,
    "pause_reason": None,
    "trades_today": 0,
    "pnl_today":    0.0,
    "wins_today":   0,
    "losses_today": 0,
    "last_balance_check": 0,
    "connected":    False,
    "last_msg_time": time.time(),
}

windows           = {}
prices            = {}
prices_cc         = {}
prices_cb         = {}
prices_cg         = {}
prices_chainlink  = {}  # From Polymarket's RTDS WebSocket - same source as their settlement
price_histories   = {}
active_directions = {}
for a in ASSET_LIST:
    for tf in TIMEFRAMES:
        price_histories[(a, tf)] = deque(maxlen=180)
last_prices_time = 0
update_offset    = None

clob_client = None
market_cache = {}  # cache of slug → token IDs


# ─── TIME HELPERS ────────────────────────────────────────────────────────────
def est_now():
    return datetime.now(timezone.utc) - timedelta(hours=4)

def est_str():
    return est_now().strftime("%H:%M EST")

def est_full():
    return est_now().strftime("%b %d %H:%M EST")

def fmt_time_short(iso_str):
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (dt - timedelta(hours=4)).strftime("%H:%M")
    except:
        return "??:??"


# ─── DATABASE ────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect("trades_live.db")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entered_at TEXT, asset TEXT, timeframe INTEGER, direction TEXT,
            pct_move REAL, open_price REAL, entry_price REAL,
            real_entry_cents REAL, shares REAL, cost REAL,
            order_id TEXT, close_price REAL, result TEXT,
            payout REAL, profit_loss REAL, balance_after REAL,
            window_open TEXT, window_close TEXT
        )
    """)
    conn.commit()
    conn.close()


def db_insert_trade(t):
    conn = sqlite3.connect("trades_live.db")
    c = conn.cursor()
    c.execute("""
        INSERT INTO trades (entered_at, asset, timeframe, direction, pct_move,
            open_price, entry_price, real_entry_cents, shares, cost, order_id,
            window_open, window_close, result)
        VALUES (:entered_at, :asset, :timeframe, :direction, :pct_move,
            :open_price, :entry_price, :real_entry_cents, :shares, :cost, :order_id,
            :window_open, :window_close, 'PENDING')
    """, t)
    row_id = c.lastrowid
    conn.commit()
    conn.close()
    return row_id


def db_log_skip(asset, tf, direction, pct_move, open_price, entry_cents, reason, open_time, close_time, secs_left):
    """Log a skipped signal to the trades DB for history tracking (no money spent)."""
    conn = sqlite3.connect("trades_live.db")
    c = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    c.execute("""
        INSERT INTO trades (entered_at, asset, timeframe, direction, pct_move,
            open_price, entry_price, real_entry_cents, shares, cost, order_id,
            window_open, window_close, result, profit_loss)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, 'SKIPPED', 0)
    """, (now, asset, tf, direction, round(pct_move, 4),
          open_price, open_price, round(entry_cents, 1),
          reason[:100], open_time.isoformat(), close_time.isoformat()))
    conn.commit()
    conn.close()


def db_settle(row_id, close_price, result, payout, pl, bal):
    conn = sqlite3.connect("trades_live.db")
    conn.execute("""
        UPDATE trades SET close_price=?, result=?, payout=?,
        profit_loss=?, balance_after=? WHERE id=?
    """, (close_price, result, payout, pl, bal, row_id))
    conn.commit()
    conn.close()


def db_stats():
    conn = sqlite3.connect("trades_live.db")
    c = conn.cursor()
    c.execute("""
        SELECT COUNT(*), SUM(CASE WHEN result='WON' THEN 1 ELSE 0 END), SUM(profit_loss)
        FROM trades WHERE result IN ('WON','LOST')
    """)
    r = c.fetchone()
    conn.close()
    return r[0] or 0, r[1] or 0, r[2] or 0.0


def db_recent_trades(limit=10, offset=0):
    conn = sqlite3.connect("trades_live.db")
    c = conn.cursor()
    c.execute("""
        SELECT entered_at, asset, timeframe, direction, pct_move, result, profit_loss, real_entry_cents, window_close
        FROM trades ORDER BY id DESC LIMIT ? OFFSET ?
    """, (limit, offset))
    rows = c.fetchall()
    conn.close()
    return rows


# ─── TELEGRAM ────────────────────────────────────────────────────────────────
def tg(msg):
    try:
        state["last_msg_time"] = time.time()
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
            tg("⏸ <b>Paused</b>\n/resume to continue")

        elif text == "/resume":
            state["paused"] = False
            state["pause_reason"] = None
            tg("▶️ <b>Resumed</b>")

        elif text == "/stop":
            state["paused"] = True
            state["pause_reason"] = "Emergency stop"
            tg("🛑 <b>EMERGENCY STOP</b>\n/resume to restart")

        elif text == "/status":
            total, wins, pnl = db_stats()
            wr = f"{wins/total*100:.1f}%" if total > 0 else "—"
            # Count skipped signals
            conn = sqlite3.connect("trades_live.db")
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM trades WHERE result='SKIPPED'")
            skipped = c.fetchone()[0] or 0
            conn.close()
            status_e = "⏸" if state["paused"] else "🟢"
            status_w = "PAUSED" if state["paused"] else "ACTIVE"
            tg(
                f"{status_e} <b>{status_w}</b>\n\n"
                f"💰 Balance: <b>${state['balance_usd']:.2f}</b>\n"
                f"🎯 Bet: ${CONFIG['bet_size']}\n"
                f"📊 Today: {state['trades_today']} · ${state['pnl_today']:+.2f}\n"
                f"🏆 All-time: {total} · {wr} · ${pnl:+.2f}\n"
                f"⏭ Skipped: {skipped}\n"
                f"🕐 {est_str()}"
            )

        elif text == "/balance":
            bal = check_balance(force=True)
            tg(f"💰 <b>Balance:</b> ${bal:.2f}\n🕐 {est_str()}")

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
                    t, a, tf, d, pct, res, pl, ec, wc = row
                    emoji = ASSET_EMOJI.get(a, "")
                    arrow = "🔺" if d == "UP" else "🔻"
                    if res == "WON":
                        res_str = "✅"
                    elif res == "LOST":
                        res_str = "❌"
                    elif res == "SKIPPED":
                        res_str = "⏭"
                    else:
                        res_str = "⏳"
                    pl_str = f"${pl:+.2f}" if pl is not None and res != "SKIPPED" else ("—" if res == "SKIPPED" else "pending")
                    # Calculate seconds left when entered
                    secs_left_str = ""
                    try:
                        entered_dt = datetime.fromisoformat(t)
                        close_dt = datetime.fromisoformat(wc)
                        if entered_dt.tzinfo is None: entered_dt = entered_dt.replace(tzinfo=timezone.utc)
                        if close_dt.tzinfo is None: close_dt = close_dt.replace(tzinfo=timezone.utc)
                        secs_left = max(0, int((close_dt - entered_dt).total_seconds()))
                        secs_left_str = f" ({secs_left}s)"
                    except:
                        pass
                    out += f"  {fmt_time_short(t)}{secs_left_str} {emoji}{a} {tf}m {arrow} {pct:+.2f}% @{ec or '?'}¢ {res_str} {pl_str}\n"
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


# ─── POLYMARKET CONNECTION ───────────────────────────────────────────────────
def init_clob():
    global clob_client
    if not V2_AVAILABLE:
        tg(f"❌ <b>py-clob-client-v2 import failed</b>\n<code>{IMPORT_ERROR}</code>")
        return False
    if not POLY_PRIVATE_KEY or not POLY_FUNDER:
        tg("❌ <b>POLY_PRIVATE_KEY or POLY_FUNDER missing</b>")
        return False

    host = "https://clob.polymarket.com"
    chain_id = 137

    try:
        log.info("Creating temporary client for L1 auth (with sig=3)...")
        temp = ClobClient(
            host=host,
            chain_id=chain_id,
            key=POLY_PRIVATE_KEY,
            signature_type=3,
            funder=POLY_FUNDER,
        )
        creds = temp.create_or_derive_api_key()

        log.info("Creating authenticated client (sig=3, POLY_1271)...")
        clob_client = ClobClient(
            host=host,
            chain_id=chain_id,
            key=POLY_PRIVATE_KEY,
            creds=creds,
            signature_type=3,
            funder=POLY_FUNDER,
        )
        check_balance(force=True)
        state["connected"] = True
        log.info(f"Connected. Balance: ${state['balance_usd']:.2f}")
        return True
    except Exception as e:
        log.error(f"CLOB init failed: {e}")
        tg(f"❌ <b>Polymarket connection failed</b>\n<code>{str(e)[:200]}</code>")
        return False


def check_balance(force=False):
    global clob_client
    now = time.time()
    if not force and now - state["last_balance_check"] < 60:
        return state["balance_usd"]
    if not clob_client:
        return 0.0
    try:
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        bal_resp = clob_client.get_balance_allowance(params)
        if isinstance(bal_resp, dict):
            bal_raw = bal_resp.get("balance", 0)
        else:
            bal_raw = getattr(bal_resp, "balance", 0)
        state["balance_usd"] = float(bal_raw) / 1_000_000
        state["last_balance_check"] = now
        return state["balance_usd"]
    except Exception as e:
        log.error(f"Balance check failed: {e}")
        return state["balance_usd"]


# ─── PRICE FEEDS ─────────────────────────────────────────────────────────────
def fetch_cryptocompare():
    return {}  # Disabled in Chainlink-only mode


def fetch_coinbase():
    return {}  # Disabled in Chainlink-only mode


def fetch_coingecko():
    return {}  # Disabled in Chainlink-only mode


# ─── CHAINLINK WEBSOCKET (Polymarket's actual settlement source) ─────────────
CHAINLINK_WS_URL = "wss://ws-live-data.polymarket.com"
CHAINLINK_SYMBOLS = {
    "BTC":  "btc/usd",
    "ETH":  "eth/usd",
    "SOL":  "sol/usd",
    "DOGE": "doge/usd",
    "BNB":  "bnb/usd",
}
chainlink_last_update = {}  # Track when we last got a price for each asset


def chainlink_websocket_worker(asset):
    """
    Polymarket sends a snapshot of last 2 minutes of prices on subscribe, then
    no further updates. So we reconnect every 20 seconds to get a fresh snapshot.
    The latest price in each snapshot becomes our current price.
    """
    symbol = CHAINLINK_SYMBOLS.get(asset)
    if not symbol:
        return

    while True:
        ws = None
        try:
            log.info(f"[Chainlink WS] Connecting for {asset} ({symbol})...")
            ws = websocket.create_connection(CHAINLINK_WS_URL, timeout=10)
            ws.settimeout(5)

            subscribe_msg = {
                "action": "subscribe",
                "subscriptions": [
                    {
                        "topic": "crypto_prices_chainlink",
                        "type": "update",
                        "filters": json.dumps({"symbol": symbol})
                    }
                ]
            }
            ws.send(json.dumps(subscribe_msg))

            # Receive the snapshot (should arrive within 1-2 seconds)
            connect_time = time.time()
            got_data = False
            while time.time() - connect_time < 5:  # Wait up to 5s for snapshot
                try:
                    msg = ws.recv()
                    if not msg:
                        continue
                    data = json.loads(msg)
                    payload = data.get("payload", {})

                    if isinstance(payload, dict):
                        data_arr = payload.get("data")
                        if isinstance(data_arr, list) and data_arr:
                            latest = data_arr[-1]
                            if isinstance(latest, dict):
                                value = float(latest.get("value", 0))
                                if value > 0:
                                    prices_chainlink[asset] = value
                                    chainlink_last_update[asset] = time.time()
                                    got_data = True
                                    break  # Got fresh price, move on
                        elif "value" in payload:
                            value = float(payload.get("value", 0))
                            if value > 0:
                                prices_chainlink[asset] = value
                                chainlink_last_update[asset] = time.time()
                                got_data = True
                                break
                except websocket.WebSocketTimeoutException:
                    continue
                except Exception as e:
                    log.warning(f"[Chainlink WS] {asset} recv error: {e}")
                    break

            if got_data:
                log.info(f"[Chainlink WS] {asset} fresh price: {prices_chainlink.get(asset)}")
            else:
                log.warning(f"[Chainlink WS] {asset} no data in snapshot")

        except Exception as e:
            log.error(f"[Chainlink WS] {asset} connection error: {e}")
        finally:
            try:
                if ws:
                    ws.close()
            except:
                pass

        # Wait 5 seconds before reconnecting for fresh snapshot
        time.sleep(5)


def start_chainlink_threads():
    """Start a background thread for each asset to maintain Chainlink WS connection."""
    if not WEBSOCKET_AVAILABLE:
        log.warning("websocket-client not installed - Chainlink feed disabled")
        return False
    for asset in ASSET_LIST:
        t = threading.Thread(target=chainlink_websocket_worker, args=(asset,), daemon=True)
        t.start()
    log.info(f"[Chainlink WS] Started {len(ASSET_LIST)} background threads")
    return True


def get_chainlink_price(asset):
    """
    Get the latest Chainlink price for an asset.
    Returns None if no recent update (data older than 30 seconds is stale).
    """
    if asset not in prices_chainlink:
        return None
    last_update = chainlink_last_update.get(asset, 0)
    if time.time() - last_update > 30:
        return None  # Stale data, don't use
    return prices_chainlink[asset]


# ─── COINBASE WEBSOCKET (For lag detection vs Chainlink) ─────────────────────
# Strategy: Coinbase updates spot prices in real-time. Chainlink has its own
# update interval (lags by a few seconds). When Coinbase shows BTC moved up
# significantly but Chainlink hasn't updated yet, we know Chainlink will tick up
# soon. We enter BEFORE Chainlink/Polymarket catches up.
#
# Per research: This is how the most successful Polymarket bots work
# (e.g. 0x8dxd turned $313 -> $438K with ~98% win rate using this approach).
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
COINBASE_PRODUCTS_MAP = {
    "BTC":  "BTC-USD",
    "ETH":  "ETH-USD",
    "SOL":  "SOL-USD",
    "DOGE": "DOGE-USD",
    "BNB":  None,  # Coinbase doesn't list BNB
}
prices_coinbase = {}
coinbase_last_update = {}


def coinbase_websocket_worker():
    """
    Single thread that subscribes to Coinbase ticker feed for all supported assets.
    Coinbase pushes real-time ticker updates. We just listen and update the dict.
    """
    products = [p for p in COINBASE_PRODUCTS_MAP.values() if p]
    if not products:
        return

    while True:
        ws = None
        try:
            log.info(f"[Coinbase WS] Connecting for {products}...")
            ws = websocket.create_connection(COINBASE_WS_URL, timeout=10)
            ws.settimeout(30)
            subscribe_msg = {
                "type": "subscribe",
                "product_ids": products,
                "channels": ["ticker"],
            }
            ws.send(json.dumps(subscribe_msg))
            log.info(f"[Coinbase WS] Subscribed to ticker for {products}")

            # Reverse map: "BTC-USD" -> "BTC"
            product_to_asset = {v: k for k, v in COINBASE_PRODUCTS_MAP.items() if v}

            while True:
                try:
                    msg = ws.recv()
                    if not msg:
                        continue
                    data = json.loads(msg)
                    if data.get("type") != "ticker":
                        continue
                    product_id = data.get("product_id")
                    asset = product_to_asset.get(product_id)
                    if not asset:
                        continue
                    price = float(data.get("price", 0))
                    if price > 0:
                        prices_coinbase[asset] = price
                        coinbase_last_update[asset] = time.time()
                except websocket.WebSocketTimeoutException:
                    continue
                except Exception as e:
                    log.warning(f"[Coinbase WS] recv error: {e}")
                    break
        except Exception as e:
            log.error(f"[Coinbase WS] connection error: {e}")
        finally:
            try:
                if ws:
                    ws.close()
            except:
                pass
        time.sleep(3)  # Wait before reconnecting


def start_coinbase_thread():
    """Start the single Coinbase WS thread."""
    if not WEBSOCKET_AVAILABLE:
        return False
    t = threading.Thread(target=coinbase_websocket_worker, daemon=True)
    t.start()
    log.info("[Coinbase WS] Background thread started")
    return True


def get_coinbase_price(asset):
    """Returns latest Coinbase price, or None if stale (>10s)."""
    if asset not in prices_coinbase:
        return None
    last_update = coinbase_last_update.get(asset, 0)
    if time.time() - last_update > 10:
        return None
    return prices_coinbase[asset]


def detect_price_lag(asset, lag_threshold_pct=0.05):
    """
    Compare Coinbase real-time price to Chainlink price.
    Returns a tuple (direction, lag_pct) when there's meaningful lag:
        ("UP", lag_pct)  if Coinbase > Chainlink by lag_threshold_pct
        ("DOWN", lag_pct) if Coinbase < Chainlink by lag_threshold_pct
        (None, 0) if no lag detected or data unavailable

    lag_threshold_pct is in % points (e.g. 0.05 = 0.05% = 5bps)
    """
    chainlink = get_chainlink_price(asset)
    coinbase = get_coinbase_price(asset)
    if not chainlink or not coinbase:
        return None, 0
    lag_pct = (coinbase - chainlink) / chainlink * 100
    if abs(lag_pct) < lag_threshold_pct:
        return None, lag_pct
    return ("UP" if lag_pct > 0 else "DOWN"), lag_pct


def fetch_validated_prices():
    """
    CHAINLINK-ONLY MODE: Returns prices from Polymarket's Chainlink WebSocket feed ONLY.
    This is the same source Polymarket uses for settlement, ensuring no data mismatch.

    If Chainlink is unavailable (>30s stale), the asset is skipped entirely.
    No fallback to CryptoCompare/Coinbase/CoinGecko.
    """
    validated = {}
    for asset in ASSET_LIST:
        chainlink_price = get_chainlink_price(asset)
        if chainlink_price:
            validated[asset] = chainlink_price
        # No fallback - if Chainlink doesn't have data, asset is skipped
    return validated


# ─── WINDOW LOGIC ────────────────────────────────────────────────────────────
def get_window_times(tf):
    now = datetime.now(timezone.utc)
    slot = (now.minute // tf) * tf
    open_ = now.replace(minute=slot, second=0, microsecond=0)
    cm = slot + tf
    if cm >= 60:
        close_ = open_.replace(hour=(open_.hour + 1) % 24, minute=0)
    else:
        close_ = open_.replace(minute=cm)
    return open_, close_, max(0, int((close_ - now).total_seconds()))


def wkey(asset, tf, open_time):
    return f"{asset}_{tf}_{open_time.strftime('%Y%m%d%H%M')}"


def count_reversals(hist):
    if len(hist) < 3:
        return 0
    revs, prev = 0, None
    pl = list(hist)
    for i in range(1, len(pl)):
        d = pl[i] - pl[i-1]
        if d == 0: continue
        cur = "up" if d > 0 else "dn"
        if prev and cur != prev: revs += 1
        prev = cur
    return revs


def count_reversals_recent(hist, seconds=30):
    """Count reversals in the last N seconds (price history is appended ~1/sec)."""
    if len(hist) < 3:
        return 0
    recent = list(hist)[-seconds:]
    if len(recent) < 3:
        return 0
    revs, prev = 0, None
    for i in range(1, len(recent)):
        d = recent[i] - recent[i-1]
        if d == 0: continue
        cur = "up" if d > 0 else "dn"
        if prev and cur != prev: revs += 1
        prev = cur
    return revs


def check_momentum(hist, direction, n):
    if len(hist) < n + 1:
        return True
    r = list(hist)[-(n+1):]
    if direction == "UP":
        return all(r[i] <= r[i+1] for i in range(len(r)-1))
    return all(r[i] >= r[i+1] for i in range(len(r)-1))


def check_correlation(asset, direction):
    pairs = [("BTC", "ETH"), ("ETH", "BTC")]
    for a1, a2 in pairs:
        if asset == a1:
            for tf in TIMEFRAMES:
                if active_directions.get((a2, tf)) == direction:
                    return False, f"{a2} active {direction}"
    return True, None


# ─── POLYMARKET MARKET LOOKUP ────────────────────────────────────────────────
def find_market_for_window(asset, tf, open_time):
    """
    Find the Polymarket condition_id + token_ids for a given crypto Up/Down market.
    Polymarket uses Unix timestamp-based event slugs:
      btc-updown-5m-{timestamp}      (5-min event, timestamp = window start Unix epoch)
      eth-updown-5m-{timestamp}
      btc-updown-15m-{timestamp}     (15-min event)
      eth-updown-15m-{timestamp}
    Note: the slug is an EVENT slug. We query /events?slug=... and pull markets from inside.
    """
    asset_short = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "DOGE": "doge", "BNB": "bnb"}.get(asset)
    if not asset_short:
        return None, None, None

    # Convert window open_time to Unix timestamp (seconds since epoch)
    window_ts = int(open_time.timestamp())
    slug = f"{asset_short}-updown-{tf}m-{window_ts}"

    if slug in market_cache:
        return market_cache[slug]

    try:
        # Query the EVENTS endpoint (not markets) since the slug is an event slug
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        res = requests.get(url, timeout=10)
        data = res.json()
        if not data or not isinstance(data, list) or len(data) == 0:
            log.warning(f"Event not found for slug: {slug}")
            return None, None, None
        event = data[0]
        markets = event.get("markets", [])
        if not markets:
            log.warning(f"No markets in event: {slug}")
            return None, None, None
        # Up/Down events typically have ONE market with two outcomes (YES=Up, NO=Down)
        market = markets[0]
        condition_id = market.get("conditionId")
        token_ids = market.get("clobTokenIds")
        if isinstance(token_ids, str):
            import json
            token_ids = json.loads(token_ids)
        # token_ids[0] = YES (UP), token_ids[1] = NO (DOWN)
        up_token = token_ids[0]
        down_token = token_ids[1]
        market_cache[slug] = (condition_id, up_token, down_token)
        log.info(f"Found market via event {slug}: condId={condition_id[:10]}...")
        return condition_id, up_token, down_token
    except Exception as e:
        log.warning(f"Market lookup error for {slug}: {e}")
        return None, None, None


# ─── ORDER PLACEMENT (SIGNALS-ONLY MODE) ─────────────────────────────────────
# Polymarket API has a known bug preventing order placement for new accounts
# (signer/API key mismatch). While Polymarket fixes this (ETA Tuesday May 19),
# bot runs in signals-only mode: detects signals, alerts via Telegram with
# market link, you manually trade via Polymarket app.
SIGNALS_ONLY_MODE = True

def place_order(asset, tf, direction, bet_size, open_time):
    """In signals-only mode, returns the order book info without placing an order."""
    condition_id, up_token, down_token = find_market_for_window(asset, tf, open_time)
    if not up_token:
        return {"status": "failed", "error": "Market not found"}

    token_id = up_token if direction == "UP" else down_token

    try:
        order_book = clob_client.get_order_book(token_id)
        if not order_book:
            return {"status": "failed", "error": "No order book"}

        if isinstance(order_book, dict):
            asks = order_book.get("asks", [])
        else:
            asks = getattr(order_book, "asks", [])

        if not asks:
            return {"status": "failed", "error": "Empty order book"}

        first_ask = asks[0]
        if isinstance(first_ask, dict):
            best_ask = float(first_ask.get("price", 0))
        else:
            best_ask = float(getattr(first_ask, "price", 0))

        if best_ask <= 0:
            return {"status": "failed", "error": "Invalid ask price"}

        entry_cents = best_ask * 100

        if entry_cents < CONFIG["min_entry_cents"]:
            return {
                "status": "skipped",
                "error": f"Ask {entry_cents:.1f}¢ < min {CONFIG['min_entry_cents']:.0f}¢ (market disagrees)",
                "entry_cents": entry_cents,
            }

        if entry_cents > CONFIG["max_entry_cents"]:
            return {
                "status": "skipped",
                "error": f"Ask {entry_cents:.1f}¢ > max {CONFIG['max_entry_cents']:.0f}¢",
                "entry_cents": entry_cents,
            }

        shares = round(bet_size / best_ask, 4)

        if SIGNALS_ONLY_MODE:
            # Build market URL for manual trading
            asset_short = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "DOGE": "doge", "BNB": "bnb"}.get(asset, asset.lower())
            window_ts = int(open_time.timestamp())
            market_url = f"https://polymarket.com/event/{asset_short}-updown-{tf}m-{window_ts}"
            return {
                "status": "signal",
                "shares": shares,
                "entry_cents": entry_cents,
                "market_url": market_url,
                "order_id": "SIGNAL_ONLY",
            }

        # === REAL ORDER PLACEMENT (currently disabled while Polymarket fixes bug) ===
        order_args = OrderArgs(
            token_id=token_id,
            price=best_ask,
            size=shares,
            side=BUY,
        )
        resp = clob_client.create_and_post_order(
            order_args,
            options=PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
            order_type=OrderType.FOK,
        )
        log.info(f"Order response: {resp}")

        success = False
        if isinstance(resp, dict):
            success = resp.get("success", False) or resp.get("status") == "matched"
            order_id = resp.get("orderID") or resp.get("orderId", "unknown")
        else:
            success = getattr(resp, "success", False)
            order_id = getattr(resp, "orderID", "unknown")

        return {
            "status": "filled" if success else "failed",
            "shares": shares,
            "entry_cents": best_ask * 100,
            "order_id": order_id,
            "raw": str(resp)[:200],
        }
    except Exception as e:
        log.error(f"Order placement error: {e}")
        return {"status": "failed", "error": str(e)}


# ─── TRADING LOOP ────────────────────────────────────────────────────────────
def process_tick():
    """
    EXPERIMENTAL v3.7 SIMPLIFIED MODE:
    - No move thresholds
    - No reversal/conviction/choppy filters
    - Just: if Polymarket ask is in [min_entry_cents, max_entry_cents] range
      during the time window, enter that side
    - Tries BOTH UP and DOWN sides every tick
    - 5m: last entry_window_seconds of the window (default 180s = full last 3 min)
    - 15m: last entry_window_seconds_15 of the window (default 300s = full last 5 min)
    """
    for tf in TIMEFRAMES:
        open_time, close_time, secs_left = get_window_times(tf)

        for asset in ASSET_LIST:
            price = prices.get(asset)
            if not price:
                continue

            wk_key = wkey(asset, tf, open_time)
            ws_key = (asset, tf)

            if ws_key not in windows or windows[ws_key]["key"] != wk_key:
                prev = windows.get(ws_key)
                if prev and prev["traded"] and prev["trade_db_id"]:
                    settle(prev, asset, tf, price)
                if ws_key in active_directions:
                    del active_directions[ws_key]
                windows[ws_key] = {
                    "key": wk_key, "open_time": open_time, "close_time": close_time,
                    "open_price": price, "traded": False, "skipped": False,
                    "trade_db_id": None, "direction": None,
                    "last_retry_at": 0,
                }
                price_histories[ws_key].clear()

            price_histories[ws_key].append(price)
            w = windows[ws_key]
            pct = (price - w["open_price"]) / w["open_price"] * 100

            if w["traded"] or w["skipped"]:
                continue

            # Contrarian strategy: don't enter in the last seconds (no time to reverse).
            # 5m: need at least 40s left, 15m: need at least 60s left.
            if secs_left <= 0:
                continue
            min_secs_5 = int(os.environ.get("MIN_SECS_5", "40"))
            min_secs_15 = int(os.environ.get("MIN_SECS_15", "60"))
            if tf == 5 and secs_left < min_secs_5:
                continue
            if tf == 15 and secs_left < min_secs_15:
                continue
            if state["paused"]:
                continue
            if state["balance_usd"] < CONFIG["min_balance"]:
                continue

            # Rate-limit retry attempts - only check every N seconds
            now_ts = time.time()
            if now_ts - w["last_retry_at"] < CONFIG["retry_interval_secs"]:
                continue
            w["last_retry_at"] = now_ts

            # Contrarian filters: only enter when conditions favor a reversal.
            # 1. Move % should be LOW (market hasn't strongly committed to a direction)
            # 2. Recent reversals should be HIGH (market is choppy, reversal likely)
            absp = abs(pct)
            hist = price_histories[ws_key]
            recent_revs = count_reversals_recent(hist, seconds=30)

            max_move = float(os.environ.get("CONTRARIAN_MAX_MOVE", "0.20"))
            min_recent_revs = int(os.environ.get("CONTRARIAN_MIN_REVS", "10"))

            if absp > max_move:
                continue  # Too strong a trend, unlikely to reverse
            if recent_revs < min_recent_revs:
                continue  # Not choppy enough, reversal unlikely

            # Check both UP and DOWN sides - whichever is in 1-3¢ range, take it
            condition_id, up_token, down_token = find_market_for_window(asset, tf, open_time)
            if not up_token or not down_token:
                # DEBUG: log when market lookup fails
                if asset == "BTC" and tf == 5:
                    log.warning(f"DEBUG: find_market_for_window returned None for BTC 5m at window {open_time}")
                continue

            min_c = CONFIG["min_entry_cents"]
            max_c = CONFIG["max_entry_cents"]

            # DEBUG: collect asks for BTC 5m and send to Telegram occasionally
            debug_asks = {}

            for dirn, token_id in [("UP", up_token), ("DOWN", down_token)]:
                try:
                    # USE get_price() not get_order_book() - the book endpoint
                    # returns stale "ghost" data (always 0.99/0.01). get_price gives
                    # the actual live price. (Known bug in py-clob-client #180)
                    # side="SELL" gives us the best ASK (what we'd pay to BUY)
                    price_resp = clob_client.get_price(token_id, side="SELL")
                    if isinstance(price_resp, dict):
                        ask_price = float(price_resp.get("price", 0))
                    else:
                        ask_price = float(price_resp)

                    # Also fetch midpoint and BUY-side price for debug
                    bid_str = "—"
                    try:
                        mid_resp = clob_client.get_midpoint(token_id)
                        mid_price = float(mid_resp.get("mid", 0)) if isinstance(mid_resp, dict) else float(mid_resp)
                        if mid_price > 0:
                            bid_str = f"mid={mid_price*100:.1f}¢"
                    except Exception:
                        pass

                    if ask_price <= 0:
                        if asset == "BTC" and tf == 5:
                            debug_asks[dirn] = f"INVALID({ask_price})"
                        continue
                    ask_cents = ask_price * 100

                    # DEBUG: record what we see
                    if asset == "BTC" and tf == 5:
                        debug_asks[dirn] = f"ask={ask_cents:.1f}¢ {bid_str}"

                    # If ask is in target range, enter
                    if min_c <= ask_cents <= max_c:
                        log.info(f"ENTER {asset} {tf}m {dirn} - ask={ask_cents:.1f}¢ (no filters)")
                        enter_trade_simple(w, asset, tf, price, pct, dirn, secs_left,
                                          open_time, close_time, ask_price, ask_cents)
                        break  # Only enter one direction per window
                except Exception as e:
                    log.warning(f"Price check failed for {asset} {tf}m {dirn}: {e}")
                    if asset == "BTC" and tf == 5:
                        debug_asks[dirn] = f"ERR:{str(e)[:30]}"
                    continue

            # DEBUG: send Telegram message with asks once per minute for BTC 5m
            if asset == "BTC" and tf == 5 and debug_asks:
                now_ts = time.time()
                if now_ts - state.get("last_debug_time", 0) > 60:
                    state["last_debug_time"] = now_ts
                    window_ts = int(open_time.timestamp())
                    tg(
                        f"🔍 <b>DEBUG · BTC 5m</b>\n"
                        f"Window: {window_ts}\n"
                        f"UP: {debug_asks.get('UP', 'none')}\n"
                        f"UP tok: <code>{up_token}</code>\n"
                        f"DOWN: {debug_asks.get('DOWN', 'none')}\n"
                        f"DOWN tok: <code>{down_token}</code>\n"
                        f"Target: {min_c:.0f}-{max_c:.0f}¢\n"
                        f"Secs left: {secs_left}"
                    )


def enter_trade_simple(w, asset, tf, price, pct, dirn, secs_left, open_time, close_time, ask_price, ask_cents):
    """Simplified entry - no filters, just record the signal."""
    bet = CONFIG["bet_size"]
    emoji = ASSET_EMOJI.get(asset, "")
    arrow = "🔺" if dirn == "UP" else "🔻"
    shares = round(bet / ask_price, 4)

    w["traded"] = True
    now = datetime.now(timezone.utc)
    trade = {
        "entered_at": now.isoformat(), "asset": asset, "timeframe": tf,
        "direction": dirn, "pct_move": round(pct, 4),
        "open_price": w["open_price"], "entry_price": price,
        "real_entry_cents": round(ask_cents, 1),
        "shares": shares, "cost": bet,
        "order_id": "SIGNAL_ONLY",
        "window_open": open_time.isoformat(),
        "window_close": close_time.isoformat(),
    }
    db_id = db_insert_trade(trade)
    w["trade_db_id"] = db_id
    w["direction"] = dirn
    active_directions[(asset, tf)] = dirn

    # Build market URL
    asset_short = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "DOGE": "doge", "BNB": "bnb"}.get(asset, asset.lower())
    window_ts = int(open_time.timestamp())
    market_url = f"https://polymarket.com/event/{asset_short}-updown-{tf}m-{window_ts}"

    tg(
        f"🚨 <b>SIGNAL · {emoji} {asset} · {tf}m · {dirn}</b> {arrow}\n\n"
        f"📈 Move: {pct:+.3f}%\n"
        f"⏱ {secs_left}s left\n"
        f"💰 Polymarket ask: <b>{ask_cents:.1f}¢</b>\n"
        f"📦 Would buy: {shares} shares\n"
        f"💵 Cost: ${bet:.2f}\n\n"
        f"<a href=\"{market_url}\">🔗 Trade on Polymarket</a>\n"
        f"🕐 {est_str()}"
    )


def enter_trade(w, asset, tf, price, pct, dirn, secs_left, open_time, close_time, revs=0, recent_revs=0):
    """Legacy function - kept for compatibility but no longer used in v3.7 simplified mode."""
    pass


def enter_trade(w, asset, tf, price, pct, dirn, secs_left, open_time, close_time, revs=0, recent_revs=0):
    bet = min(CONFIG["bet_size"], state["balance_usd"])
    if bet < CONFIG["min_balance"]:
        return

    result = place_order(asset, tf, dirn, bet, open_time)

    emoji = ASSET_EMOJI.get(asset, "")
    arrow = "🔺" if dirn == "UP" else "🔻"

    if result["status"] == "signal":
        # Signals-only mode - alert user with market info for manual trading
        w["traded"] = True  # Mark as traded so we don't fire again for this window
        now = datetime.now(timezone.utc)
        # Save to DB as a "signal" record
        # In signals-only mode, "cost" is the hypothetical cost (what we WOULD have spent)
        # so PnL math correctly reflects what a real trade would have earned/lost.
        trade = {
            "entered_at": now.isoformat(), "asset": asset, "timeframe": tf,
            "direction": dirn, "pct_move": round(pct, 4),
            "open_price": w["open_price"], "entry_price": price,
            "real_entry_cents": round(result["entry_cents"], 1),
            "shares": result["shares"], "cost": bet,  # hypothetical cost for PnL accuracy
            "order_id": "SIGNAL_ONLY",
            "window_open": open_time.isoformat(),
            "window_close": close_time.isoformat(),
        }
        db_id = db_insert_trade(trade)
        w["trade_db_id"] = db_id
        w["direction"] = dirn
        active_directions[(asset, tf)] = dirn

        tg(
            f"🚨 <b>SIGNAL · {emoji} {asset} · {tf}m · {dirn}</b> {arrow}\n\n"
            f"📈 Move: {pct:+.3f}%\n"
            f"⏱ {secs_left}s left\n"
            f"📊 30s reversals: {recent_revs}/{CONFIG['choppy_threshold']}\n"
            f"💰 Polymarket ask: <b>{result['entry_cents']:.1f}¢</b>\n"
            f"📦 Would buy: {result['shares']} shares\n"
            f"💵 Cost: ${bet:.2f}\n\n"
            f"<a href=\"{result['market_url']}\">🔗 Trade on Polymarket</a>\n"
            f"🕐 {est_str()}"
        )
        return

    if result["status"] == "skipped":
        # Entry too expensive or too cheap - log to DB silently, no Telegram spam
        w["skipped"] = True
        log.info(f"Skipped {asset} {tf}m {dirn}: {result['error']}")
        db_log_skip(
            asset, tf, dirn, pct, w["open_price"],
            result.get("entry_cents", 0), result["error"],
            open_time, close_time, secs_left
        )
        return

    if result["status"] == "filled":
        w["traded"] = True
        now = datetime.now(timezone.utc)
        trade = {
            "entered_at": now.isoformat(), "asset": asset, "timeframe": tf,
            "direction": dirn, "pct_move": round(pct, 4),
            "open_price": w["open_price"], "entry_price": price,
            "real_entry_cents": round(result["entry_cents"], 1),
            "shares": result["shares"], "cost": bet,
            "order_id": result["order_id"],
            "window_open": open_time.isoformat(),
            "window_close": close_time.isoformat(),
        }
        db_id = db_insert_trade(trade)
        w["trade_db_id"] = db_id
        w["direction"] = dirn
        state["balance_usd"] -= bet
        state["trades_today"] += 1
        active_directions[(asset, tf)] = dirn

        tg(
            f"{arrow} <b>{emoji} {asset} · {tf}m · {dirn}</b>\n\n"
            f"📈 Move: {pct:+.3f}%\n"
            f"⏱ {secs_left}s left · {revs_str(w, asset, tf)} rev\n"
            f"💵 ${bet} @ <b>{result['entry_cents']:.1f}¢</b>\n"
            f"📦 {result['shares']} shares\n"
            f"🆔 <code>{result['order_id'][:12]}...</code>\n"
            f"🕐 {est_str()}"
        )
    else:
        # Failed (empty book, network error, etc) - DON'T mark traded, allow retries
        err = result.get("error", "unknown")
        log.warning(f"Trade attempt failed (will retry): {err}")
        # Detect geoblock - this is fatal, pause bot
        if "403" in err or "restricted" in err.lower() or "geoblock" in err.lower():
            w["skipped"] = True
            state["paused"] = True
            state["pause_reason"] = "Geoblocked by Polymarket"
            tg(
                f"🚫 <b>GEOBLOCKED</b>\n\n"
                f"Polymarket is blocking the bot's server IP.\n"
                f"Trading paused. Need VPS in allowed region.\n"
                f"<code>{err[:200]}</code>"
            )


def revs_str(w, asset, tf):
    ws_key = (asset, tf)
    return count_reversals(price_histories[ws_key])


def fetch_polymarket_outcome(asset, tf, open_time, retry_count=0):
    """
    Fetch the ACTUAL settled outcome from Polymarket (Chainlink-based) rather than
    calculating it from our price sources.

    Returns:
        "UP" if Polymarket settled the market as UP/YES
        "DOWN" if Polymarket settled as DOWN/NO
        None if not yet settled or error
    """
    asset_short = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "DOGE": "doge", "BNB": "bnb"}.get(asset)
    if not asset_short:
        return None

    window_ts = int(open_time.timestamp())
    slug = f"{asset_short}-updown-{tf}m-{window_ts}"

    try:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        res = requests.get(url, timeout=10)
        data = res.json()
        if not data or not isinstance(data, list) or len(data) == 0:
            log.warning(f"Outcome fetch: event not found for {slug}")
            return None
        event = data[0]
        markets = event.get("markets", [])
        if not markets:
            log.warning(f"Outcome fetch: no markets in event {slug}")
            return None
        market = markets[0]

        # Polymarket markets have a "outcomePrices" field that resolves to ["1","0"] or ["0","1"]
        # after settlement. Index 0 = UP (YES), Index 1 = DOWN (NO).
        outcome_prices = market.get("outcomePrices")
        if isinstance(outcome_prices, str):
            try:
                outcome_prices = json.loads(outcome_prices)
            except:
                pass

        if not outcome_prices or len(outcome_prices) < 2:
            log.info(f"Outcome fetch: {slug} no outcomePrices yet")
            return None

        # Check outcome prices directly - they resolve to 1/0 or 0/1 once settled
        # Don't rely on closed/umaResolutionStatus flags since Chainlink markets
        # may not set these the same way as UMA-resolved markets
        try:
            up_price = float(outcome_prices[0])
            down_price = float(outcome_prices[1])
        except (ValueError, TypeError):
            log.warning(f"Outcome fetch: invalid prices for {slug}: {outcome_prices}")
            return None

        # During trading, prices are between 0 and 1 (e.g., 0.65, 0.35).
        # After settlement, the winner is exactly 1.0 and loser is 0.0
        if up_price >= 0.99:
            log.info(f"Outcome fetch: {slug} settled UP (prices: {outcome_prices})")
            return "UP"
        elif down_price >= 0.99:
            log.info(f"Outcome fetch: {slug} settled DOWN (prices: {outcome_prices})")
            return "DOWN"
        else:
            # Not yet settled (prices still showing live market sentiment)
            log.info(f"Outcome fetch: {slug} not yet settled (prices: UP={up_price}, DOWN={down_price})")
            return None
    except Exception as e:
        log.warning(f"Outcome fetch error for {slug}: {e}")
        return None


def settle(w, asset, tf, close_price):
    """
    Schedules settlement in a background thread so the main loop isn't blocked.
    The settlement retries fetching Polymarket's outcome for up to 60 seconds.
    """
    # Capture state we need (in case window state changes during settlement)
    settle_data = {
        "trade_db_id": w["trade_db_id"],
        "direction": w["direction"],
        "open_time": w["open_time"],
        "open_price": w["open_price"],
        "close_price": close_price,
    }
    t = threading.Thread(target=_settle_worker, args=(settle_data, asset, tf), daemon=True)
    t.start()


def _settle_worker(d, asset, tf):
    """Background worker that does the actual settlement with retries.
    Polymarket Gamma API can be slow (5-30 min sometimes), so we retry a long time.
    NEVER fall back to local Chainlink math - that math is unreliable due to
    timing differences between our Chainlink WS snapshots and Polymarket's
    actual settlement price. Better to mark UNVERIFIED than to report wrong outcome.
    """
    # Retry fetching Polymarket outcome for up to 30 minutes
    poly_outcome = None
    max_retries = 180  # 180 attempts × 10 sec = 30 minutes max wait
    retry_delay = 10

    for attempt in range(max_retries):
        poly_outcome = fetch_polymarket_outcome(asset, tf, d["open_time"])
        if poly_outcome:
            break
        if attempt < max_retries - 1:
            if attempt % 6 == 0:  # Log every minute instead of every retry
                log.info(f"Outcome not ready for {asset} {tf}m, retrying (attempt {attempt+1}/{max_retries})...")
            time.sleep(retry_delay)

    if poly_outcome:
        actual = poly_outcome
        source = "Polymarket"
    else:
        # Polymarket Gamma never returned outcome after 30 min - very unusual.
        # Mark UNVERIFIED. Do NOT report a win/loss because we can't trust it.
        log.error(f"Settled {asset} {tf}m UNVERIFIED - Polymarket Gamma never published outcome after 30min")
        # Update DB but skip the Telegram message to avoid bad data
        conn = sqlite3.connect("trades_live.db")
        conn.execute("UPDATE trades SET result=?, profit_loss=0, balance_after=? WHERE id=?",
                     ("UNVERIFIED", state["balance_usd"], d["trade_db_id"]))
        conn.commit()
        conn.close()
        tg(
            f"❓ <b>{ASSET_EMOJI.get(asset, '')} {asset} {tf}m · UNVERIFIED</b>\n\n"
            f"Polymarket Gamma never returned outcome.\n"
            f"Trade was placed; outcome unknown.\n"
            f"Check Polymarket portfolio for real result."
        )
        return

    won = actual == d["direction"]

    conn = sqlite3.connect("trades_live.db")
    c = conn.cursor()
    c.execute("SELECT real_entry_cents, cost, shares FROM trades WHERE id=?", (d["trade_db_id"],))
    row = c.fetchone()
    conn.close()
    entry_c, cost, shares = (row or (99.0, CONFIG["bet_size"], 0))

    if won:
        # Each share pays out $1.00 (100¢)
        payout = shares * 1.0
        pl = payout - cost
    else:
        payout = 0
        pl = -cost

    # Refresh real balance from Polymarket
    check_balance(force=True)
    state["pnl_today"] += pl
    if won:
        state["wins_today"] += 1
    else:
        state["losses_today"] += 1

    result = "WON" if won else "LOST"
    db_settle(d["trade_db_id"], d["close_price"], result, payout, pl, state["balance_usd"])

    total, wins, total_pnl = db_stats()
    wr = f"{wins/total*100:.1f}%" if total > 0 else "—"
    emoji = ASSET_EMOJI.get(asset, "")
    out_emoji = "✅" if won else "❌"

    tg(
        f"{out_emoji} <b>{emoji} {asset} {tf}m · {result}</b>\n\n"
        f"Called: {d['direction']} · Actual: {actual}\n"
        f"<i>Source: {source}</i>\n"
        f"PnL: <b>${pl:+.3f}</b> · Bal: ${state['balance_usd']:.2f}\n"
        f"📊 {total} · {wr} · ${total_pnl:+.2f}"
    )

    # Auto-pause on loss DISABLED for data collection mode.
    # Re-enable by uncommenting these 3 lines when ready to trade real money:
    # if not won:
    #     state["paused"] = True
    #     state["pause_reason"] = f"Loss on {asset} {tf}m"
    #     tg(f"⏸ <b>Auto-paused after loss</b>\n/resume when ready.")


# ─── LAG DETECTION TRADING ──────────────────────────────────────────────────
# Tracks last-fired window per asset to avoid double-firing on the same window
lag_fired_windows = {}  # {(asset, tf, window_key): timestamp}


def process_lag_signals():
    """
    Detects when Coinbase shows price movement that Chainlink hasn't caught up to.
    Fires a signal in the matching direction.

    Currently configured for BTC only (highest liquidity, cleanest detection).
    """
    LAG_ASSETS = os.environ.get("LAG_ASSETS", "BTC").split(",")
    lag_threshold = float(os.environ.get("LAG_THRESHOLD_PCT", "0.05"))

    for asset in LAG_ASSETS:
        asset = asset.strip()
        if asset not in ASSET_LIST:
            continue

        direction, lag_pct = detect_price_lag(asset, lag_threshold)
        if direction is None:
            continue

        # Lag detected - try to enter both 5m and 15m markets
        for tf in TIMEFRAMES:
            open_time, close_time, secs_left = get_window_times(tf)
            window_key = (asset, tf, wkey(asset, tf, open_time))

            # Don't fire if we already fired for this window
            if window_key in lag_fired_windows:
                continue

            # Don't fire in the last 10 seconds (window about to settle)
            if secs_left < 10:
                continue

            # Don't fire too EARLY in the window either.
            # 5m markets: skip first 60s (wait for real trend, secs_left must be < 240)
            # 15m markets: skip first 180s (wait for sustained move, secs_left must be < 720)
            # The early part of windows is just noise; lag signals there are unreliable.
            if tf == 5:
                early_skip = int(os.environ.get("EARLY_SKIP_5", "60"))
                window_total = 300
                if secs_left > (window_total - early_skip):
                    continue
            else:  # 15m
                early_skip = int(os.environ.get("EARLY_SKIP_15", "180"))
                window_total = 900
                if secs_left > (window_total - early_skip):
                    continue

            # Check Polymarket order book for this direction
            condition_id, up_token, down_token = find_market_for_window(asset, tf, open_time)
            if not up_token or not down_token:
                continue

            token_id = up_token if direction == "UP" else down_token

            try:
                # Use get_price (not get_order_book - that's broken in SDK)
                price_resp = clob_client.get_price(token_id, side="SELL")
                if isinstance(price_resp, dict):
                    ask_price = float(price_resp.get("price", 0))
                else:
                    ask_price = float(price_resp)
                if ask_price <= 0:
                    continue
                ask_cents = ask_price * 100
            except Exception as e:
                log.warning(f"Lag price check failed for {asset} {tf}m {direction}: {e}")
                continue

            # Only enter if price hasn't already spiked to 99¢
            min_c = float(os.environ.get("LAG_MIN_CENTS", "30"))
            max_c = float(os.environ.get("LAG_MAX_CENTS", "85"))
            if ask_cents < min_c or ask_cents > max_c:
                continue

            lag_fired_windows[window_key] = time.time()

            # Record signal in DB
            bet = CONFIG["bet_size"]
            shares = round(bet / ask_price, 4)
            now = datetime.now(timezone.utc)
            trade = {
                "entered_at": now.isoformat(), "asset": asset, "timeframe": tf,
                "direction": direction, "pct_move": round(lag_pct, 4),
                "open_price": get_chainlink_price(asset),
                "entry_price": get_coinbase_price(asset),
                "real_entry_cents": round(ask_cents, 1),
                "shares": shares, "cost": bet,
                "order_id": "LAG_SIGNAL",
                "window_open": open_time.isoformat(),
                "window_close": close_time.isoformat(),
            }
            db_id = db_insert_trade(trade)

            # Set up window tracking for settlement
            ws_key = (asset, tf)
            if ws_key in windows:
                windows[ws_key]["traded"] = True
                windows[ws_key]["trade_db_id"] = db_id
                windows[ws_key]["direction"] = direction

            asset_short = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "DOGE": "doge", "BNB": "bnb"}.get(asset, asset.lower())
            window_ts = int(open_time.timestamp())
            market_url = f"https://polymarket.com/event/{asset_short}-updown-{tf}m-{window_ts}"

            emoji = ASSET_EMOJI.get(asset, "")
            arrow = "🔺" if direction == "UP" else "🔻"
            cb_price = get_coinbase_price(asset)
            cl_price = get_chainlink_price(asset)

            tg(
                f"⚡ <b>LAG SIGNAL · {emoji} {asset} · {tf}m · {direction}</b> {arrow}\n\n"
                f"🪙 Coinbase: ${cb_price:.2f}\n"
                f"🔗 Chainlink: ${cl_price:.2f}\n"
                f"📊 Lag: {lag_pct:+.3f}%\n"
                f"⏱ {secs_left}s left\n"
                f"💰 Polymarket ask: <b>{ask_cents:.1f}¢</b>\n"
                f"📦 Would buy: {shares} shares\n"
                f"💵 Cost: ${bet:.2f}\n\n"
                f"<a href=\"{market_url}\">🔗 Trade on Polymarket</a>\n"
                f"🕐 {est_str()}"
            )

    # Cleanup old fired windows (older than 30 min)
    now_ts = time.time()
    expired = [k for k, t in lag_fired_windows.items() if now_ts - t > 1800]
    for k in expired:
        del lag_fired_windows[k]


# ─── MAIN ────────────────────────────────────────────────────────────────────
def main():
    global prices, last_prices_time
    init_db()

    # Start Chainlink WebSocket background threads for accurate settlement-matching prices
    chainlink_ok = start_chainlink_threads()

    # Start Coinbase WebSocket for real-time spot prices (lag detection)
    coinbase_ok = start_coinbase_thread()

    tg(
        f"🚀 <b>PolySniper LIVE v3.8-CONTRARIAN</b>\n"
        f"🎲 <b>EXTREME LOSING SIDE STRATEGY</b>\n"
        f"📲 Signals-only paper trading\n\n"
        f"🎯 Buy the 1-3¢ side (heavily losing)\n"
        f"   When it wins (rare), payoff is 30:1+\n\n"
        f"💵 Bet size: ${CONFIG['bet_size']}\n"
        f"💲 Entry range: {CONFIG['min_entry_cents']:.1f}-{CONFIG['max_entry_cents']:.1f}¢\n"
        f"🟠 BTC · 🔷 ETH · 🟣 SOL · 🟡 DOGE · 🟨 BNB\n"
        f"⏱ 5m + 15m windows\n"
        f"⏰ Enter at any time (no window filter)\n"
        f"🔗 Chainlink WS: {'✅' if chainlink_ok else '❌'}\n\n"
        f"<i>Need only 3-4% win rate to break even.</i>\n"
        f"<i>5%+ would be very profitable.</i>\n\n"
        f"🕐 {est_full()}\n\n"
        f"Connecting to Polymarket..."
    )

    if not init_clob():
        log.error("Failed to init Polymarket")
        return

    tg(
        f"✅ <b>Connected!</b>\n\n"
        f"💰 Balance: <b>${state['balance_usd']:.2f}</b>\n"
        f"🎯 Watching {' + '.join(ASSET_LIST)} markets\n"
        f"🚨 Signals-only mode\n\n"
        f"You'll get a Telegram alert with market link when a signal fires.\n"
        f"Click the link to trade manually.\n\n"
        f"/help for commands"
    )

    last_price_fetch = 0
    startup_time = time.time()
    last_prices_time = startup_time  # Initialize to startup time, not 0, so we don't fire false warnings
    grace_period = 60  # Don't fire "feed down" warnings for first 60s after startup

    while True:
        try:
            now = time.time()
            if now - last_price_fetch >= CONFIG["price_interval_secs"]:
                new_p = fetch_validated_prices()
                if new_p:
                    prices = new_p
                    last_prices_time = now
                    if state["pause_reason"] == "Price feed down":
                        state["paused"] = False
                        state["pause_reason"] = None
                        tg("✅ <b>Price feed restored</b>")
                elif now - last_prices_time > 120 and now - startup_time > grace_period and not state["paused"]:
                    state["paused"] = True
                    state["pause_reason"] = "Price feed down"
                    tg("⚠️ <b>Price feed down 2+ min</b>")
                last_price_fetch = now

            handle_commands()
            if prices and not state["paused"]:
                process_tick()
                # Lag detection disabled for contrarian strategy
                # process_lag_signals()

            # Periodic balance refresh
            if now - state["last_balance_check"] > 300:
                check_balance()
        except Exception as e:
            log.error(f"Main loop error: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
