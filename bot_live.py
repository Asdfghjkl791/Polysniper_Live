#!/usr/bin/env python3
# LIVE BTC JUMP SNIPER (Oct 27 2026) - replaces the fav-dip bot in the 50secmaker service.
#
# THE RULE (from the paper jump logger; passed its fair test)
#   When BTC moves >= JUMP_BPS within JUMP_WINDOW seconds on Binance, look at the live BTC 5m and 15m
#   up/down markets. For the side the jump favours:
#     fair  = model chance that side wins (BTC vs the window's start price, BTC's recent volatility, time left)
#     edge  = fair - ask (ask read EDGE_DELAY seconds after the jump)
#   Trade when edge >= MIN_EDGE, ask < MAX_ASK, and (ask >= CHEAP_CENTS or edge >= CHEAP_EDGE), ask 5-95c.
#   Buy LIVE_STAKE dollars (fill-or-kill, never above ask + LIVE_MAX_OVERPAY_CENTS), sell HOLD_SECS later
#   (fill-or-kill, never below bid - LIVE_SELL_SLIP_CENTS; retries; if it still can't sell, it holds to the end).
#
# SAFETY
#   - OFF until LIVE_ENABLED=true. While off it runs the same logic on paper and messages every signal.
#   - Daily loss limit (LIVE_DAILY_LOSS_LIMIT, counts the worst case of anything still open), max trades per
#     hour, one position per market at a time. /stop pauses instantly, /go resumes.
#   - Every trade is shown next to what paper would have made, plus the real USDC balance change.
#
# COMMANDS: /status · /trades · /today · /balance · /stop · /go · /export · /help
#
# ENV (required): TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, POLY_PRIVATE_KEY, POLY_FUNDER, POLY_SIG_TYPE
# ENV (optional): LIVE_ENABLED=false  LIVE_STAKE=5  LIVE_MAX_OVERPAY_CENTS=2  LIVE_SELL_SLIP_CENTS=2
#   LIVE_DAILY_LOSS_LIMIT=15  LIVE_MAX_TRADES_HOUR=20  HOLD_SECS=5  JL_TFS=5,15  JUMP_BPS=5  JUMP_WINDOW=2
#   JUMP_COOLDOWN=10  EDGE_DELAY=0.25  MIN_EDGE=10  MAX_ASK=85  CHEAP_CENTS=20  CHEAP_EDGE=15
#   DB_PATH=/data/live_jumper.db

import os, time, json, math, sqlite3, logging, threading, requests, csv, io, gzip, re
from collections import deque
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import websocket
import types
E2_ON = False
early2 = None                                  # second early logger, built in main()

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
DB_PATH = os.environ.get("DB_PATH", "live_jumper.db")
env_f = lambda k, d: float(os.environ.get(k, d))
LIVE_ENABLED = os.environ.get("LIVE_ENABLED", "false").strip().lower() == "true"
JUMPER_ON = os.environ.get("JUMPER", "off").strip().lower() in ("1", "true", "yes", "on")   # old jump sniper: off unless JUMPER=on
LIVE_STAKE = env_f("LIVE_STAKE", "5")
LIVE_MAX_OVERPAY = env_f("LIVE_MAX_OVERPAY_CENTS", "2")
LIVE_SELL_SLIP = env_f("LIVE_SELL_SLIP_CENTS", "2")
LIVE_DAILY_LOSS = env_f("LIVE_DAILY_LOSS_LIMIT", "15")
LIVE_MAX_TRADES_HOUR = int(env_f("LIVE_MAX_TRADES_HOUR", "20"))
HOLD_SECS = env_f("HOLD_SECS", "5")
TFS = [int(x) for x in os.environ.get("JL_TFS", "5,15").split(",") if x.strip()]
JUMP_BPS = env_f("JUMP_BPS", "5")
JUMP_WINDOW = env_f("JUMP_WINDOW", "2")
JUMP_COOLDOWN = env_f("JUMP_COOLDOWN", "10")
EDGE_DELAY = env_f("EDGE_DELAY", "0")            # decide on the book at the jump itself (fastest)
KEEP_EDGE = env_f("KEEP_EDGE_CENTS", "8")          # smart cap: may pay up to fair - 8c ...
SMART_MAX = env_f("SMART_MAX_OVERPAY_CENTS", "8")  # ... but never more than ask + 8c
MIN_EDGE = env_f("MIN_EDGE", "10")
MAX_ASK = env_f("MAX_ASK", "85")
CHEAP_CENTS = env_f("CHEAP_CENTS", "20")
CHEAP_EDGE = env_f("CHEAP_EDGE", "15")
POLY_PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "").strip()
POLY_FUNDER = os.environ.get("POLY_FUNDER", "").strip()
POLY_SIG_TYPE = int(os.environ.get("POLY_SIG_TYPE", "1"))
FEE_RATE = 0.07
ASSET = "BTC"
CLOB_BASE = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS = "wss://data-stream.binance.vision/stream?streams=btcusdt@bookTicker"
BINANCE_REST = "https://data-api.binance.vision/api/v3/klines"
ET = ZoneInfo("America/New_York")
TZ = ZoneInfo("America/Toronto")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("live-jumper")

lock = threading.RLock()          # books / markets
jlock = threading.Lock()          # price state
db_lock = threading.Lock()
trade_lock = threading.Lock()     # one live order at a time
books = {}                        # token -> {"bids": {c: sz}, "asks": {c: sz}}
markets = {}                      # (tf, open_ts) -> (up_tok, dn_tok)
opens = {}                        # (tf, open_ts) -> window start price
busy = set()                      # (tf, open_ts) with an open position
price = {"px": None, "ts": 0.0}
ticks = deque(maxlen=5000)        # (ts, px), last JUMP_WINDOW seconds
px1s = deque(maxlen=1200)         # (ts, px) ~1/s
state = {"last_jump": 0.0, "jumps": 0, "signals": 0, "started": time.time(), "bn": 0, "pm": 0, "err": ""}
_clob = None


def fee(sh, p):
    return sh * FEE_RATE * p * (1 - p)


def money(x):
    return f"{'+' if x >= 0 else chr(0x2212)}${abs(x):.2f}"


# ── database ────────────────────────────────────────────────────────────────
def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with db_lock:
        c = db()
        c.execute("""CREATE TABLE IF NOT EXISTS lj_trades (id INTEGER PRIMARY KEY AUTOINCREMENT, t0 REAL, tf INTEGER,
            open_ts INTEGER, side TEXT, bps REAL, fair REAL, ask REAL, bid0 REAL, edge REAL, mode TEXT, status TEXT,
            buy_cost REAL, buy_shares REAL, buy_px REAL, buy_ts REAL, sell_proceeds REAL, sell_shares REAL,
            sell_px REAL, sell_ts REAL, paper_bid REAL, paper_pnl REAL, pnl REAL, bal_before REAL, bal_after REAL,
            result TEXT, note TEXT)""")
        c.execute("CREATE TABLE IF NOT EXISTS lj_kv (k TEXT PRIMARY KEY, v TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS lj_early (id INTEGER PRIMARY KEY AUTOINCREMENT, t0 REAL, feed TEXT, data TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS lj_deribit (id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, data TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS lj_extra (id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, kind TEXT, data TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS lj_close (id INTEGER PRIMARY KEY AUTOINCREMENT, close_ts REAL, tf INTEGER, "
                  "open_ts INTEGER, outcome TEXT, data TEXT)")
        for col in ("fair_iv REAL", "sig REAL", "sig_iv REAL", "cl_open REAL", "bn_open REAL"):
            try:
                c.execute(f"ALTER TABLE lj_trades ADD COLUMN {col}")
            except sqlite3.OperationalError:
                pass
        c.commit()
        c.close()


def db_exec(sql, args=()):
    with db_lock:
        c = db()
        cur = c.execute(sql, args)
        c.commit()
        rid = cur.lastrowid
        c.close()
        return rid


def db_query(sql, args=()):
    with db_lock:
        c = db()
        r = c.execute(sql, args).fetchall()
        c.close()
        return r


def kv_get(k, d=None):
    r = db_query("SELECT v FROM lj_kv WHERE k=?", (k,))
    return r[0]["v"] if r else d


def kv_set(k, v):
    db_exec("INSERT OR REPLACE INTO lj_kv (k, v) VALUES (?, ?)", (k, str(v)))


def paused():
    return kv_get("paused", "0") == "1"


# ── telegram ────────────────────────────────────────────────────────────────
def tg(msg):
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4000], "parse_mode": "HTML"}, timeout=8)
        if r.status_code != 200:
            log.error(f"tg {r.status_code}: {r.text[:150]}")
            plain = re.sub(r"</?b>|</?i>", "", msg).replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": plain[:4000]}, timeout=8)
    except Exception as e:
        log.error(f"tg: {e}")


def tg_document(name, data, caption=""):
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
                      data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:1000]},
                      files={"document": (name, data, "application/gzip")}, timeout=120)
    except Exception as e:
        tg(f"⚠️ upload failed: {e}")


# ── Polymarket client (same setup the fav-dip bot used on this account) ─────
def clob():
    global _clob
    if _clob is not None:
        return _clob
    if not POLY_PRIVATE_KEY:
        raise RuntimeError("POLY_PRIVATE_KEY is not set in Railway")
    if POLY_SIG_TYPE in (1, 2) and not POLY_FUNDER:
        raise RuntimeError("POLY_FUNDER (your Polymarket wallet address) is not set")
    from py_clob_client_v2 import ClobClient
    c = ClobClient(CLOB_BASE, chain_id=137, key=POLY_PRIVATE_KEY, signature_type=POLY_SIG_TYPE,
                   funder=POLY_FUNDER or None)
    c.set_api_creds(c.create_or_derive_api_key())
    _clob = c
    log.info("[LIVE] connected")
    return c


def usdc_balance():
    from py_clob_client_v2 import BalanceAllowanceParams, AssetType
    r = clob().get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL,
                                                             signature_type=POLY_SIG_TYPE))
    return float(r.get("balance", 0)) / 1e6


def token_balance(token):
    from py_clob_client_v2 import BalanceAllowanceParams, AssetType
    p = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token, signature_type=POLY_SIG_TYPE)
    try:
        clob().update_balance_allowance(p)
    except Exception:
        pass
    r = clob().get_balance_allowance(p)
    return float(r.get("balance", 0)) / 1e6


def _amounts(resp):
    try:
        return float(resp.get("makingAmount") or 0), float(resp.get("takingAmount") or 0)
    except Exception:
        return 0.0, 0.0


ORDER_TYPE = os.environ.get("ORDER_TYPE", "FAK").strip().upper()     # FAK = fill what you can, cancel the rest
warmed = set()
timing = {}                                                          # last order: build / post seconds


def warm(token):
    """Build (and sign) a throwaway order for a token WITHOUT sending it, so market info, fee info and the
    signing code are all loaded before a real trade needs them."""
    if not (LIVE_ENABLED or D_LIVE) or token in warmed:
        return
    try:
        from py_clob_client_v2 import MarketOrderArgs, OrderType, Side
        t0 = time.time()
        clob().create_market_order(MarketOrderArgs(token_id=token, amount=1.0, side=Side.BUY, price=0.01,
                                                   order_type=OrderType.FOK))
        warmed.add(token)
        log.info(f"[LIVE] warmed {token[:10]}… in {time.time() - t0:.2f}s")
    except Exception as e:
        log.warning(f"[LIVE] warm {token[:10]}…: {e}")


def fast_post(c, order, ot):
    """Send a signed order and return Polymarket's answer straight away. (The library's post_order then keeps
    polling until the trade settles on-chain, which can take seconds and would push our 5-second sell later.)"""
    try:
        import py_clob_client_v2.client as cm
        owner = c.creds.api_key or ""
        payload = (cm.order_to_json_v2(order, owner, ot, False, False) if cm._is_v2_order(order)
                   else cm.order_to_json_v1(order, owner, ot, False, False))
        ser = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        headers = c._l2_headers("POST", cm.POST_ORDER, body=payload, serialized_body=ser)
        return c._post(f"{c.host}{cm.POST_ORDER}", headers=headers, data=ser)
    except (AttributeError, ImportError, TypeError) as e:     # library changed: fall back to the normal call
        log.warning(f"[LIVE] fast_post fallback: {e}")
        return c.post_order(order, ot)


LIVE_TFS = {int(x) for x in os.environ.get("LIVE_TFS", "5,15").replace(" ", "").split(",") if x}   # which markets trade for real
BATCH_ORDERS = os.environ.get("BATCH_ORDERS", "true").strip().lower() in ("1", "true", "yes", "on")


def build_order(token, side, amount, limit_c):
    from py_clob_client_v2 import MarketOrderArgs, OrderType, Side
    ot = OrderType.FAK if ORDER_TYPE == "FAK" else OrderType.FOK
    lim = max(0.01, min(0.99, round(limit_c / 100.0, 2)))
    mo = MarketOrderArgs(token_id=token, amount=round(amount, 2), side=Side.BUY if side == "BUY" else Side.SELL,
                         price=lim, order_type=ot)
    return clob().create_market_order(mo), ot


def check_resp(resp):
    """(ok, resp, note) from one order answer"""
    if not isinstance(resp, dict):
        resp = {"success": False, "errorMsg": str(resp)[:200]}
    if not resp.get("success", False) or resp.get("errorMsg"):
        return False, resp, str(resp.get("errorMsg") or resp)[:200]
    mk, tk = _amounts(resp)
    if str(resp.get("status", "")).lower() != "matched" and tk <= 0:
        return False, resp, f"status={resp.get('status')}"
    return True, resp, ""


def batch_post(c, items):
    """Send several signed orders in ONE request, so they all reach Polymarket together
    (sent one by one, the second waits ~0.4s behind the first). Returns one answer per order.
    Raises if the whole request fails."""
    import py_clob_client_v2.client as cm
    POST_ORDERS = getattr(cm, "POST_ORDERS", "/orders")
    owner = c.creds.api_key or ""
    payload = [cm.order_to_json_v2(o, owner, ot, False, False) if cm._is_v2_order(o)
               else cm.order_to_json_v1(o, owner, ot, False, False) for o, ot in items]
    ser = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    headers = c._l2_headers("POST", POST_ORDERS, body=payload, serialized_body=ser)
    res = c._post(f"{c.host}{POST_ORDERS}", headers=headers, data=ser)
    if isinstance(res, dict) and isinstance(res.get("data"), list):
        res = res["data"]
    if not isinstance(res, list) or len(res) != len(items):
        raise RuntimeError(f"batch answer not understood: {str(res)[:150]}")
    return res


def market_order(token, side, amount, limit_c, tim=None):
    """Market order with a price limit. BUY: amount = dollars, limit = most we pay. SELL: amount = shares,
    limit = least we accept. FAK fills what it can at or better than the limit and cancels the rest.
    Returns (ok, resp, note)."""
    t0 = time.time()
    order, ot = build_order(token, side, amount, limit_c)
    t1 = time.time()
    try:
        resp = fast_post(clob(), order, ot) or {}
    except Exception as e:
        resp = {"success": False, "errorMsg": str(e)[:200]}
    t2 = time.time()
    if tim is not None:
        tim.update(build=t1 - t0, post=t2 - t1, sent=t1)
    log.info(f"[LIVE] {side} build {t1 - t0:.2f}s post {t2 - t1:.2f}s resp: {str(resp)[:300]}")
    return check_resp(resp)


# ── markets ─────────────────────────────────────────────────────────────────
def window(tf, now):
    L = tf * 60
    o = int(now // L) * L
    return o, o + L


def resolve(tf, open_ts):
    try:
        arr = requests.get(f"{GAMMA}/events", params={"slug": f"btc-updown-{tf}m-{open_ts}"}, timeout=8).json()
        ev = arr[0] if isinstance(arr, list) and arr else None
        mk = (ev or {}).get("markets", [])
        if mk:
            t = json.loads(mk[0].get("clobTokenIds", "[]"))
            if len(t) == 2:
                return str(t[0]), str(t[1])
    except Exception as e:
        log.debug(f"resolve {tf} {open_ts}: {e}")
    return None


def outcome(tf, open_ts):
    try:
        arr = requests.get(f"{GAMMA}/events", params={"slug": f"btc-updown-{tf}m-{open_ts}"}, timeout=10).json()
        mk = (arr[0] if isinstance(arr, list) and arr else {}).get("markets", [])
        op = mk[0].get("outcomePrices") if mk else None
        op = json.loads(op) if isinstance(op, str) else op
        if op and len(op) >= 2:
            if float(op[0]) >= 0.99:
                return "UP"
            if float(op[1]) >= 0.99:
                return "DOWN"
    except Exception as e:
        log.warning(f"outcome {tf} {open_ts}: {e}")
    return None


def kline_open(open_ts):
    try:
        j = requests.get(BINANCE_REST, params=dict(symbol="BTCUSDT", interval="1m", startTime=open_ts * 1000,
                                                   endTime=open_ts * 1000 + 59000, limit=1), timeout=8).json()
        if isinstance(j, list) and j:
            return float(j[0][1])
    except Exception as e:
        log.warning(f"kline {open_ts}: {e}")
    return None


def resolver_worker():
    while True:
        try:
            now = time.time()
            for tf in TFS:
                cur = window(tf, now)[0]
                if (tf, cur) not in opens and now >= cur + 60:
                    op = kline_open(cur)               # didn't see the open live: use Binance's 1m open
                    if op:
                        opens[(tf, cur)] = op
                for o in (cur, window(tf, now + 90)[0]):
                    if (tf, o) not in markets:
                        t = resolve(tf, o)
                        if t:
                            with lock:
                                markets[(tf, o)] = t
                            for tok in t:
                                warm(tok)
            with lock:
                for k in [k for k in markets if k[1] + k[0] * 60 < now - 600]:
                    for t in markets.pop(k):
                        books.pop(t, None)
            for k in [k for k in opens if k[1] + k[0] * 60 < now - 3600]:
                opens.pop(k, None)
        except Exception as e:
            state["err"] = f"resolver: {e}"
            log.error(state["err"])
        time.sleep(3)


# ── feeds ───────────────────────────────────────────────────────────────────
def pm_handle(raw):
    try:
        data = json.loads(raw)
    except Exception:
        return
    for m in (data if isinstance(data, list) else [data]):
        if not isinstance(m, dict):
            continue
        state["pm"] += 1
        et = m.get("event_type")
        with lock:
            if et == "book":
                tok = str(m.get("asset_id"))
                books[tok] = {"bids": {round(float(x["price"]) * 100, 3): float(x["size"]) for x in (m.get("bids") or m.get("buys") or [])},
                              "asks": {round(float(x["price"]) * 100, 3): float(x["size"]) for x in (m.get("asks") or m.get("sells") or [])}}
            elif et == "last_trade_price":                 # Nov 4: Polymarket's own trades (paper research)
                try:
                    pm_trades.append((time.time(), str(m.get("asset_id")), round(float(m.get("price", 0)) * 100, 2),
                                      float(m.get("size", 0)), str(m.get("side", "")).upper()))
                except (TypeError, ValueError):
                    pass
            elif et == "price_change":
                for ch in (m.get("price_changes") or m.get("changes") or []):
                    tok = str(ch.get("asset_id") or m.get("asset_id"))
                    b = books.setdefault(tok, {"bids": {}, "asks": {}})
                    lvl = b["bids"] if str(ch.get("side", "")).upper() == "BUY" else b["asks"]
                    px, sz = round(float(ch["price"]) * 100, 3), float(ch.get("size", 0))
                    if sz <= 0:
                        lvl.pop(px, None)
                    else:
                        lvl[px] = sz


def pm_worker():
    while True:
        try:
            with lock:
                toks = tuple(sorted(t for v in markets.values() for t in v))
            if not toks:
                time.sleep(2)
                continue
            stop = threading.Event()

            def on_open(ws):
                ws.send(json.dumps({"assets_ids": list(toks), "type": "market"}))

                def pinger():
                    while not stop.is_set():
                        time.sleep(10)
                        try:
                            ws.send("PING")
                        except Exception:
                            return
                threading.Thread(target=pinger, daemon=True).start()

            def on_message(ws, msg):
                if msg in ("PONG", "pong"):
                    return
                pm_handle(msg)
                with lock:
                    cur = tuple(sorted(t for v in markets.values() for t in v))
                if cur != toks:
                    ws.close()

            ws = websocket.WebSocketApp(MARKET_WS, on_open=on_open, on_message=on_message)
            ws.run_forever(ping_interval=20, ping_timeout=10)
            stop.set()
        except Exception as e:
            log.warning(f"pm ws: {e}")
        time.sleep(1)


def on_tick(now, px):
    with jlock:
        price["px"], price["ts"] = px, now
        if not px1s or now - px1s[-1][0] >= 1.0:
            px1s.append((now, px))
        ticks.append((now, px))
        while ticks and ticks[0][0] < now - JUMP_WINDOW:
            ticks.popleft()
        for tf in TFS:                                  # remember the price at each window's open (like the scanner)
            o = window(tf, now)[0]
            if (tf, o) not in opens and now - o <= 5:
                opens[(tf, o)] = px
        if now - state["last_jump"] < JUMP_COOLDOWN or len(ticks) < 2:
            return
        bps = (px / ticks[0][1] - 1) * 1e4
        if abs(bps) < JUMP_BPS:
            return
        state["last_jump"] = now
        state["jumps"] += 1
    if JUMPER_ON:                       # old 5-second jump sniper (off by default since Nov 2; D rules are separate)
        threading.Thread(target=on_jump, args=(now, bps, px), daemon=True).start()


def bn_worker():
    while True:
        try:
            ws = websocket.create_connection(BINANCE_WS, timeout=10)
            ws.settimeout(30)
            log.info("binance connected")
            while True:
                d = json.loads(ws.recv()).get("data", {})
                b, a = float(d.get("b", 0)), float(d.get("a", 0))
                if b > 0 and a > 0:
                    state["bn"] += 1
                    now_ = time.time()
                    on_tick(now_, (b + a) / 2)
                    if EARLY_LOG:
                        feed_tick("vis", now_, (b + a) / 2)
        except Exception as e:
            log.warning(f"binance: {e}")
        time.sleep(3)


# ── early-signal logger (paper only, never trades) ──────────────────────────
# Watches two faster Binance feeds next to the one we trade on, and logs the Polymarket book right after any
# smaller/earlier BTC move. Lets us test "act earlier" without risking money. /earlyexport downloads it.
EARLY_LOG = os.environ.get("EARLY_LOG", "true").strip().lower() in ("1", "true", "yes", "on")
EARLY_FEEDS = {"spot": "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker",
               "fut": "wss://fstream.binance.com/stream?streams=btcusdt@bookTicker"}
EARLY_BPS, EARLY_WIN, EARLY_COOL = 3.0, 2.0, 10.0
EARLY_SNAPS = (0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0)
feeds = {k: deque(maxlen=3000) for k in ("vis", "spot", "fut", "cl")}     # (t, mid) per feed
cross = deque(maxlen=200)                                          # (t, feed, level, sign) first crossings
flock = threading.Lock()
early_state = {"last": {1: 0.0, -1: 0.0}, "n": 0, "ticks": {"spot": 0, "fut": 0, "cl": 0}, "cl_err": ""}
CL_WS = "wss://ws-live-v2.polymarket.com/ws"     # Polymarket's live feed of the Chainlink price the markets settle on
cl_hist = deque(maxlen=4000)                      # (chainlink_ts, received_ts, value)
cl_opens = {}                                     # (tf, open_ts) -> Chainlink price at the window start


def feed_tick(name, now, px):
    with flock:
        q = feeds[name]
        q.append((now, px))
        while q and q[0][0] < now - EARLY_WIN:        # keep only the last 2s (cheap, so it never slows trading)
            q.popleft()
        old = q[0][1]
        bps = (px / old - 1) * 1e4
        for lvl in (2.0, 3.0, 5.0):
            if abs(bps) >= lvl:
                sg = 1 if bps > 0 else -1
                if not any(f == name and l == lvl and s == sg and now - t_ < 5 for t_, f, l, s in cross):
                    cross.append((now, name, lvl, sg))
        if name != "cl" and D_TRIG_LOW and abs(bps) >= D_TRIG_LOW:      # Nov 4: extra lower trigger for the D rules only
            sg_ = 1 if bps > 0 else -1
            if now - d_trig_last[sg_] >= EARLY_COOL:
                d_trig_last[sg_] = now
                threading.Thread(target=d_trigger_low, args=(now, name, bps), daemon=True).start()
        if not EARLY_LOG or abs(bps) < EARLY_BPS or name == "cl":
            return
        sg = 1 if bps > 0 else -1
        if now - early_state["last"][sg] < EARLY_COOL:
            return
        early_state["last"][sg] = now
    threading.Thread(target=early_capture, args=(now, name, bps), daemon=True).start()


# Nov 4: optional lower trigger for the D rules (no logging). D_TRIGGER_BP_5 / D_TRIGGER_BP_15 = 2 means that market's D rule also
# acts on 2bp moves (the 3bp trigger keeps working as before; one trade per market, so whichever move comes first is used).
def _trig_env(k):
    try:
        v = float(os.environ.get(k, "3"))
        return v if 0 < v < 3 else None
    except ValueError:
        return None
D_TRIG_BP = {5: _trig_env("D_TRIGGER_BP_5"), 15: _trig_env("D_TRIGGER_BP_15")}
D_TRIG_LOW = min([v for v in D_TRIG_BP.values() if v] or [0]) or None
d_trig_last = {1: 0.0, -1: 0.0}


def d_trigger_low(t0, feed, bps):
    try:
        if not D_ON:
            return
        up = bps > 0
        with lock:
            live = [(k, v) for k, v in markets.items() if k[1] <= t0 < k[1] + k[0] * 60]
        with flock:
            last = {k: (q[-1][1] if q else None) for k, q in feeds.items()}
        px = last.get("vis") or price["px"]
        s, siv = sigma(), sigma_iv()
        for (tf, o), toks in live:
            lvl = D_TRIG_BP.get(tf)
            if not lvl or abs(bps) < lvl or abs(bps) >= EARLY_BPS:   # 3bp+ moves already go through early_capture
                continue
            tok = toks[0] if up else toks[1]
            f, fi = fair_up(tf, o, t0, px), fair_up_iv(tf, o, t0, px)
            a, _b = _ladder(tok)
            if not a:
                continue
            threading.Thread(target=d_signal, args=(t0, feed, tf, o, tok, up, a[0][0], a[0][1],
                                                    None if f is None else round(f if up else 100 - f, 2),
                                                    None if fi is None else round(fi if up else 100 - fi, 2),
                                                    s, siv, last.get("vis")), daemon=True).start()
    except Exception as e:
        log.warning(f"d_trigger_low: {e}")


def _ladder(tok, n=6):
    with lock:
        b = books.get(tok) or {}
        a = sorted((b.get("asks") or {}).items())[:n]
        bi = sorted((b.get("bids") or {}).items(), reverse=True)[:n]
    return [[p, round(s, 1)] for p, s in a], [[p, round(s, 1)] for p, s in bi]


def early_capture(t0, feed, bps):
    try:
        up = bps > 0
        with lock:
            live = [(k, v) for k, v in markets.items() if k[1] <= t0 < k[1] + k[0] * 60]
        with flock:
            last = {k: (q[-1][1] if q else None) for k, q in feeds.items()}
        px = last.get("vis") or price["px"]
        rec = {"t0": t0, "feed": feed, "bps": round(bps, 2), "px": last, "sig": sigma(), "sig_iv": sigma_iv(), "mk": {}}
        for (tf, o), toks in live:
            f = fair_up(tf, o, t0, px)
            tok = toks[0] if up else toks[1]
            fi = fair_up_iv(tf, o, t0, px)
            rec["mk"][str(tf)] = {"o": o, "open": opens.get((tf, o)), "cl_open": cl_opens.get((tf, o)),
                                  "fair_iv": None if fi is None else round(fi if up else 100 - fi, 2), "left": round(o + tf * 60 - t0, 1),
                                  "fair": None if f is None else round(f if up else 100 - f, 2), "snaps": {}}
            if EXTRA_LOG:                                # Nov 5: real settlement rule (TWAP) strike + fair, paper only
                k_, src_ = tw_strike(tf, o)
                ft, fto = fair_tw(tf, o, t0, rec["sig_iv"]), fair_tw(tf, o, t0, rec["sig"])
                rec["mk"][str(tf)]["tw"] = {"k": k_, "src": src_, "fair_tw": None if ft is None else round(ft if up else 100 - ft, 2),
                                            "fair_two": None if fto is None else round(fto if up else 100 - fto, 2)}
            a, b = _ladder(tok)
            rec["mk"][str(tf)]["snaps"]["0"] = {"a": a, "b": b}
            if D_ON and a:                               # D rules (paper or live) decide from this same snapshot
                m_ = rec["mk"][str(tf)]
                threading.Thread(target=d_signal, args=(t0, feed, tf, o, tok, up, a[0][0], a[0][1], m_["fair"],
                                                        m_["fair_iv"], rec["sig"], rec["sig_iv"], last.get("vis")),
                                 daemon=True).start()
        toks_by_tf = {str(tf): (toks[0] if up else toks[1]) for (tf, o), toks in live}
        opp_by_tf = {str(tf): (toks[1] if up else toks[0]) for (tf, o), toks in live}       # Nov 4: the other side's book too
        for tf, tok in opp_by_tf.items():
            if tf in rec["mk"]:
                a, b = _ladder(tok, 3)
                rec["mk"][tf]["opp"] = {"0": {"a": a, "b": b}}
        for off in EARLY_SNAPS:
            time.sleep(max(0.0, t0 + off - time.time()))
            for tf, tok in toks_by_tf.items():
                a, b = _ladder(tok)
                rec["mk"][tf]["snaps"][f"{off:g}"] = {"a": a, "b": b}
            if off in (0.5, 1.0, 2.0, 10.0):
                for tf, tok in opp_by_tf.items():
                    if tf in rec["mk"] and "opp" in rec["mk"][tf]:
                        a, b = _ladder(tok, 3)
                        rec["mk"][tf]["opp"][f"{off:g}"] = {"a": a, "b": b}
            with flock:
                rec.setdefault("pxs", {})[f"{off:g}"] = {k: (q[-1][1] if q else None) for k, q in feeds.items()}
        with flock:
            rec["cross"] = [[round(t_ - t0, 3), f, l, s] for t_, f, l, s in cross if -5 <= t_ - t0 <= 10]
        rec["cl"] = [[round(ts - t0, 3), round(rt - t0, 3), v] for ts, rt, v in list(cl_hist) if -10 <= rt - t0 <= 12]
        rec["jump"] = round(state["last_jump"] - t0, 3) if abs(state["last_jump"] - t0) <= 10 else None
        if FLOW_LOG:
            rec["flow"] = flow_features(t0, 1 if up else -1, px)
        if EXTRA_LOG:
            rec["xtra"] = extra_features(t0, 1 if up else -1, toks_by_tf, opp_by_tf)
            rec["lead"] = lead_features(t0, 1 if up else -1, px)
        db_exec("INSERT INTO lj_early (t0, feed, data) VALUES (?,?,?)", (t0, feed, json.dumps(rec, separators=(",", ":"))))
        early_state["n"] += 1
    except Exception as e:
        log.warning(f"early: {e}")


# ── order-flow recorder (paper only, Nov 4): Binance trades (who is buying/selling), liquidations, order book depth, Coinbase price ──
# Attached to every early-logger move as rec["flow"], so the D rules can be tested with this extra information. FLOW_LOG=false turns it off.
FLOW_LOG = os.environ.get("FLOW_LOG", "true").strip().lower() in ("1", "true", "yes", "on")
FLOW_WS = {"fut": "wss://fstream.binance.com/stream?streams=btcusdt@depth20@100ms",                    # futures order book (public stream)
           "futm": "wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade/btcusdt@forceOrder", # futures trades + liquidations ("market" streams moved here)
           "spot": "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@depth20@100ms"}
CB_WS = "wss://ws-feed.exchange.coinbase.com"
flow_tr = {"fut": deque(maxlen=80000), "spot": deque(maxlen=40000)}     # (t, qty_btc, +1 buyer-initiated / -1 seller-initiated)
flow_liq = deque(maxlen=2000)                                          # (t, notional_usd, +1 shorts squeezed (forced buy) / -1 longs liquidated (forced sell))
flow_dep = {"fut": deque(maxlen=1500), "spot": deque(maxlen=1500)}     # (t, bid10, ask10, bid20, ask20)
flow_cb = deque(maxlen=4000)                                           # (t, coinbase price)
flow_state = {"err": "", "n": 0}


def flow_worker(venue):
    while True:
        try:
            ws = websocket.create_connection(FLOW_WS[venue], timeout=10)
            ws.settimeout(30)
            log.info(f"flow {venue} connected")
            vk = "fut" if venue.startswith("fut") else venue
            while True:
                m = json.loads(ws.recv())
                now = time.time()
                st, d = m.get("stream", ""), m.get("data") or {}
                if st.endswith("@aggTrade"):
                    flow_tr[vk].append((now, float(d["q"]), -1 if d.get("m") else 1))
                elif st.endswith("@forceOrder"):
                    o = d.get("o") or {}
                    flow_liq.append((now, float(o.get("q", 0)) * float(o.get("ap") or o.get("p") or 0), 1 if o.get("S") == "BUY" else -1))
                elif "@depth" in st:
                    b, a = d.get("b") or d.get("bids") or [], d.get("a") or d.get("asks") or []
                    sb = lambda L, n: sum(float(q) for _, q in L[:n])
                    flow_dep[vk].append((now, sb(b, 10), sb(a, 10), sb(b, 20), sb(a, 20)))
                flow_state["n"] += 1
        except Exception as e:
            flow_state["err"] = f"{venue}: {str(e)[:100]}"
            log.warning(f"flow {venue}: {e}")
        time.sleep(5)


def coinbase_worker():
    while True:
        try:
            ws = websocket.create_connection(CB_WS, timeout=10)
            ws.settimeout(30)
            ws.send(json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}))
            log.info("coinbase connected")
            while True:
                m = json.loads(ws.recv())
                if m.get("type") == "ticker" and m.get("price"):
                    flow_cb.append((time.time(), float(m["price"])))
        except Exception as e:
            flow_state["err"] = f"coinbase: {str(e)[:100]}"
            log.warning(f"coinbase: {e}")
        time.sleep(5)


def flow_features(t0, sg, vis0):
    """order-flow numbers around a move at t0, signed so + = in the move's direction (sg = +1 up / -1 down)"""
    out = {}
    try:
        def recent(q, since):                       # walk back from the newest item only as far as needed (keeps it cheap)
            out_ = []
            for x in reversed(q):
                if x[0] < since:
                    break
                out_.append(x)
            return out_
        for v, q in flow_tr.items():
            L = recent(q, t0 - 60)
            for lab, a, b in (("m60", -60, 0), ("m30", -30, 0), ("m10", -10, 0), ("m5", -5, 0), ("m2", -2, 0), ("p025", 0, 0.25), ("p05", 0, 0.5), ("p1", 0, 1.0)):
                vol = [(qq, s) for t, qq, s in L if t0 + a < t <= t0 + b]
                out[f"{v}_net_{lab}"] = round(sum(qq * s for qq, s in vol) * sg, 3)
                out[f"{v}_vol_{lab}"] = round(sum(qq for qq, s in vol), 3)
                if lab in ("m10", "m60"):
                    out[f"{v}_n_{lab}"] = len(vol)
                    out[f"{v}_big_{lab}"] = round(sum(qq * s for qq, s in vol if qq >= 2.0) * sg, 3)
        LQ = recent(flow_liq, t0 - 60)
        for lab, a in (("m60", -60), ("m10", -10)):
            z = [(n, s) for t, n, s in LQ if t0 + a < t <= t0]
            out[f"liq_net_{lab}"] = round(sum(n * s for n, s in z) * sg)
            out[f"liq_tot_{lab}"] = round(sum(n for n, s in z))
        for v, q in flow_dep.items():
            Dq = recent(q, t0 - 5)[::-1]
            pre = [x for x in Dq if x[0] <= t0]
            if pre:
                t, b10, a10, b20, a20 = pre[-1]
                out[f"{v}_imb10"] = round((b10 - a10) / max(b10 + a10, 1e-9) * sg, 4)
                out[f"{v}_imb20"] = round((b20 - a20) / max(b20 + a20, 1e-9) * sg, 4)
            post = [x for x in Dq if t0 < x[0] <= t0 + 0.5]
            if post:
                t, b10, a10, b20, a20 = post[-1]
                out[f"{v}_imb10_p05"] = round((b10 - a10) / max(b10 + a10, 1e-9) * sg, 4)
        CB = recent(flow_cb, t0 - 5)[::-1]
        at = lambda tt: next((p for t, p in reversed(CB) if t <= tt), None)
        c0, cm2, cp05 = at(t0), at(t0 - 2), at(t0 + 0.5)
        if c0 and vis0:
            out["cb_basis_bp"] = round((c0 / vis0 - 1) * 1e4 * sg, 2)
        if c0 and cm2:
            out["cb_move_m2"] = round((c0 / cm2 - 1) * 1e4 * sg, 2)
        if c0 and cp05:
            out["cb_move_p05"] = round((cp05 / c0 - 1) * 1e4 * sg, 2)
    except Exception as e:
        out["err"] = str(e)[:80]
    return out


# ── extra recorder (paper only, Nov 4). EXTRA_LOG=false turns it off. Table lj_extra (kind, data); /extraexport sends new rows. ──
#  win  : every 5s, for each live BTC 5m/15m market: both sides' top-3 book, BTC prices (vis/spot/fut/Chainlink), ETH/SOL, vol, Polymarket trades since last snapshot
#  pmt  : Polymarket trades (last_trade_price) in our BTC markets, flushed every 5s
#  meta : Polymarket's market data for each window, ~30s and ~120s after the start (to find the official start price / "price to beat")
#  bn   : once a minute, Binance futures funding, mark/index price, open interest, taker buy/sell ratio, long/short account ratio
# Every early-logger move also gets rec["xtra"]: ETH/SOL moves around the move and Polymarket trades in the 10s before / 1s after.
EXTRA_LOG = os.environ.get("EXTRA_LOG", "true").strip().lower() in ("1", "true", "yes", "on")
pm_trades = deque(maxlen=20000)                      # (t, token, price_c, size, side)
alt_px = {"ETH": deque(maxlen=3000), "SOL": deque(maxlen=3000)}   # (t, mid), at most ~5 per second
ALT_WS = "wss://data-stream.binance.vision/stream?streams=ethusdt@bookTicker/solusdt@bookTicker"
FAPI = "https://fapi.binance.com"
extra_state = {"err": "", "win": 0, "meta": 0, "bn": 0, "pmt": 0}


def alt_worker():
    while True:
        try:
            ws = websocket.create_connection(ALT_WS, timeout=10)
            ws.settimeout(30)
            log.info("alt coins feed connected")
            while True:
                m = json.loads(ws.recv())
                d = m.get("data") or {}
                c = {"ETHUSDT": "ETH", "SOLUSDT": "SOL"}.get(d.get("s"))
                if not c:
                    continue
                now = time.time()
                q = alt_px[c]
                if not q or now - q[-1][0] >= 0.2:
                    q.append((now, (float(d["b"]) + float(d["a"])) / 2))
        except Exception as e:
            extra_state["err"] = f"alt: {str(e)[:100]}"
            log.warning(f"alt: {e}")
        time.sleep(5)


def _alt_at(c, t):
    for tt, p in reversed(alt_px[c]):
        if tt <= t:
            return p
    return None


def extra_features(t0, sg, toks_by_tf, opp_by_tf):
    out = {}
    try:
        for c in alt_px:
            p0 = _alt_at(c, t0)
            for lab, dt in (("m2", -2), ("m10", -10), ("m60", -60), ("p05", 0.5), ("p1", 1.0)):
                p1 = _alt_at(c, t0 + dt)
                if p0 and p1:
                    out[f"{c.lower()}_{lab}"] = round(((p1 / p0 - 1) if dt > 0 else (p0 / p1 - 1)) * 1e4 * sg, 2)
        T = [x for x in list(pm_trades) if t0 - 10 <= x[0] <= t0 + 1]
        for tf in toks_by_tf:
            ours, opp = toks_by_tf[tf], opp_by_tf.get(tf)
            for lab, a, b in (("m10", -10, 0), ("p1", 0, 1)):
                our_v = sum(s for t, k, p, s, sd in T if k == ours and t0 + a <= t < t0 + b)
                opp_v = sum(s for t, k, p, s, sd in T if k == opp and t0 + a <= t < t0 + b)
                out[f"pm{tf}_ours_{lab}"] = round(our_v, 1)
                out[f"pm{tf}_opp_{lab}"] = round(opp_v, 1)
    except Exception as e:
        out["err"] = str(e)[:80]
    return out


def extra_recorder():
    """every 5s: window snapshots + Polymarket trades; meta fetches; Binance stats once a minute; prunes rows older than 7 days"""
    last_bn = 0.0
    last_prune = 0.0
    meta_done = set()
    flushed = 0.0
    while True:
        try:
            now = time.time()
            with lock:
                live = [(k, v) for k, v in markets.items() if k[1] <= now < k[1] + k[0] * 60]
            with flock:
                px = {k: (q[-1][1] if q else None) for k, q in feeds.items()}
            alts = {c.lower(): (q[-1][1] if q else None) for c, q in alt_px.items()}
            alts.update({v: (q[-1][1] if q else None) for v, q in lead_px.items()})
            alts["cb"] = flow_cb[-1][1] if flow_cb else None
            new_tr = [x for x in list(pm_trades) if x[0] > flushed]
            if new_tr:
                flushed = max(x[0] for x in new_tr)
            tok_map = {}
            for (tf, o), toks in live:
                tok_map[toks[0]] = (tf, o, "UP")
                tok_map[toks[1]] = (tf, o, "DOWN")
                ua, ub = _ladder(toks[0], 3)
                da, dbb = _ladder(toks[1], 3)
                vol = {}
                for t, k, p, s, sd in new_tr:
                    if k in toks:
                        key = "up" if k == toks[0] else "dn"
                        vol[key] = round(vol.get(key, 0) + s, 1)
                dep = {}
                with lock:                                  # total shares within 5c / 10c of the best price, each side of each book
                    for nm, tk in (("u", toks[0]), ("d", toks[1])):
                        bk = books.get(tk) or {}
                        A, B = bk.get("asks") or {}, bk.get("bids") or {}
                        if A:
                            ma = min(A)
                            dep[nm + "a5"], dep[nm + "a10"] = round(sum(z for q_, z in A.items() if q_ <= ma + 5), 1), round(sum(z for q_, z in A.items() if q_ <= ma + 10), 1)
                        if B:
                            mb = max(B)
                            dep[nm + "b5"], dep[nm + "b10"] = round(sum(z for q_, z in B.items() if q_ >= mb - 5), 1), round(sum(z for q_, z in B.items() if q_ >= mb - 10), 1)
                k_, src_ = tw_strike(tf, o)
                ft = fair_tw(tf, o, now, sigma_iv())
                tw = {"last": twap_hist[-1][2] if twap_hist else None, "k": k_, "src": src_, "fair_up": None if ft is None else round(ft, 2)}
                rec = {"tf": tf, "o": o, "into": round(now - o, 1), "tw": tw, "ua": ua, "ub": ub, "da": da, "db": dbb, "dep": dep, "px": px, "alt": alts,
                       "open": opens.get((tf, o)), "cl_open": cl_opens.get((tf, o)), "sig": sigma(), "sig_iv": sigma_iv(), "pmv": vol}
                db_exec("INSERT INTO lj_extra (ts, kind, data) VALUES (?,?,?)", (now, "win", json.dumps(rec, separators=(",", ":"))))
                extra_state["win"] += 1
            twn = [[round(ts, 3), v] for ts, rt, v in list(twap_hist) if ts > twap_state["flushed"]]
            if twn:
                twap_state["flushed"] = twn[-1][0]
                db_exec("INSERT INTO lj_extra (ts, kind, data) VALUES (?,?,?)", (now, "twap", json.dumps(twn, separators=(",", ":"))))
            mine = [[round(t, 2)] + list(tok_map[k]) + [p, s, sd] for t, k, p, s, sd in new_tr if k in tok_map]
            if mine:
                db_exec("INSERT INTO lj_extra (ts, kind, data) VALUES (?,?,?)", (now, "pmt", json.dumps(mine, separators=(",", ":"))))
                extra_state["pmt"] += len(mine)
            for (tf, o), toks in live:                     # Polymarket market data ~30s and ~120s after the start
                for stage in (30, 120):
                    if (tf, o, stage) not in meta_done and now - o >= stage:
                        meta_done.add((tf, o, stage))
                        threading.Thread(target=_fetch_meta, args=(tf, o, stage), daemon=True).start()
            if now - last_bn >= 60:
                last_bn = now
                threading.Thread(target=_fetch_bn, daemon=True).start()
            if now - last_prune >= 3600:
                last_prune = now
                db_exec("DELETE FROM lj_extra WHERE ts < ?", (now - 7 * 86400,))
                meta_done = {x for x in meta_done if now - x[1] < 7200}
        except Exception as e:
            extra_state["err"] = f"recorder: {str(e)[:100]}"
            log.warning(f"extra recorder: {e}")
        time.sleep(max(0.5, 5 - (time.time() - now)))


def _fetch_meta(tf, o, stage):
    try:
        arr = requests.get(f"{GAMMA}/events", params={"slug": f"btc-updown-{tf}m-{o}"}, timeout=10).json()
        ev = arr[0] if isinstance(arr, list) and arr else {}
        keep = lambda d: {k: v for k, v in (d or {}).items()
                          if any(w in k.lower() for w in ("price", "metadata", "strike", "threshold", "reference", "target", "start", "end", "resolution", "description", "outcome"))}
        mk = (ev.get("markets") or [{}])[0]
        rec = {"tf": tf, "o": o, "stage": stage, "event": keep(ev), "market": keep(mk)}
        for d in (rec["event"], rec["market"]):
            if isinstance(d.get("description"), str):
                d["description"] = d["description"][:600]
        db_exec("INSERT INTO lj_extra (ts, kind, data) VALUES (?,?,?)", (time.time(), "meta", json.dumps(rec, separators=(",", ":"), default=str)))
        extra_state["meta"] += 1
    except Exception as e:
        extra_state["err"] = f"meta: {str(e)[:100]}"


def _fetch_bn():
    out = {}
    for name, path, params in (("premium", "/fapi/v1/premiumIndex", {"symbol": "BTCUSDT"}), ("oi", "/fapi/v1/openInterest", {"symbol": "BTCUSDT"}),
                               ("taker", "/futures/data/takerlongshortRatio", {"symbol": "BTCUSDT", "period": "5m", "limit": 1}),
                               ("lsr", "/futures/data/globalLongShortAccountRatio", {"symbol": "BTCUSDT", "period": "5m", "limit": 1}),
                               ("top", "/futures/data/topLongShortPositionRatio", {"symbol": "BTCUSDT", "period": "5m", "limit": 1}),
                               ("bybit", "https://api.bybit.com/v5/market/tickers", {"category": "linear", "symbol": "BTCUSDT"}),
                               ("okx_fund", "https://www.okx.com/api/v5/public/funding-rate", {"instId": "BTC-USDT-SWAP"}),
                               ("okx_oi", "https://www.okx.com/api/v5/public/open-interest", {"instId": "BTC-USDT-SWAP"})):
        try:
            out[name] = requests.get(path if path.startswith("http") else FAPI + path, params=params, timeout=8).json()
        except Exception as e:
            out[name] = {"err": str(e)[:80]}
    try:
        db_exec("INSERT INTO lj_extra (ts, kind, data) VALUES (?,?,?)", (time.time(), "bn", json.dumps(out, separators=(",", ":"))))
        extra_state["bn"] += 1
    except Exception as e:
        extra_state["err"] = f"bn: {str(e)[:100]}"


# ── lead recorder (paper only, Nov 4): which exchange moves first. Best bid/ask from Bybit, OKX, Hyperliquid (perps), Kraken, Bitstamp (spot),
# kept next to Binance (vis/spot/fut), Coinbase and Chainlink. Every early move gets rec["lead"]: for each venue the move in the 2s/10s
# before and 0.5s/1s after (signed our way), when it first crossed 1/2/3bp our way (relative to its price 2s before), and its gap to Binance.
lead_px = {v: deque(maxlen=6000) for v in ("bybit", "okx", "hl", "kraken", "bitstamp")}   # (t, mid), at most 20 per second
lead_state = {v: 0 for v in lead_px}
lead_state["err"] = ""


def _lead_push(v, b, a):
    if b and a and b > 0 and a > 0:
        now = time.time()
        q = lead_px[v]
        if not q or now - q[-1][0] >= 0.05:
            q.append((now, (b + a) / 2))
            lead_state[v] += 1


def lead_worker(v):
    cfg = {"bybit": ("wss://stream.bybit.com/v5/public/linear", {"op": "subscribe", "args": ["orderbook.1.BTCUSDT"]}, {"op": "ping"}),
           "okx": ("wss://ws.okx.com:8443/ws/v5/public", {"op": "subscribe", "args": [{"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"}]}, "ping"),
           "hl": ("wss://api.hyperliquid.xyz/ws", {"method": "subscribe", "subscription": {"type": "bbo", "coin": "BTC"}}, {"method": "ping"}),
           "kraken": ("wss://ws.kraken.com/v2", {"method": "subscribe", "params": {"channel": "ticker", "symbol": ["BTC/USD"], "event_trigger": "bbo"}}, {"method": "ping"}),
           "bitstamp": ("wss://ws.bitstamp.net", {"event": "bts:subscribe", "data": {"channel": "order_book_btcusd"}}, None)}
    url, sub, ping = cfg[v]
    while True:
        try:
            ws = websocket.create_connection(url, timeout=10)
            ws.settimeout(30)
            ws.send(json.dumps(sub))
            log.info(f"lead {v} connected")
            last_ping, bb, ba = time.time(), None, None
            while True:
                if ping and time.time() - last_ping > 15:
                    ws.send(ping if isinstance(ping, str) else json.dumps(ping))
                    last_ping = time.time()
                raw = ws.recv()
                if raw in ("pong", "") or not raw.startswith("{"):
                    continue
                m = json.loads(raw)
                if v == "bybit":
                    d = m.get("data") or {}
                    if d.get("b") and float(d["b"][0][1]) > 0:
                        bb = float(d["b"][0][0])
                    if d.get("a") and float(d["a"][0][1]) > 0:
                        ba = float(d["a"][0][0])
                    if d:
                        _lead_push(v, bb, ba)
                elif v == "okx":
                    for d in m.get("data") or []:
                        if d.get("bids") and d.get("asks"):
                            _lead_push(v, float(d["bids"][0][0]), float(d["asks"][0][0]))
                elif v == "hl":
                    d = m.get("data") or {}
                    bbo = d.get("bbo") if isinstance(d, dict) else None
                    if bbo and len(bbo) == 2 and bbo[0] and bbo[1]:
                        _lead_push(v, float(bbo[0]["px"]), float(bbo[1]["px"]))
                elif v == "kraken":
                    if m.get("channel") == "ticker":
                        for d in m.get("data") or []:
                            _lead_push(v, float(d.get("bid") or 0), float(d.get("ask") or 0))
                elif v == "bitstamp":
                    d = m.get("data") or {}
                    if d.get("bids") and d.get("asks"):
                        _lead_push(v, float(d["bids"][0][0]), float(d["asks"][0][0]))
        except Exception as e:
            lead_state["err"] = f"{v}: {str(e)[:100]}"
            log.warning(f"lead {v}: {e}")
        time.sleep(5)


def lead_features(t0, sg, vis0):
    """per venue: move before/after t0 (bp, + = our way), first time it crossed 1/2/3bp our way vs its price 2s before, gap to Binance"""
    out = {}
    try:
        venues = dict(lead_px)
        venues["cb"] = flow_cb
        for v, q in venues.items():
            L = [x for x in list(q) if t0 - 61 <= x[0] <= t0 + 2.5]
            if not L:
                continue
            at = lambda tt: next((p for t, p in reversed(L) if t <= tt), None)
            p0 = at(t0)
            if not p0:
                continue
            r = {"age": round(t0 - next(t for t, p in reversed(L) if t <= t0), 3)}
            for lab, dt in (("m60", -60), ("m10", -10), ("m2", -2), ("p05", 0.5), ("p1", 1.0), ("p2", 2.0)):
                p1 = at(t0 + dt)
                if p1:
                    r[lab] = round(((p1 / p0 - 1) if dt > 0 else (p0 / p1 - 1)) * 1e4 * sg, 2)
            base = at(t0 - 2)
            if base:
                for lvl in (1, 2, 3):
                    hit = next((t for t, p in L if t >= t0 - 2 and (p / base - 1) * 1e4 * sg >= lvl), None)
                    r[f"x{lvl}"] = None if hit is None else round(hit - t0, 3)
            if vis0:
                r["gap"] = round((p0 / vis0 - 1) * 1e4, 2)
            out[v] = r
    except Exception as e:
        out["err"] = str(e)[:80]
    return out


# ── TWAP recorder (paper only, Nov 5). The 5m/15m markets settle on Chainlink's 60-second TWAP: UP if TWAP60 at the end second >= TWAP60 at
# the start second (since Aug 2026). TWAP60(T) ~ average Chainlink price over about [T-62s, T-3s]. So the real "price to beat" is the average of
# the minute BEFORE the window, and the final price is the average of the last minute. Polymarket streams it (price.crypto.twap, 60s).
# Recorded: every TWAP point (lj_extra kind='twap'), the exact start value per window, and on every early move rec["mk"][tf]["tw"] =
# strike + a corrected fair price (fair_tw = Deribit vol, fair_two = our vol), side-flipped like the others. Nothing trades on it yet.
twap_hist = deque(maxlen=8000)                    # (twap_ts, received_ts, value)
tw_opens = {}                                     # (tf, open_ts) -> TWAP60 value at the start second (the real price to beat)
twap_state = {"n": 0, "err": "", "flushed": 0.0}


def twap_worker():
    while True:
        try:
            cr = clob().creds
            ws = websocket.create_connection(CL_WS, timeout=10)
            ws.settimeout(30)
            ws.send(json.dumps({"op": "auth", "rid": "a2", "auth": {"apiKey": cr.api_key, "secret": cr.api_secret, "passphrase": cr.api_passphrase}}))
            ws.send(json.dumps({"op": "subscribe", "rid": "s2", "subscriptions": [{"channel": "price.crypto.twap", "filter": {"symbol": "btcusd", "window_seconds": 60}}]}))
            log.info("twap feed connected")
            last_ping = time.time()
            while True:
                if time.time() - last_ping > 15:
                    ws.ping()
                    last_ping = time.time()
                raw = ws.recv()
                now = time.time()
                try:
                    m = json.loads(raw)
                except Exception:
                    continue
                if isinstance(m, dict) and m.get("op") == "error":
                    twap_state["err"] = str(m)[:150]
                if not isinstance(m, dict) or "twap" not in str(m.get("channel", "")):
                    continue
                for ts, v in sorted(_cl_points(m)):
                    if twap_hist and ts <= twap_hist[-1][0]:
                        continue
                    twap_hist.append((ts, now, v))
                    twap_state["n"] += 1
                    for tf in TFS:
                        o = window(tf, ts)[0]
                        if (tf, o) not in tw_opens and abs(ts - o) < 0.5:
                            tw_opens[(tf, o)] = v
        except Exception as e:
            twap_state["err"] = str(e)[:150]
            log.warning(f"twap: {e}")
        time.sleep(10)


def tw_strike(tf, o):
    """(price to beat, source): exact TWAP at the start second if we saw it, else our own average of Chainlink ticks over [o-62, o-3]"""
    if (tf, o) in tw_opens:
        return tw_opens[(tf, o)], "feed"
    pts = [v for ts, rt, v in list(cl_hist) if o - 62 <= ts <= o - 3]
    if len(pts) >= 20:
        return sum(pts) / len(pts), "cl"
    return None, None


def fair_tw(tf, o, now, s):
    """chance (0-100) that UP wins under the real rule: final = average Chainlink price over [end-62, end-3] >= TWAP strike"""
    try:
        k, _src = tw_strike(tf, o)
        if not k or not s or not cl_hist:
            return None
        e = o + tf * 60
        a, b = e - 62, e - 3
        H = list(cl_hist)
        ts_l, rt_l, x = H[-1]
        bn = [p for t, p in list(px1s) if t <= rt_l]
        if bn and px1s:                                # bring the last Chainlink price up to now using Binance's move since then
            x = x * px1s[-1][1] / bn[-1]
        if now >= b:
            pts = [v for ts, rt, v in H if a <= ts <= b]
            return None if not pts else (100.0 if sum(pts) / len(pts) >= k else 0.0)
        known = [v for ts, rt, v in H if a <= ts <= now] if now > a else []
        f = min(1.0, (now - a) / (b - a)) if (now > a and known) else 0.0
        start = max(now, a)
        d, L = start - now, b - start
        mean = (sum(known) / len(known) if known else x) * f + x * (1 - f)
        sd = (1 - f) * s * math.sqrt(max(1e-9, d + L / 3.0))
        return 100.0 * norm_cdf(math.log(mean / k) / sd)
    except Exception:
        return None


def _cl_points(m):
    """pull (timestamp_ms, value) points out of a feed message, whatever its exact shape"""
    out = []
    def walk(x):
        if isinstance(x, dict):
            v, ts = x.get("value"), x.get("timestamp") or x.get("ts")
            if v is not None and ts is not None:
                try:
                    try:
                        tsf = float(ts)
                    except (TypeError, ValueError):
                        tsf = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp()
                    out.append((tsf / 1000 if tsf > 1e11 else tsf, float(v)))
                    return
                except (TypeError, ValueError):
                    pass
            for k in ("payload", "data", "snapshot"):
                if k in x:
                    walk(x[k])
        elif isinstance(x, list):
            for y in x:
                walk(y)
    walk(m)
    return out


def chainlink_worker():
    seen = 0
    while True:
        try:
            cr = clob().creds
            ws = websocket.create_connection(CL_WS, timeout=10)
            ws.settimeout(30)
            ws.send(json.dumps({"op": "auth", "rid": "a1", "auth": {"apiKey": cr.api_key, "secret": cr.api_secret,
                                                                    "passphrase": cr.api_passphrase}}))
            ws.send(json.dumps({"op": "subscribe", "rid": "s1",
                                "subscriptions": [{"channel": "price.crypto", "filter": {"symbol": "btcusd"}}]}))
            log.info("chainlink feed connected")
            last_ping = time.time()
            while True:
                if time.time() - last_ping > 15:
                    ws.ping()
                    last_ping = time.time()
                raw = ws.recv()
                now = time.time()
                try:
                    m = json.loads(raw)
                except Exception:
                    continue
                if seen < 3:                                 # first few frames to the log, to check the format
                    log.info(f"chainlink frame: {str(m)[:300]}")
                    seen += 1
                if isinstance(m, dict) and m.get("op") in ("error",):
                    early_state["cl_err"] = str(m)[:150]
                pts = _cl_points(m)
                for ts, v in pts:
                    if cl_hist and ts <= cl_hist[-1][0]:
                        continue
                    cl_hist.append((ts, now, v))
                    early_state["ticks"]["cl"] += 1
                    feed_tick("cl", now, v)
                    for tf in TFS:
                        o = window(tf, ts)[0]
                        if (tf, o) not in cl_opens and ts - o < 5:
                            cl_opens[(tf, o)] = v
        except Exception as e:
            early_state["cl_err"] = str(e)[:150]
            log.warning(f"chainlink: {e}")
        time.sleep(10)


# ── close logger (paper only): the last 10 seconds of every 5m/15m window ──────────────────────────
# The market settles on Chainlink, which runs a little behind Binance. So in the final second the settlement
# price may already be (almost) known. This logs both books + all feeds around each close to test that.
CLOSE_SNAPS = (-10, -5, -3, -2, -1.5, -1, -0.75, -0.5, -0.25, -0.1, 0.25, 1, 3)


def close_capture(tf, o):
    C = o + tf * 60
    try:
        with lock:
            toks = markets.get((tf, o))
        if not toks:
            return
        rec = {"tf": tf, "o": o, "close": C, "open": opens.get((tf, o)), "cl_open": cl_opens.get((tf, o)),
               "sig": sigma(), "sig_iv": sigma_iv(), "snaps": {}}
        for off in CLOSE_SNAPS:
            time.sleep(max(0.0, C + off - time.time()))
            ua, ub = _ladder(toks[0])
            da, db = _ladder(toks[1])
            with flock:
                px = {k: (q[-1][1] if q else None) for k, q in feeds.items()}
            lc = cl_hist[-1] if cl_hist else None
            rec["snaps"][f"{off:g}"] = {"t": round(time.time() - C, 3), "ua": ua, "ub": ub, "da": da, "db": db, "px": px,
                                       "cl": None if not lc else [round(lc[0] - C, 3), round(lc[1] - C, 3), lc[2]]}
        time.sleep(2)
        pts = [(ts, v) for ts, rt, v in list(cl_hist) if ts <= C + 0.001]
        rec["cl_close"] = pts[-1][1] if pts else None              # last Chainlink value at or before the close
        rec["cl_close_ts"] = round(pts[-1][0] - C, 3) if pts else None
        rec["cl_tail"] = [[round(ts - C, 3), round(rt - C, 3), v] for ts, rt, v in list(cl_hist) if -15 <= ts - C <= 5]
        db_exec("INSERT INTO lj_close (close_ts, tf, open_ts, data) VALUES (?,?,?,?)",
                (C, tf, o, json.dumps(rec, separators=(",", ":"))))
        early_state["closes"] = early_state.get("closes", 0) + 1
    except Exception as e:
        log.warning(f"close log: {e}")


def close_worker():
    done = set()
    while True:
        try:
            now = time.time()
            for tf in TFS:
                o, C = window(tf, now)
                if C - now <= 11 and (tf, o) not in done:
                    done.add((tf, o))
                    threading.Thread(target=close_capture, args=(tf, o), daemon=True).start()
            if len(done) > 500:
                done = {k for k in done if k[1] > now - 3600}
        except Exception as e:
            log.warning(f"close worker: {e}")
        time.sleep(0.5)


def close_outcomes():
    """fill in who won, a few minutes after each close"""
    while True:
        try:
            for r in db_query("SELECT id, tf, open_ts FROM lj_close WHERE outcome IS NULL AND close_ts < ? LIMIT 20",
                              (time.time() - 150,)):
                out = outcome(r["tf"], r["open_ts"])
                if out:
                    db_exec("UPDATE lj_close SET outcome=? WHERE id=?", (out, r["id"]))
                time.sleep(1)
        except Exception as e:
            log.warning(f"close outcomes: {e}")
        time.sleep(60)


def early_feed_worker(name):
    url = EARLY_FEEDS[name]
    while True:
        try:
            ws = websocket.create_connection(url, timeout=10)
            ws.settimeout(30)
            log.info(f"early feed {name} connected")
            while True:
                d = json.loads(ws.recv()).get("data", {})
                b, a = float(d.get("b", 0)), float(d.get("a", 0))
                if b > 0 and a > 0:
                    early_state["ticks"][name] += 1
                    feed_tick(name, time.time(), (b + a) / 2)
        except Exception as e:
            log.warning(f"early feed {name}: {e}")
        time.sleep(5)


# ── options volatility (paper comparison only) ─────────────────────────────
DERIBIT = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency"
ivs = {"iv": None, "ts": 0.0, "exp_h": None, "err": ""}


def deribit_atm_iv():
    """at-the-money implied vol of the nearest BTC options expiry at least 2h away (annual, e.g. 0.36)"""
    r = requests.get(DERIBIT, params={"currency": "BTC", "kind": "option"}, timeout=15).json()
    now = time.time()
    by = {}
    for x in r.get("result", []):
        p = (x.get("instrument_name") or "").split("-")
        if len(p) != 4 or not x.get("mark_iv"):
            continue
        try:
            e = datetime.strptime(p[1], "%d%b%y").replace(hour=8, tzinfo=timezone.utc).timestamp()
            K, F = float(p[2]), float(x.get("underlying_price") or 0)
        except Exception:
            continue
        if e - now < 7200 or F <= 0:
            continue
        by.setdefault(e, {"F": F, "pts": []})["pts"].append((abs(K / F - 1), float(x["mark_iv"]) / 100.0))
    if not by:
        return None, None
    e = min(by)
    allp = sorted(by[e]["pts"])
    pts = [x for x in allp if x[0] <= 0.03][:4] or allp[:2]   # options within 3% of the price (closest 2 if none)
    return sum(v for _, v in pts) / len(pts), (e - now) / 3600


def deribit_worker():
    while True:
        try:
            iv, eh = deribit_atm_iv()
            if iv:
                ivs.update(iv=iv, ts=time.time(), exp_h=eh, err="")
        except Exception as e:
            ivs["err"] = str(e)[:100]
            log.warning(f"deribit: {e}")
        time.sleep(60)


# ── Deribit extra logger (paper research only, added Nov 2 2026) ─────────────────────────────────
# Every 60s saves one row to lj_deribit with: ATM vol for the nearest expiries (incl. same-day), skew (OTM put vol minus OTM
# call vol at about +/-1% and +/-2% from the price), perpetual futures price / funding / open interest, and option + perpetual
# trades in the last minute (buy/sell volume, block trades, biggest trade). /deribitexport downloads it. Never trades.
DERIBIT_LOG = os.environ.get("DERIBIT_LOG", "true").strip().lower() in ("1", "true", "yes", "on")
DB_API = "https://www.deribit.com/api/v2/public"
dlog_state = {"n": 0, "err": ""}


def _dget(path, **params):
    r = requests.get(f"{DB_API}/{path}", params=params, timeout=15).json()
    return r.get("result")


def deribit_snapshot():
    now = time.time()
    rec = {"ts": now}
    opts = _dget("get_book_summary_by_currency", currency="BTC", kind="option") or []
    by = {}
    for x in opts:
        p = (x.get("instrument_name") or "").split("-")
        if len(p) != 4 or not x.get("mark_iv"):
            continue
        try:
            e = datetime.strptime(p[1], "%d%b%y").replace(hour=8, tzinfo=timezone.utc).timestamp()
            K, F = float(p[2]), float(x.get("underlying_price") or 0)
        except Exception:
            continue
        if e <= now or F <= 0:
            continue
        by.setdefault(e, []).append((K, F, p[3], float(x["mark_iv"]), float(x.get("open_interest") or 0), float(x.get("volume") or 0)))
    exps = []
    for e in sorted(by)[:4]:                                  # the 4 nearest expiries (first is usually same-day)
        pts = by[e]; F = pts[0][1]
        def iv_at(m, kind):
            c = [(abs(K / F - m), iv) for K, F_, k, iv, oi, v in pts if k == kind]
            return min(c)[1] if c else None
        atm = [iv for K, F_, k, iv, oi, v in pts if abs(K / F - 1) <= 0.01] or [iv for _, iv in sorted((abs(K / F - 1), iv) for K, F_, k, iv, oi, v in pts)[:2]]
        exps.append({"exp": e, "hours": round((e - now) / 3600, 2), "F": F, "atm_iv": round(sum(atm) / len(atm), 2),
                     "skew1": None if iv_at(0.99, "P") is None or iv_at(1.01, "C") is None else round(iv_at(0.99, "P") - iv_at(1.01, "C"), 2),
                     "skew2": None if iv_at(0.98, "P") is None or iv_at(1.02, "C") is None else round(iv_at(0.98, "P") - iv_at(1.02, "C"), 2),
                     "put_oi": round(sum(oi for K, F_, k, iv, oi, v in pts if k == "P"), 1), "call_oi": round(sum(oi for K, F_, k, iv, oi, v in pts if k == "C"), 1),
                     "put_vol24": round(sum(v for K, F_, k, iv, oi, v in pts if k == "P"), 1), "call_vol24": round(sum(v for K, F_, k, iv, oi, v in pts if k == "C"), 1)})
    rec["expiries"] = exps
    fut = _dget("get_book_summary_by_currency", currency="BTC", kind="future") or []
    perp = next((x for x in fut if x.get("instrument_name") == "BTC-PERPETUAL"), None)
    if perp:
        rec["perp"] = {k: perp.get(k) for k in ("mark_price", "last", "funding_8h", "current_funding", "open_interest", "estimated_delivery_price", "volume_usd")}
    end = int(now * 1000); start = end - 60000
    for kind in ("option", "future"):
        tr = (_dget("get_last_trades_by_currency_and_time", currency="BTC", kind=kind, start_timestamp=start, end_timestamp=end, count=1000) or {}).get("trades", [])
        s = {"n": len(tr), "blocks": sum(1 for t in tr if t.get("block_trade_id"))}
        if kind == "option":
            for side in ("buy", "sell"):
                for typ in ("C", "P"):
                    s[f"{typ}_{side}"] = round(sum(t.get("amount", 0) for t in tr if t.get("direction") == side and t.get("instrument_name", "").endswith("-" + typ)), 2)
            big = max(tr, key=lambda t: t.get("amount", 0), default=None)
            s["biggest"] = None if not big else {k: big.get(k) for k in ("instrument_name", "direction", "amount", "iv", "block_trade_id")}
        else:
            tr = [t for t in tr if t.get("instrument_name") == "BTC-PERPETUAL"]
            s["n"] = len(tr)
            s["buy_usd"] = round(sum(t.get("amount", 0) for t in tr if t.get("direction") == "buy"))
            s["sell_usd"] = round(sum(t.get("amount", 0) for t in tr if t.get("direction") == "sell"))
            big = max(tr, key=lambda t: t.get("amount", 0), default=None)
            s["biggest"] = None if not big else {k: big.get(k) for k in ("direction", "amount", "price")}
        rec["trades_" + kind] = s
    return rec


def deribit_logger():
    while True:
        try:
            rec = deribit_snapshot()
            db_exec("INSERT INTO lj_deribit (ts, data) VALUES (?, ?)", (rec["ts"], json.dumps(rec, separators=(",", ":"))))
            dlog_state["n"] += 1
            dlog_state["err"] = ""
        except Exception as e:
            dlog_state["err"] = str(e)[:150]
            log.warning(f"deribit logger: {e}")
        time.sleep(60)


def sigma_iv():
    if not ivs["iv"] or time.time() - ivs["ts"] > 600:
        return None
    return ivs["iv"] / math.sqrt(365 * 86400)


def fair_up_iv(tf, o, now, px):
    """same fair price, but using the options market's volatility instead of the last 15 minutes"""
    s = sigma_iv()
    op = opens.get((tf, o))
    left = o + tf * 60 - now
    if not s or not op or not px or left <= 0:
        return None
    return 100.0 * norm_cdf(math.log(px / op) / (s * math.sqrt(left)))


# ── model ───────────────────────────────────────────────────────────────────
def norm_cdf(z):
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def sigma():
    pts = [p for p in list(px1s) if p[0] >= time.time() - 900]
    r = [(math.log(b / a)) ** 2 / (t1 - t0) for (t0, a), (t1, b) in zip(pts, pts[1:]) if t1 > t0 and a > 0 and b > 0]
    if len(r) < 30:
        return 6e-5
    return max(2e-5, math.sqrt(sum(r) / len(r)))


def fair_up(tf, o, now, px):
    op = opens.get((tf, o))
    left = o + tf * 60 - now
    if not op or not px or left <= 0:
        return None
    return 100.0 * norm_cdf(math.log(px / op) / (sigma() * math.sqrt(left)))


def best(tok, kind):
    with lock:
        b = books.get(tok)
        if not b or not b[kind]:
            return None
        return max(b[kind]) if kind == "bids" else min(b[kind])


# ── the trade ───────────────────────────────────────────────────────────────
def risk_check(pending=0):
    if paused():
        return "paused (/go to resume)"
    t_day = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    rows = db_query("SELECT pnl, buy_cost, status, t0 FROM lj_trades WHERE mode='LIVE' AND t0>=?", (t_day,))
    realized = sum(r["pnl"] or 0 for r in rows if r["pnl"] is not None)
    open_cost = sum(r["buy_cost"] or 0 for r in rows if r["pnl"] is None and r["status"] in ("BOUGHT", "HOLDING"))
    open_cost += LIVE_STAKE * pending
    if realized - open_cost - LIVE_STAKE < -LIVE_DAILY_LOSS - 1e-9:
        return f"daily loss limit ${LIVE_DAILY_LOSS:g} reached (today {money(realized)})"
    last_hour = sum(1 for r in rows if r["t0"] >= time.time() - 3600)
    if last_hour + pending >= LIVE_MAX_TRADES_HOUR:
        return f"{last_hour} live trades in the last hour (max {LIVE_MAX_TRADES_HOUR})"
    return None


def on_jump(t0, bps, px0):
    up = bps > 0
    for k in [k for k in list(extra) if k[0] < t0 - 60]:
        extra.pop(k, None)
    fairs = {}
    with lock:
        live = [(k, v) for k, v in markets.items() if k[1] <= t0 < k[1] + k[0] * 60]
    for (tf, o), toks in live:
        f = fair_up(tf, o, t0, px0)
        if f is not None:
            fairs[(tf, o)] = (toks, f if up else 100 - f)
            fi = fair_up_iv(tf, o, t0, px0)
            extra[(t0, tf)] = {"fair_iv": None if fi is None else (fi if up else 100 - fi), "sig": sigma(),
                               "sig_iv": sigma_iv(), "cl_open": cl_opens.get((tf, o)), "bn_open": opens.get((tf, o))}
    time.sleep(max(0.0, t0 + EDGE_DELAY - time.time()))
    sigs = []
    for (tf, o), (toks, fair) in fairs.items():
        tok = toks[0] if up else toks[1]
        ask, bid = best(tok, "asks"), best(tok, "bids")
        if ask is None or bid is None or not (5 <= ask <= 95):
            continue
        edge = fair - ask
        if not (edge >= MIN_EDGE and ask < MAX_ASK and (ask >= CHEAP_CENTS or edge >= CHEAP_EDGE)):
            continue
        state["signals"] += 1
        sigs.append((t0, tf, o, tok, "UP" if up else "DOWN", bps, fair, ask, bid, edge))
    if not sigs:
        return
    sigs.sort(key=lambda s: -s[9])                 # biggest edge goes out first
    if LIVE_ENABLED and BATCH_ORDERS and len([s for s in sigs if s[1] in LIVE_TFS]) > 1:
        for s in sigs:
            if s[1] not in LIVE_TFS:
                threading.Thread(target=trade, args=s, daemon=True).start()
        live_group([s for s in sigs if s[1] in LIVE_TFS])
        return
    for s in sigs:
        threading.Thread(target=trade, args=s, daemon=True).start()


extra = {}                                  # (t0, tf) -> options fair & friends, picked up when the signal is logged


def _head(tf, side, bps, fair, ask, edge):
    return f"{tf}m {side} {edge:+.0f}¢"


def short_why(note, cap):
    n = (note or "").lower()
    if "no orders found to match" in n or "not filled" in n or "status=" in n:
        return f"price ran past {cap:.0f}¢"
    if "not enough balance" in n or "allowance" in n:
        return "not enough USDC"
    return (note or "?")[:60]


def admit(t0, tf, o, tok, side, bps, fair, ask, bid, edge, mode, pending=0):
    """log the signal; for LIVE, check the limits and lock the market. Returns (rid, head, allowed)."""
    head = _head(tf, side, bps, fair, ask, edge)
    x = extra.pop((t0, tf), {})
    rid = db_exec("INSERT INTO lj_trades (t0,tf,open_ts,side,bps,fair,ask,bid0,edge,mode,status,fair_iv,sig,sig_iv,cl_open,bn_open) "
                  "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  (t0, tf, o, side, bps, fair, ask, bid, edge, mode, "SIGNAL", x.get("fair_iv"), x.get("sig"),
                   x.get("sig_iv"), x.get("cl_open"), x.get("bn_open")))
    if mode != "LIVE":
        return rid, head, True
    with trade_lock:
        if (tf, o) in busy:
            db_exec("UPDATE lj_trades SET mode='SKIPPED', status='SKIPPED', note=? WHERE id=?", ("position already open", rid))
            return rid, head, False
        why = risk_check(pending)
        if why:
            db_exec("UPDATE lj_trades SET mode='BLOCKED', status='BLOCKED', note=? WHERE id=?", (why, rid))
            tg(f"⛔ {head} · skipped: {why}")
            return rid, head, False
        busy.add((tf, o))
    return rid, head, True


def buy_cap(ask, fair):
    return max(ask + LIVE_MAX_OVERPAY, min(ask + SMART_MAX, fair - KEEP_EDGE))


def live_group(sigs):
    """Two or more signals on the same jump: send all the buys in one request so none waits behind another."""
    acc = []
    for s in sigs:
        rid, head, ok = admit(*s, mode="LIVE", pending=len(acc))
        if ok:
            acc.append((s, rid, head))
    if not acc:
        return
    if len(acc) == 1:
        s, rid, head = acc[0]
        t0, tf, o, tok, side, bps, fair, ask, bid, edge = s
        _run_live(rid, head, tf, o, tok, side, ask, t0, fair)
        return
    tb = time.time()
    items, built = [], []
    for s, rid, head in acc:
        t0, tf, o, tok, side, bps, fair, ask, bid, edge = s
        cap = buy_cap(ask, fair)
        try:
            items.append(build_order(tok, "BUY", LIVE_STAKE, cap))
            built.append((s, rid, head, cap))
        except Exception as e:
            db_exec("UPDATE lj_trades SET mode='MISSED', status='MISSED', note=? WHERE id=?", (f"build: {str(e)[:150]}", rid))
            tg(f"⏭ {head} · order error: {str(e)[:60]}")
            busy.discard((tf, o))
    if not built:
        return
    t1 = time.time()
    try:
        resps, unsure = (batch_post(clob(), items) if len(items) > 1 else [fast_post(clob(), *items[0])]), False
    except Exception as e:
        resps, unsure = [{"success": False, "errorMsg": str(e)[:200]}] * len(items), True
    t2 = time.time()
    log.info(f"[LIVE] batch of {len(items)} build {t1 - tb:.2f}s post {t2 - t1:.2f}s resp: {str(resps)[:400]}")
    tim = {"build": t1 - tb, "post": t2 - t1, "sent": t1, "n": len(items)}
    for (s, rid, head, cap), r in zip(built, resps):
        t0, tf, o, tok, side, bps, fair, ask, bid, edge = s
        buy = (*check_resp(r), dict(tim), cap, unsure)
        threading.Thread(target=_run_live, args=(rid, head, tf, o, tok, side, ask, t0, fair, buy), daemon=True).start()


def _run_live(rid, head, tf, o, tok, side, ask, t0, fair, buy=None):
    try:
        live_round_trip(rid, head, tf, o, tok, side, ask, t0, fair, buy)
    finally:
        busy.discard((tf, o))


def trade(t0, tf, o, tok, side, bps, fair, ask, bid, edge):
    mode = "LIVE" if LIVE_ENABLED and tf in LIVE_TFS else "PAPER"
    rid, head, allowed = admit(t0, tf, o, tok, side, bps, fair, ask, bid, edge, mode)
    if not allowed:
        return
    if mode == "LIVE":
        try:
            live_round_trip(rid, head, tf, o, tok, side, ask, t0, fair)
        finally:
            busy.discard((tf, o))
    else:
        time.sleep(max(0.0, t0 + EDGE_DELAY + HOLD_SECS - time.time()))
        b = best(tok, "bids")
        st = LIVE_STAKE
        sh = st / (ask / 100)
        pp = None if b is None else sh * b / 100 - st - fee(sh, ask / 100) - fee(sh, b / 100)
        db_exec("UPDATE lj_trades SET status='PAPER', paper_bid=?, paper_pnl=?, pnl=? WHERE id=?", (b, pp, pp, rid))
        bs = "—" if b is None else f"{b:g}"
        tg(f"📝 {head} · {ask:g}→{bs}¢ · <b>{money(pp) if pp is not None else 'n/a'}</b>")


def held_shares(tf, o, side):
    return sum((r["buy_shares"] or 0) for r in db_query(
        "SELECT buy_shares FROM lj_trades WHERE status='HOLDING' AND tf=? AND open_ts=? AND side=?", (tf, o, side)))


def live_round_trip(rid, head, tf, o, tok, side, ask, t_jump=0.0, fair=0.0, buy=None):
    bal0 = state.get("bal")                      # cached balance (fetching it here would slow the buy down)
    unsure = False
    # BUY
    if buy is None:
        tim = {}
        cap = buy_cap(ask, fair)
        try:
            ok, resp, note = market_order(tok, "BUY", LIVE_STAKE, cap, tim)
        except Exception as e:
            ok, resp, note = False, {}, f"error: {str(e)[:150]}"
    else:
        ok, resp, note, tim, cap, unsure = buy
    lag = (f"{tim.get('sent', 0) + tim.get('post', 0) - t_jump:.2f}s"
           + (f" ×{tim['n']}" if tim.get("n", 1) > 1 else ""))
    lag_full = (f"reached Polymarket {lag} after the jump (build {tim.get('build', 0) * 1000:.0f}ms · "
                f"send {tim.get('post', 0) * 1000:.0f}ms)")
    if not ok and unsure:
        # the whole group request errored: make sure nothing filled anyway before calling it a miss
        time.sleep(max(0.0, t_jump + HOLD_SECS - time.time()))
        extra = 0.0
        for _ in range(3):
            try:
                extra = token_balance(tok) - held_shares(tf, o, side)
            except Exception:
                extra = 0.0
            if extra >= 1:
                break
            time.sleep(1)
        if extra >= 1:
            ok, note = True, "group request errored but shares arrived — cost estimated at the limit price"
            resp = {"makingAmount": str(round(extra * cap / 100, 4)), "takingAmount": str(round(extra, 4))}
    if not ok:
        db_exec("UPDATE lj_trades SET mode='MISSED', status='MISSED', note=?, bal_before=? WHERE id=?", (f"{note} | {lag_full}", bal0, rid))
        tg(f"⏭ {head} · missed, {short_why(note, cap)} · {lag}")
        return
    cost, shares = _amounts(resp)
    if cost <= 0 or shares <= 0:
        cost, shares = LIVE_STAKE, LIVE_STAKE / (ask / 100)
    bpx = cost / shares * 100
    t_buy = time.time()
    db_exec("UPDATE lj_trades SET status='BOUGHT', buy_cost=?, buy_shares=?, buy_px=?, buy_ts=?, bal_before=?, note=? WHERE id=?",
            (cost, shares, bpx, t_buy, bal0, (note + " | " if note else "") + lag_full, rid))
    # wait, then SELL (fill-or-kill with a floor; a few retries)
    time.sleep(max(0.0, (t_jump or t_buy) + HOLD_SECS - time.time()))
    paper_bid = best(tok, "bids")
    sold, proceeds, s_sh, note = False, 0.0, 0.0, ""
    for attempt in range(6):
        b = best(tok, "bids")
        if b is None:
            time.sleep(1)
            continue
        try:
            have = token_balance(tok)
        except Exception as e:
            have = shares
            note = f"balance check: {str(e)[:80]}"
        qty = math.floor(min(have, shares) * 100) / 100
        if qty <= 0:
            note = "no shares in the wallet yet"
            time.sleep(1)
            continue
        try:
            ok, resp, note = market_order(tok, "SELL", qty, max(1, b - LIVE_SELL_SLIP))
        except Exception as e:
            ok, note = False, f"error: {str(e)[:150]}"
        if ok:
            mk, tk = _amounts(resp)
            s_sh, proceeds = (mk, tk) if mk > 0 and tk > 0 else (qty, qty * b / 100)
            sold = True
            break
        time.sleep(1)
    try:
        bal1 = usdc_balance()
    except Exception:
        bal1 = None
    pp = None if paper_bid is None else (shares * paper_bid / 100 - cost - fee(shares, ask / 100) - fee(shares, paper_bid / 100))
    if sold:
        spx = proceeds / s_sh * 100 if s_sh > 0 else 0
        fees = fee(shares, bpx / 100) + fee(s_sh, spx / 100)       # Polymarket takes these from the wallet separately
        pnl = proceeds - cost - fees
        db_exec("UPDATE lj_trades SET status='SOLD', sell_proceeds=?, sell_shares=?, sell_px=?, sell_ts=?, paper_bid=?, "
                "paper_pnl=?, pnl=?, bal_after=?, result=?, note=? WHERE id=?",
                (proceeds, s_sh, spx, time.time(), paper_bid, pp, pnl, bal1, "WIN" if pnl > 0 else "LOSS",
                 lag_full + (f" | {note}" if note else ""), rid))
        tg(f"{'✅' if pnl > 0 else '❌'} {head} · {bpx:.0f}→{spx:.0f}¢ · <b>{money(pnl)}</b>"
           f" (paper {money(pp) if pp is not None else 'n/a'}) · {lag}")
    else:
        db_exec("UPDATE lj_trades SET status='HOLDING', paper_bid=?, paper_pnl=?, bal_after=?, note=? WHERE id=?",
                (paper_bid, pp, bal1, lag_full + (f" | {note}" if note else ""), rid))
        tg(f"⏳ {head} · bought {bpx:.0f}¢, couldn't sell · holding to the end")


def keep_warm():
    """Touch Polymarket every 3s over the same connection orders use, so it never goes idle and has to reconnect
    when a jump comes (the library drops idle connections after 5s)."""
    while True:
        try:
            if LIVE_ENABLED or D_LIVE:
                clob().get_server_time()
        except Exception as e:
            log.debug(f"keep_warm: {e}")
        time.sleep(3)


def balance_worker():
    """keep a recent USDC balance on hand without slowing down orders"""
    while True:
        try:
            if (LIVE_ENABLED or D_LIVE) and not busy:
                state["bal"] = usdc_balance()
        except Exception as e:
            log.warning(f"balance: {e}")
        time.sleep(20)


def settler():
    while True:
        try:
            now = time.time()
            for r in db_query("SELECT * FROM lj_trades WHERE status='HOLDING' AND mode NOT IN ('D','DPAPER') AND open_ts + tf*60 < ?", (now - 90,)):
                out = outcome(r["tf"], r["open_ts"])
                if not out:
                    continue
                won = out == r["side"]
                pnl = (r["buy_shares"] if won else 0) - r["buy_cost"] - fee(r["buy_shares"], (r["buy_px"] or 50) / 100)
                db_exec("UPDATE lj_trades SET status='SETTLED', pnl=?, result=? WHERE id=?", (pnl, "WIN" if won else "LOSS", r["id"]))
                tg(f"{'✅' if won else '❌'} held {r['tf']}m {r['side']} settled {out} · <b>{money(pnl)}</b>")
        except Exception as e:
            log.warning(f"settler: {e}")
        time.sleep(30)


# ── D rules: buy after a BTC move when Polymarket lags, HOLD TO THE END (added Nov 2 2026) ──────────────
# Exactly the paper rules from the early logger (same trigger: BTC moves >= 3bp within 2s on vis/spot/fut,
# 10s cooldown per direction; same snapshot; first qualifying move per market per rule):
#   base   : buy the side the move points to if Deribit-vol fair (fair_iv) - ask >= D_EDGE (10c) and ask 15-95c
#   5m  AV : base + ask didn't rise > 2c within 0.5s (A) + Deribit vol <= 1.5x BTC's recent vol (V)
#   5m  AVS: AV + at least 60 shares at the best ask at the move
#   5m  AVK: AV + BTC kept moving our way by >= 0.5bp in the 0.5s after the move (added Nov 2, fair test from 11:50am)
#   5m  LAK: fair from the LOWER of Deribit vol and our vol, + A + K (>= 0.5bp), no vol-ratio filter (added Nov 2, fair test from 1:40pm)
#   15m C  : base + our own fair (1-second BTC vol) also says >= 5c cheap
#   15m K  : base + BTC kept moving our way in the 0.5s after the move
#   15m X  : base + A + V          15m R : base + ask didn't rise > 1c within 0.25s        15m D15: base only
#   15m CKS: C + K + at least 60 shares at the best ask (added Nov 2, fair test from 10:05am)
# Order: fill-and-kill BUY of D_STAKE dollars, limit = ask at decision + D_OVERPAY_CENTS (never above 97c).
# OFF by default: D_LIVE=false -> paper (same decisions, buys priced from the book, settled at the end).
# Safety: daily loss limit counts open positions as lost; max trades per hour; one trade per market per rule;
#         /dstop pauses instantly, /dgo resumes.
# ENV: D_TRIGGER_BP_5=3 / D_TRIGGER_BP_15=3 (set 2 to also act on 2bp moves)  D_LIVE=false  D_RULE_5=AVS (A/AV/AVS/AVK/LAK/off)  D_RULE_15=C (C/K/X/R/D15/CKS/off)  D_STAKE=5  D_OVERPAY_CENTS=3
#      D_DAILY_LOSS=25  D_MAX_TRADES_HOUR=10  D_EDGE=10
D_ON = os.environ.get("D_RULES", "true").strip().lower() in ("1", "true", "yes", "on")
D_LIVE = os.environ.get("D_LIVE", "false").strip().lower() == "true"
D_RULE = {5: os.environ.get("D_RULE_5", "AVS").strip().upper(), 15: os.environ.get("D_RULE_15", "C").strip().upper()}
D_STAKE = env_f("D_STAKE", "5")
D_OVERPAY = env_f("D_OVERPAY_CENTS", "3")
D_DAILY_LOSS = env_f("D_DAILY_LOSS", "25")
D_MAX_HOUR = int(env_f("D_MAX_TRADES_HOUR", "10"))
D_EDGE = env_f("D_EDGE", "10")
D_RULES_OK = {5: ("A", "AV", "AVS", "AVK", "LAK"), 15: ("C", "K", "X", "R", "D15", "CKS")}
d_lock = threading.Lock()
d_done = set()                      # (tf, open_ts) already traded (or tried) by the D rule


def d_paused():
    return kv_get("d_paused", "0") == "1"


def d_name(tf):
    r = D_RULE.get(tf, "OFF")
    return ("D5-" + r) if tf == 5 else ("D-15" if r == "D15" else "D-15" + r)


def _ask_at(tok):
    a, _ = _ladder(tok)
    a = [l for l in a if l[0] < 100]
    return a


def _walk(lad, stake):
    spent = sh = 0.0
    for px, sz in lad:
        c = px / 100 * sz
        if spent + c >= stake:
            sh += (stake - spent) / (px / 100)
            spent = stake
            break
        spent += c
        sh += sz
    return spent, sh


def d_risk(stake):
    if d_paused():
        return "paused (/dgo to resume)"
    t_day = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    rows = db_query("SELECT pnl, buy_cost, status, t0 FROM lj_trades WHERE mode='D' AND t0>=?", (t_day,))
    realized = sum(r["pnl"] or 0 for r in rows if r["pnl"] is not None)
    open_cost = sum(r["buy_cost"] or 0 for r in rows if r["pnl"] is None and r["status"] == "HOLDING")
    if realized - open_cost - stake < -D_DAILY_LOSS - 1e-9:
        return f"daily loss limit ${D_DAILY_LOSS:g} (today {money(realized)}, open ${open_cost:.0f})"
    if sum(1 for r in rows if r["t0"] >= time.time() - 3600 and r["status"] in ("HOLDING", "SETTLED")) >= D_MAX_HOUR:
        return f"max {D_MAX_HOUR} trades per hour"
    return None


def d_signal(t0, feed, tf, o, tok, up, ask0, size0, fair, fair_iv, sig, sig_iv, px0):
    """one market, one move: check the rule exactly like the paper tracker, then buy (live) or log (paper)"""
    try:
        rule = D_RULE.get(tf, "OFF")
        if rule not in D_RULES_OK.get(tf, ()):
            return
        if (tf, o) in d_done or fair_iv is None:
            return
        fv = fair_iv
        if rule == "LAK":                                 # fair from the LOWER of Deribit vol and BTC's own recent vol
            if fair is None or not sig or not sig_iv:
                return
            fv = fair if sig < sig_iv else fair_iv
        if not (fv - ask0 >= D_EDGE and 15 <= ask0 <= 95):
            return
        if rule in ("C", "CKS") and (fair is None or fair - ask0 < 5):
            return
        if rule in ("AVS", "CKS") and size0 < 60:
            return
        if rule in ("AV", "AVS", "AVK", "X") and (not sig or not sig_iv or sig_iv / sig > 1.5):
            return
        if rule == "R":
            time.sleep(max(0.0, t0 + 0.25 - time.time()))
            a = _ask_at(tok)
            if not a or a[0][0] - ask0 > 1:
                return
        if rule in ("A", "AV", "AVS", "AVK", "LAK", "X", "K", "CKS"):     # A (5m, Nov 3) = only the ask-jump check, no vol filter
            time.sleep(max(0.0, t0 + 0.5 - time.time()))
            if rule in ("K", "CKS"):
                with flock:
                    p05 = feeds["vis"][-1][1] if feeds["vis"] else None
                if not px0 or not p05 or (p05 - px0) * (1 if up else -1) <= 0:
                    return
            else:
                a = _ask_at(tok)
                if not a or a[0][0] - ask0 > 2:
                    return
                if rule in ("AVK", "LAK"):               # BTC kept moving our way by >= 0.5bp in the 0.5s after the move
                    with flock:
                        p05 = feeds["vis"][-1][1] if feeds["vis"] else None
                    if not px0 or not p05 or (p05 - px0) * (1 if up else -1) / px0 * 1e4 < 0.5:
                        return
        side = "UP" if up else "DOWN"
        name = d_name(tf)
        with d_lock:
            if (tf, o) in d_done:
                return
            d_done.add((tf, o))
            why = d_risk(D_STAKE) if D_LIVE else None
        a = _ask_at(tok)
        cur = a[0][0] if a else ask0
        cap = min(97.0, cur + D_OVERPAY)
        head = f"{name} {side} · ask {ask0:g}¢ fair {fv:.0f}¢ (+{fv - ask0:.0f})"
        mode = "D" if D_LIVE else "DPAPER"
        rid = db_exec("INSERT INTO lj_trades (t0,tf,open_ts,side,bps,fair,ask,edge,mode,status,fair_iv,sig,sig_iv,note) "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      (t0, tf, o, side, None, fair, ask0, fv - ask0, mode, "SIGNAL", fair_iv, sig, sig_iv,
                       f"{name} · feed {feed}"))
        if not D_LIVE:                                        # paper: price it from the book right now
            spent, sh = _walk(a, D_STAKE)
            if spent <= 0:
                db_exec("UPDATE lj_trades SET status='MISSED', note=note||' · no ask' WHERE id=?", (rid,))
                return
            db_exec("UPDATE lj_trades SET status='HOLDING', buy_cost=?, buy_shares=?, buy_px=?, buy_ts=? WHERE id=?",
                    (spent, sh, spent / sh * 100, time.time(), rid))
            tg(f"📝 {head} · paper buy {spent / sh * 100:.0f}¢ · {time.time() - t0:.2f}s")
            return
        if why:
            db_exec("UPDATE lj_trades SET status='BLOCKED', note=note||' · '||? WHERE id=?", (why, rid))
            tg(f"⛔ {head} · skipped: {why}")
            return
        tim = {}
        try:
            ok, resp, note = market_order(tok, "BUY", D_STAKE, cap, tim)
        except Exception as e:
            ok, resp, note = False, {}, f"error: {str(e)[:150]}"
        lag = tim.get("sent", t0) + tim.get("post", 0) - t0
        if not ok:
            db_exec("UPDATE lj_trades SET status='MISSED', note=note||' · '||? WHERE id=?", (f"{note} · {lag:.2f}s", rid))
            tg(f"⏭ {head} · not filled ({short_why(note, cap)}) · {lag:.2f}s")
            return
        cost, shares = _amounts(resp)
        if cost <= 0 or shares <= 0:
            cost, shares = D_STAKE, D_STAKE / (cap / 100)
        bpx = cost / shares * 100
        db_exec("UPDATE lj_trades SET status='HOLDING', buy_cost=?, buy_shares=?, buy_px=?, buy_ts=?, note=note||' · '||? WHERE id=?",
                (cost, shares, bpx, time.time(), f"reached Polymarket {lag:.2f}s after the move", rid))
        tg(f"🎯 {head} · bought ${cost:.2f} at {bpx:.0f}¢ · {lag:.2f}s · holding to the end")
    except Exception as e:
        log.warning(f"D signal: {e}")


def d_settler():
    for r in db_query("SELECT tf, open_ts FROM lj_trades WHERE mode IN ('D','DPAPER') AND t0 > ?", (time.time() - 3600,)):
        d_done.add((r["tf"], r["open_ts"]))                  # after a restart, don't trade the same market twice
    while True:
        try:
            for k in [k for k in list(d_done) if k[1] + k[0] * 60 < time.time() - 600]:
                d_done.discard(k)
            for r in db_query("SELECT * FROM lj_trades WHERE mode IN ('D','DPAPER') AND status='HOLDING' AND open_ts + tf*60 < ?",
                              (time.time() - 90,)):
                out = outcome(r["tf"], r["open_ts"])
                if not out:
                    continue
                won = out == r["side"]
                p = (r["buy_px"] or 50) / 100
                fee_ = math.ceil(r["buy_shares"] * FEE_RATE * p * (1 - p) * 100 - 1e-9) / 100
                pnl = (r["buy_shares"] if won else 0) - r["buy_cost"] - fee_
                db_exec("UPDATE lj_trades SET status='SETTLED', pnl=?, result=? WHERE id=?", (pnl, "WIN" if won else "LOSS", r["id"]))
                tag = "" if r["mode"] == "D" else "📝 paper · "
                nm = (r["note"] or "").split(" · ")[0]
                tg(f"{'✅' if won else '❌'} {tag}{nm} {r['side']} at {r['buy_px']:.0f}¢ · result {out} · <b>{money(pnl)}</b>")
        except Exception as e:
            log.warning(f"D settler: {e}")
        time.sleep(30)


def d_summary(since, label):
    rows = db_query("SELECT * FROM lj_trades WHERE mode IN ('D','DPAPER') AND t0>=? ORDER BY t0", (since,))
    L = [f"<b>D rules · {label}</b>"]
    for mode, tag in (("D", "💵 live"), ("DPAPER", "📝 paper")):
        for tf in (5, 15):
            rs = [r for r in rows if r["mode"] == mode and r["tf"] == tf]
            if not rs:
                continue
            done = [r for r in rs if r["status"] == "SETTLED"]
            w = sum(1 for r in done if r["result"] == "WIN")
            L.append(f"{tag} {tf}m: {len(done)} settled · {w} wins · <b>{money(sum(r['pnl'] or 0 for r in done))}</b> · "
                     f"holding {sum(1 for r in rs if r['status'] == 'HOLDING')} · not filled {sum(1 for r in rs if r['status'] == 'MISSED')} · "
                     f"blocked {sum(1 for r in rs if r['status'] == 'BLOCKED')}")
    if len(L) == 1:
        L.append("no D trades yet")
    return "\n".join(L)


def d_status():
    t_day = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    st = "🟢 LIVE" if D_LIVE else "📝 PAPER (D_LIVE=false)"
    return (f"🎯 <b>D rules</b> · {st}{' · ⏸ PAUSED' if d_paused() else ''}\n"
            f"5m: {d_name(5) if D_RULE[5] in D_RULES_OK[5] else 'off'} · 15m: {d_name(15) if D_RULE[15] in D_RULES_OK[15] else 'off'} · "
            f"${D_STAKE:g}/trade · buy limit ask+{D_OVERPAY:g}¢ · edge ≥{D_EDGE:g}¢ · hold to the end\n"
            f"trigger: 5m {D_TRIG_BP[5] or 3:g}bp · 15m {D_TRIG_BP[15] or 3:g}bp\n"
            f"limits: ${D_DAILY_LOSS:g}/day (open trades count as lost) · {D_MAX_HOUR}/hour · one trade per market\n"
            + d_summary(t_day, "today"))


# ── reports ─────────────────────────────────────────────────────────────────
def summary(since, label):
    rows = db_query("SELECT * FROM lj_trades WHERE t0>=? ORDER BY t0", (since,))
    live = [r for r in rows if r["mode"] == "LIVE" and r["pnl"] is not None]
    paper = [r for r in rows if r["mode"] == "PAPER" and r["pnl"] is not None]
    missed = sum(1 for r in rows if r["mode"] == "MISSED")
    blocked = sum(1 for r in rows if r["mode"] == "BLOCKED")
    L = [f"<b>{label}</b>"]
    if live:
        w = sum(1 for r in live if r["pnl"] > 0)
        pp = sum(r["paper_pnl"] or 0 for r in live)
        L.append(f"💵 live: {len(live)} trades · {w} wins · <b>{money(sum(r['pnl'] for r in live))}</b> "
                 f"(paper on the same trades {money(pp)})")
        L.append(f"   avg buy slip {sum((r['buy_px'] or r['ask']) - r['ask'] for r in live) / len(live):+.1f}¢ · "
                 f"holding {sum(1 for r in rows if r['status'] == 'HOLDING')}")
    if paper:
        L.append(f"📝 paper: {len(paper)} trades · {sum(1 for r in paper if r['pnl'] > 0)} wins · "
                 f"<b>{money(sum(r['pnl'] for r in paper))}</b>")
    L.append(f"not filled {missed} · blocked {blocked} · signals {len(rows)}")
    return "\n".join(L)


def status_text():
    t_day = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    with lock:
        nm = len(markets)
    L = [f"⚡ <b>LIVE BTC JUMP SNIPER</b> · {'🟢 LIVE' if LIVE_ENABLED else '📝 PAPER (LIVE_ENABLED=false)'}"
         f"{' · ⏸ PAUSED' if paused() else ''}",
         f"rule: jump ≥{JUMP_BPS:g}bp/{JUMP_WINDOW:g}s · edge ≥{MIN_EDGE:g}¢ · ask &lt;{MAX_ASK:g}¢ · under {CHEAP_CENTS:g}¢ needs "
         f"edge ≥{CHEAP_EDGE:g}¢ · ${LIVE_STAKE:g} · sell after {HOLD_SECS:g}s",
         f"limits: ${LIVE_DAILY_LOSS:g}/day · {LIVE_MAX_TRADES_HOUR}/hour · buy ≤ min(ask+{SMART_MAX:g}¢, fair−{KEEP_EDGE:g}¢) · "
         f"sell ≥bid−{LIVE_SELL_SLIP:g}¢ · decide after {EDGE_DELAY:g}s",
         f"up {(time.time() - state['started']) / 3600:.1f}h · BTC {price['px'] or 0:,.0f} · markets {nm} · jumps {state['jumps']} · "
         f"signals {state['signals']} · σ {sigma() * 1e5:.1f}e-5/s · options σ "
         f"{(sigma_iv() or 0) * 1e5:.1f}e-5/s · closes logged {early_state.get('closes', 0)} · early log {early_state['n']} (feeds {early_state['ticks']})"
         + (f" · chainlink: {early_state['cl_err'][:60]}" if early_state["cl_err"] else "") + (f" · last error: {state['err'][:80]}" if state["err"] else ""),
         summary(t_day, "today")]
    return "\n".join(L)


def trades_text(n=12):
    rows = db_query("SELECT * FROM lj_trades ORDER BY t0 DESC LIMIT ?", (n,))
    if not rows:
        return "no signals yet"
    L = ["<b>last signals</b>"]
    for r in rows:
        t = datetime.fromtimestamp(r["t0"], TZ).strftime("%a %H:%M:%S")
        p = r["pnl"]
        L.append(f"{t} {r['tf']}m {r['side']} ask {r['ask']:g}¢ edge {r['edge']:+.0f} · {r['mode']} {r['status']} "
                 f"{money(p) if p is not None else ''} {('(paper ' + money(r['paper_pnl']) + ')') if r['mode'] == 'LIVE' and r['paper_pnl'] is not None else ''}")
    return "\n".join(L)


def telegram_worker():
    off = 0
    while True:
        try:
            r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                             params={"offset": off, "timeout": 30}, timeout=40).json()
            for u in r.get("result", []):
                off = u["update_id"] + 1
                msg = u.get("message") or {}
                if str(msg.get("chat", {}).get("id")) != str(TELEGRAM_CHAT_ID):
                    continue
                t = (msg.get("text") or "").strip().split()[0].lower() if msg.get("text") else ""
                if E2_ON:                                   # second early logger (paper only)
                    try:
                        parts = (msg.get("text") or "").strip().split()
                        if early2.handle(t, parts[1].lower() if len(parts) > 1 else "", tg):
                            continue
                    except Exception as e:
                        tg(f"⚠️ early2: {str(e)[:200]}")
                        continue
                if t == "/dstatus":
                    tg(d_status())
                    continue
                if t == "/dstop":
                    kv_set("d_paused", "1")
                    tg("⏸ D rules paused — no new D trades. Open ones still settle. /dgo to resume.")
                    continue
                if t == "/dgo":
                    kv_set("d_paused", "0")
                    tg("▶️ D rules resumed.")
                    continue
                if t == "/dall":
                    tg(d_summary(0, "all time"))
                    continue
                if t == "/status":
                    tg(status_text())
                elif t == "/trades":
                    tg(trades_text())
                elif t == "/today":
                    tg(summary(datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0).timestamp(), "today"))
                elif t == "/all":
                    tg(summary(0, "all time"))
                elif t == "/balance":
                    try:
                        tg(f"💰 USDC balance: <b>${usdc_balance():,.2f}</b>")
                    except Exception as e:
                        tg(f"⚠️ balance failed: {str(e)[:200]}")
                elif t == "/ping":
                    ms = []
                    for _ in range(5):
                        a_ = time.time()
                        try:
                            requests.get(f"{CLOB_BASE}/time", timeout=5)
                            ms.append((time.time() - a_) * 1000)
                        except Exception:
                            pass
                    mo = []
                    for _ in range(5):
                        a_ = time.time()
                        try:
                            clob().get_server_time()
                            mo.append((time.time() - a_) * 1000)
                        except Exception:
                            pass
                    med = lambda v: f"{sorted(v)[len(v) // 2]:.0f}ms" if v else "failed"
                    tg(f"🏓 simple: {med(ms)} · order line: {med(mo)} ({', '.join(f'{x:.0f}' for x in mo)})")
                elif t == "/stop":
                    kv_set("paused", "1")
                    tg("⏸ paused — no new live trades. /go to resume.")
                elif t == "/go":
                    kv_set("paused", "0")
                    tg("▶️ resumed.")
                elif t == "/earlyexport":
                    rows = db_query("SELECT t0, feed, data FROM lj_early ORDER BY t0")
                    if rows:
                        buf = io.StringIO()
                        w = csv.writer(buf)
                        w.writerow(["t0", "feed", "data"])
                        w.writerows([list(x) for x in rows])
                        tg_document("early.csv.gz", gzip.compress(buf.getvalue().encode()), f"{len(rows)} early moves")
                    else:
                        tg(f"no early moves logged yet (feed ticks: {early_state['ticks']})")
                elif t == "/extraexport":          # Nov 4 extra recorder: rows since the last /extraexport ('all' = everything)
                    parts = (msg.get("text") or "").strip().split()
                    since = 0 if (len(parts) > 1 and parts[1].lower() == "all") else int(kv_get("extra_exported", 0) or 0)
                    rows = db_query("SELECT id, ts, kind, data FROM lj_extra WHERE id > ? ORDER BY id", (since,))
                    if rows:
                        buf = io.StringIO()
                        w = csv.writer(buf)
                        w.writerow(["id", "ts", "kind", "data"])
                        w.writerows([list(x) for x in rows])
                        kinds = {}
                        for x in rows:
                            kinds[x[2]] = kinds.get(x[2], 0) + 1
                        tg_document("extra.csv.gz", gzip.compress(buf.getvalue().encode()), f"{len(rows)} extra rows " + " ".join(f"{k}:{n}" for k, n in sorted(kinds.items())))
                        kv_set("extra_exported", rows[-1][0])
                    else:
                        tg(f"no new extra rows {('· ' + extra_state['err']) if extra_state['err'] else ''}")
                elif t == "/extrastatus":
                    ag = lambda q: f"{time.time() - q[-1][0]:.0f}s" if q else "none"
                    tg("extra recorder: " + ("on" if EXTRA_LOG else "off") + f"\nrows saved: window {extra_state['win']} · PM trades {extra_state['pmt']} · market info {extra_state['meta']} · Binance stats {extra_state['bn']}"
                       + f"\nPM trades seen: {len(pm_trades)} · ETH last {ag(alt_px['ETH'])} ago · SOL {ag(alt_px['SOL'])}"
                       + f"\nTWAP feed: {twap_state['n']} points, last {ag(twap_hist)} ago, windows with exact start {len(tw_opens)}" + (f" · {twap_state['err'][:60]}" if twap_state["err"] else "")
                       + "\nlead feeds (last tick ago): " + " · ".join(f"{v} {ag(q)}" for v, q in lead_px.items()) + f" · coinbase {ag(flow_cb)}"
                       + (f"\nlast error: {extra_state['err'][:80]}" if extra_state["err"] else "") + (f"\nlead error: {lead_state['err'][:80]}" if lead_state["err"] else ""))
                elif t == "/deribitexport":
                    rows = db_query("SELECT ts, data FROM lj_deribit ORDER BY ts")
                    if rows:
                        buf = io.StringIO()
                        w = csv.writer(buf)
                        w.writerow(["ts", "data"])
                        w.writerows([list(x) for x in rows])
                        tg_document("deribit.csv.gz", gzip.compress(buf.getvalue().encode()), f"{len(rows)} Deribit snapshots (1/min)")
                    else:
                        tg(f"no Deribit snapshots yet {('· ' + dlog_state['err']) if dlog_state['err'] else ''}")
                elif t == "/closeexport":
                    rows = db_query("SELECT close_ts, tf, open_ts, outcome, data FROM lj_close ORDER BY close_ts")
                    if rows:
                        buf = io.StringIO()
                        w = csv.writer(buf)
                        w.writerow(["close_ts", "tf", "open_ts", "outcome", "data"])
                        w.writerows([list(x) for x in rows])
                        tg_document("closes.csv.gz", gzip.compress(buf.getvalue().encode()), f"{len(rows)} window closes")
                    else:
                        tg("no closes logged yet")
                elif t == "/export":
                    rows = db_query("SELECT * FROM lj_trades ORDER BY t0")
                    if rows:
                        buf = io.StringIO()
                        w = csv.writer(buf)
                        w.writerow(rows[0].keys())
                        w.writerows([list(x) for x in rows])
                        tg_document("live_jumper.csv.gz", gzip.compress(buf.getvalue().encode()), f"{len(rows)} signals")
                    else:
                        tg("no signals yet")
                elif t in ("/help", "/start"):
                    tg("⚡ live BTC jump sniper\n/status — settings + today\n/trades — last signals\n/today · /all — totals\n"
                       "/balance — USDC in the account\n/ping — network speed to Polymarket\n/stop — pause live trading · /go — resume\n/export — all signals (CSV)\n/earlyexport — early-move log (paper research)\n/closeexport — last 10s of every window (paper research)\n/ethexport — ETH early-move log, new rows (add 'all' for everything)\n/early2export — BTC+ETH 2bp/3bp early log, new rows\n/early2status — early2 logger health\n/deribitexport — Deribit skew/same-day vol/perp/trades log (1/min)\n/extraexport — extra recorder: window books, PM trades, market info, Binance stats, new rows (add 'all' for everything)\n/extrastatus — extra + lead recorder health\n/dstatus — D rules (hold-to-end) settings + today\n/dstop · /dgo — pause / resume D rules\n/dall — D rules all-time")
        except Exception as e:
            log.warning(f"telegram: {e}")
            time.sleep(5)


# ── early2: SECOND early-move logger (paper research only, NEVER trades) ─────────────────────────
# 
# The original early logger in bot.py (BTC only, 3bp trigger, /earlyexport) is NOT touched, so all running
# fair tests stay exactly the same. This file runs next to it and logs:
# - BTC and ETH 5m/15m up/down markets
# - triggers at 2bp AND at 3bp (a move of >= level within 2s on any Binance feed: vis / spot / fut),
# each level with its own 10s cooldown per direction, so the 3bp rows match the original rule.
# - to keep the file small it only saves a trigger when, for 5m or 15m, the side the move points to is
# at least 5c cheap vs Deribit fair (or our fair if Deribit vol is missing). The D rules need 10c, so no
# possible D trade is lost.
# Fully self-contained: its own Binance feeds, Polymarket books, market list, window open prices, Deribit vol
# and market results. It shares only the database file and the Telegram bot with bot.py.
# 
# Telegram: /ethexport          -> ETH only, rows since the last /ethexport (eth.csv.gz + eth_mkts.csv.gz)
# /early2export        -> BTC+ETH, rows since the last /early2export (early2.csv.gz + early2_mkts.csv.gz)
# add 'all' (e.g. /ethexport all) -> everything
# /early2status        -> counts / feed health
def _make_early2():
    """builds the early2 logger in its own namespace, so none of its names clash with the bot's"""
    import time, json, math, threading, logging, requests, csv, io, gzip
    from collections import deque
    import websocket

    log = logging.getLogger("early2")
    ASSETS = {"BTC": "btcusdt", "ETH": "ethusdt"}
    TFS = (5, 15)
    LEVELS = (2.0, 3.0)
    WIN, COOL, MIN_GAP = 2.0, 10.0, 5.0
    EXPORT_LAG = 30.0     # rows still being captured (10s of snapshots) are left for the next export
    SNAPS = (0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0)
    _streams = "/".join(f"{s}@bookTicker" for s in ASSETS.values())
    FEEDS = {"vis": f"wss://data-stream.binance.vision/stream?streams={_streams}",
             "spot": f"wss://stream.binance.com:9443/stream?streams={_streams}",
             "fut": f"wss://fstream.binance.com/stream?streams={_streams}"}
    SYM = {v.upper(): k for k, v in ASSETS.items()}
    GAMMA = "https://gamma-api.polymarket.com"
    MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    KLINES = "https://data-api.binance.vision/api/v3/klines"
    DERIBIT = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency"

    lock = threading.RLock()
    books, markets, opens = {}, {}, {}        # tok -> {bids, asks}; (asset, tf, o) -> (up, dn); (asset, tf, o) -> open px
    q2 = {(a, f): deque(maxlen=3000) for a in ASSETS for f in FEEDS}   # last 2s of mids per asset/feed
    px1s = {a: deque(maxlen=1200) for a in ASSETS}                      # ~1/s vis mids, for our vol
    last_px = {(a, f): None for a in ASSETS for f in FEEDS}
    last_trig = {}                                                       # (asset, level, sign) -> t
    tlock = threading.Lock()
    ivs = {a: {"iv": None, "ts": 0.0} for a in ASSETS}
    st = {"ticks": {f: 0 for f in FEEDS}, "pm": 0, "trig": 0, "saved": 0, "err": ""}
    H = {}                                                               # db_exec, db_query, token, chat


    def norm_cdf(z):
        return 0.5 * (1 + math.erf(z / math.sqrt(2)))


    def window(tf, now):
        L = tf * 60
        o = int(now // L) * L
        return o, o + L


    def slug(a, tf, o):
        return f"{a.lower()}-updown-{tf}m-{o}"


    def gamma_market(a, tf, o):
        arr = requests.get(f"{GAMMA}/events", params={"slug": slug(a, tf, o)}, timeout=10).json()
        mk = (arr[0] if isinstance(arr, list) and arr else {}).get("markets", [])
        return mk[0] if mk else None


    # ── vol ─────────────────────────────────────────────────────────────────────
    def sigma(a):
        pts = [p for p in list(px1s[a]) if p[0] >= time.time() - 900]
        r = [(math.log(y / x)) ** 2 / (t1 - t0) for (t0, x), (t1, y) in zip(pts, pts[1:]) if t1 > t0 and x > 0 and y > 0]
        if len(r) < 30:
            return 6e-5
        return max(2e-5, math.sqrt(sum(r) / len(r)))


    def sigma_iv(a):
        v = ivs[a]
        if not v["iv"] or time.time() - v["ts"] > 600:
            return None
        return v["iv"] / math.sqrt(365 * 86400)


    def fair_up(a, tf, o, now, px, s):
        op = opens.get((a, tf, o))
        left = o + tf * 60 - now
        if not s or not op or not px or left <= 0:
            return None
        return 100.0 * norm_cdf(math.log(px / op) / (s * math.sqrt(left)))


    def deribit_iv(cur):
        from datetime import datetime, timezone
        r = requests.get(DERIBIT, params={"currency": cur, "kind": "option"}, timeout=15).json()
        now, by = time.time(), {}
        for x in r.get("result", []):
            p = (x.get("instrument_name") or "").split("-")
            if len(p) != 4 or not x.get("mark_iv"):
                continue
            try:
                e = datetime.strptime(p[1], "%d%b%y").replace(hour=8, tzinfo=timezone.utc).timestamp()
                K, F = float(p[2]), float(x.get("underlying_price") or 0)
            except Exception:
                continue
            if e - now < 7200 or F <= 0:
                continue
            by.setdefault(e, []).append((abs(K / F - 1), float(x["mark_iv"]) / 100.0))
        if not by:
            return None
        allp = sorted(by[min(by)])
        pts = [x for x in allp if x[0] <= 0.03][:4] or allp[:2]
        return sum(v for _, v in pts) / len(pts)


    def deribit_worker():
        while True:
            for a in ASSETS:
                try:
                    iv = deribit_iv(a)
                    if iv:
                        ivs[a].update(iv=iv, ts=time.time())
                except Exception as e:
                    st["err"] = f"deribit {a}: {str(e)[:80]}"
            time.sleep(60)


    # ── markets / books ─────────────────────────────────────────────────────────
    def kline_open(a, o):
        try:
            j = requests.get(KLINES, params=dict(symbol=ASSETS[a].upper(), interval="1m", startTime=o * 1000,
                                                 endTime=o * 1000 + 59000, limit=1), timeout=8).json()
            if isinstance(j, list) and j:
                return float(j[0][1])
        except Exception:
            pass
        return None


    def market_worker():
        while True:
            try:
                now = time.time()
                for a in ASSETS:
                    for tf in TFS:
                        cur = window(tf, now)[0]
                        if (a, tf, cur) not in opens and now >= cur + 60:
                            op = kline_open(a, cur)
                            if op:
                                opens[(a, tf, cur)] = op
                        for o in (cur, window(tf, now + 90)[0]):
                            if (a, tf, o) in markets:
                                continue
                            try:
                                m = gamma_market(a, tf, o)
                                t = json.loads(m.get("clobTokenIds", "[]")) if m else []
                            except Exception:
                                t = []
                            if len(t) == 2:
                                with lock:
                                    markets[(a, tf, o)] = (str(t[0]), str(t[1]))
                                H["db_exec"]("INSERT OR IGNORE INTO e2_mkts (asset, tf, open_ts) VALUES (?,?,?)", (a, tf, o))
                        k = (a, tf, cur)
                        if k in opens:
                            H["db_exec"]("UPDATE e2_mkts SET open=? WHERE asset=? AND tf=? AND open_ts=? AND open IS NULL",
                                         (opens[k], a, tf, cur))
                with lock:
                    for k in [k for k in markets if k[2] + k[1] * 60 < now - 600]:
                        for t in markets.pop(k):
                            books.pop(t, None)
                for k in [k for k in opens if k[2] + k[1] * 60 < now - 3600]:
                    opens.pop(k, None)
            except Exception as e:
                st["err"] = f"markets: {str(e)[:80]}"
            time.sleep(3)


    def pm_handle(raw):
        try:
            data = json.loads(raw)
        except Exception:
            return
        for m in (data if isinstance(data, list) else [data]):
            if not isinstance(m, dict):
                continue
            st["pm"] += 1
            et = m.get("event_type")
            with lock:
                if et == "book":
                    tok = str(m.get("asset_id"))
                    books[tok] = {"bids": {round(float(x["price"]) * 100, 3): float(x["size"]) for x in (m.get("bids") or m.get("buys") or [])},
                                  "asks": {round(float(x["price"]) * 100, 3): float(x["size"]) for x in (m.get("asks") or m.get("sells") or [])}}
                elif et == "price_change":
                    for ch in (m.get("price_changes") or m.get("changes") or []):
                        tok = str(ch.get("asset_id") or m.get("asset_id"))
                        b = books.setdefault(tok, {"bids": {}, "asks": {}})
                        lvl = b["bids"] if str(ch.get("side", "")).upper() == "BUY" else b["asks"]
                        px, sz = round(float(ch["price"]) * 100, 3), float(ch.get("size", 0))
                        if sz <= 0:
                            lvl.pop(px, None)
                        else:
                            lvl[px] = sz


    def pm_worker():
        while True:
            try:
                with lock:
                    toks = tuple(sorted(t for v in markets.values() for t in v))
                if not toks:
                    time.sleep(2)
                    continue
                stop = threading.Event()

                def on_open(ws):
                    ws.send(json.dumps({"assets_ids": list(toks), "type": "market"}))

                    def pinger():
                        while not stop.is_set():
                            time.sleep(10)
                            try:
                                ws.send("PING")
                            except Exception:
                                return
                    threading.Thread(target=pinger, daemon=True).start()

                def on_message(ws, msg):
                    if msg in ("PONG", "pong"):
                        return
                    pm_handle(msg)
                    with lock:
                        cur = tuple(sorted(t for v in markets.values() for t in v))
                    if cur != toks:
                        ws.close()

                ws = websocket.WebSocketApp(MARKET_WS, on_open=on_open, on_message=on_message)
                ws.run_forever(ping_interval=20, ping_timeout=10)
                stop.set()
            except Exception as e:
                st["err"] = f"pm ws: {str(e)[:80]}"
            time.sleep(1)


    def ladder(tok, n=6):
        with lock:
            b = books.get(tok) or {}
            a = sorted((b.get("asks") or {}).items())[:n]
            bi = sorted((b.get("bids") or {}).items(), reverse=True)[:n]
        return [[p, round(s, 1)] for p, s in a], [[p, round(s, 1)] for p, s in bi]


    # ── triggers ────────────────────────────────────────────────────────────────
    def tick(a, feed, now, px):
        q = q2[(a, feed)]
        q.append((now, px))
        last_px[(a, feed)] = px
        while q and q[0][0] < now - WIN:
            q.popleft()
        if feed == "vis":
            p1 = px1s[a]
            if not p1 or now - p1[-1][0] >= 1.0:
                p1.append((now, px))
            for tf in TFS:
                o = window(tf, now)[0]
                if (a, tf, o) not in opens and now - o <= 5:
                    opens[(a, tf, o)] = px
        bps = (px / q[0][1] - 1) * 1e4
        sg = 1 if bps > 0 else -1
        for lvl in LEVELS:
            if abs(bps) < lvl:
                continue
            k = (a, lvl, sg)
            with tlock:
                if now - last_trig.get(k, 0) < COOL:
                    continue
                last_trig[k] = now
            st["trig"] += 1
            threading.Thread(target=capture, args=(now, a, feed, lvl, bps), daemon=True).start()


    def capture(t0, a, feed, lvl, bps):
        try:
            up = bps > 0
            with lock:
                live = [(k, v) for k, v in markets.items() if k[0] == a and k[2] <= t0 < k[2] + k[1] * 60]
            px = last_px[(a, "vis")] or last_px[(a, feed)]
            s, siv = sigma(a), sigma_iv(a)
            rec = {"t0": t0, "asset": a, "feed": feed, "level": lvl, "bps": round(bps, 2),
                   "px": {f: last_px[(a, f)] for f in FEEDS}, "sig": s, "sig_iv": siv, "mk": {}}
            keep = False
            side_tok = {}
            for (_, tf, o), toks in live:
                fo, fi = fair_up(a, tf, o, t0, px, s), fair_up(a, tf, o, t0, px, siv)
                fo = None if fo is None else round(fo if up else 100 - fo, 2)
                fi = None if fi is None else round(fi if up else 100 - fi, 2)
                tok = toks[0] if up else toks[1]
                asks, bids = ladder(tok)
                rec["mk"][str(tf)] = {"o": o, "open": opens.get((a, tf, o)), "left": round(o + tf * 60 - t0, 1),
                                      "fair": fo, "fair_iv": fi, "snaps": {"0": {"a": asks, "b": bids}}}
                ref = fi if fi is not None else fo
                if asks and ref is not None and ref - asks[0][0] >= MIN_GAP:
                    keep = True
                side_tok[str(tf)] = tok
            if not keep:
                return
            for off in SNAPS:
                time.sleep(max(0.0, t0 + off - time.time()))
                for tf, tok in side_tok.items():
                    asks, bids = ladder(tok)
                    rec["mk"][tf]["snaps"][f"{off:g}"] = {"a": asks, "b": bids}
                rec.setdefault("pxs", {})[f"{off:g}"] = {f: last_px[(a, f)] for f in FEEDS}
            H["db_exec"]("INSERT INTO e2_early (t0, asset, feed, level, data) VALUES (?,?,?,?,?)",
                         (t0, a, feed, lvl, json.dumps(rec, separators=(",", ":"))))
            st["saved"] += 1
        except Exception as e:
            st["err"] = f"capture: {str(e)[:80]}"


    def feed_worker(name):
        while True:
            try:
                ws = websocket.create_connection(FEEDS[name], timeout=10)
                ws.settimeout(30)
                while True:
                    d = json.loads(ws.recv()).get("data", {})
                    a = SYM.get(str(d.get("s", "")).upper())
                    b, k = float(d.get("b", 0)), float(d.get("a", 0))
                    if a and b > 0 and k > 0:
                        st["ticks"][name] += 1
                        tick(a, name, time.time(), (b + k) / 2)
            except Exception as e:
                st["err"] = f"feed {name}: {str(e)[:80]}"
            time.sleep(5)


    # ── results ─────────────────────────────────────────────────────────────────
    def outcome_worker():
        while True:
            try:
                rows = H["db_query"]("SELECT asset, tf, open_ts FROM e2_mkts WHERE outcome IS NULL AND open_ts + tf*60 < ? LIMIT 40",
                                     (time.time() - 150,))
                for r in rows:
                    try:
                        m = gamma_market(r["asset"], r["tf"], r["open_ts"])
                        op = m.get("outcomePrices") if m else None
                        op = json.loads(op) if isinstance(op, str) else op
                        out = "UP" if op and float(op[0]) >= 0.99 else "DOWN" if op and float(op[1]) >= 0.99 else None
                        if out:
                            H["db_exec"]("UPDATE e2_mkts SET outcome=? WHERE asset=? AND tf=? AND open_ts=?",
                                         (out, r["asset"], r["tf"], r["open_ts"]))
                    except Exception:
                        pass
                    time.sleep(1)
            except Exception as e:
                st["err"] = f"outcomes: {str(e)[:80]}"
            time.sleep(60)


    # ── telegram (called from bot.py's command loop) ────────────────────────────
    def _send(name, rows, header, caption):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(header)
        w.writerows([list(x) for x in rows])
        r = requests.post(f"https://api.telegram.org/bot{H['token']}/sendDocument",
                          data={"chat_id": H["chat"], "caption": caption[:1000]},
                          files={"document": (name, gzip.compress(buf.getvalue().encode()), "application/gzip")}, timeout=180)
        return r.status_code == 200


    def handle(cmd, arg, tg):
        """returns True if the command was ours"""
        if cmd == "/early2status":
            n = H["db_query"]("SELECT asset, level, COUNT(*) n FROM e2_early GROUP BY asset, level")
            tg("🧪 early2 (BTC+ETH, 2bp+3bp, paper only)\n" + ", ".join(f"{r['asset']} {r['level']:g}bp: {r['n']}" for r in n)
               + f"\nsince start: triggers {st['trig']}, saved {st['saved']} · feed ticks {st['ticks']} · pm msgs {st['pm']}"
               + f" · deribit vol {', '.join('%s %.2f' % (a, v['iv'] or 0) for a, v in ivs.items())}"
               + (f"\nlast error: {st['err']}" if st["err"] else ""))
            return True
        if cmd == "/ethexport":
            return _export(arg, tg, "ETH", "eth")
        if cmd == "/early2export":
            return _export(arg, tg, None, "early2")
        return False


    def _export(arg, tg, asset, name):
        """asset=None -> BTC+ETH. Each command remembers its own 'last export' time."""
        key = f"last_export_{name}"
        since = 0.0
        if arg != "all":
            r = H["db_query"]("SELECT v FROM e2_kv WHERE k=?", (key,))
            since = float(r[0]["v"]) if r else 0.0
        upto = time.time() - EXPORT_LAG
        aw, av = ("AND asset=?", (asset,)) if asset else ("", ())
        rows = H["db_query"](f"SELECT t0, asset, feed, level, data FROM e2_early WHERE t0 > ? AND t0 <= ? {aw} ORDER BY t0", (since, upto) + av)
        if not rows:
            tg(f"no new {name} rows yet")
            return True
        what = asset or "BTC+ETH"
        ok = _send(f"{name}.csv.gz", rows, ["t0", "asset", "feed", "level", "data"], f"{len(rows)} {what} early moves (2bp+3bp)")
        mk = H["db_query"](f"SELECT asset, tf, open_ts, open, outcome FROM e2_mkts WHERE outcome IS NOT NULL {aw} ORDER BY open_ts", av)
        ok2 = _send(f"{name}_mkts.csv.gz", mk, ["asset", "tf", "open_ts", "open", "outcome"], f"{len(mk)} {what} market results")
        if ok and ok2:
            H["db_exec"]("INSERT OR REPLACE INTO e2_kv (k, v) VALUES (?, ?)", (key, str(upto)))
        else:
            tg(f"⚠️ {name} upload failed — nothing marked as sent, try again (or add 'all')")
        return True


    def start(db_exec, db_query, token, chat):
        H.update(db_exec=db_exec, db_query=db_query, token=token, chat=chat)
        db_exec("CREATE TABLE IF NOT EXISTS e2_early (id INTEGER PRIMARY KEY AUTOINCREMENT, t0 REAL, asset TEXT, feed TEXT, level REAL, data TEXT)")
        db_exec("CREATE TABLE IF NOT EXISTS e2_mkts (asset TEXT, tf INTEGER, open_ts INTEGER, open REAL, outcome TEXT, PRIMARY KEY (asset, tf, open_ts))")
        db_exec("CREATE TABLE IF NOT EXISTS e2_kv (k TEXT PRIMARY KEY, v TEXT)")
        for fn in (market_worker, pm_worker, deribit_worker, outcome_worker):
            threading.Thread(target=fn, daemon=True).start()
        for nm in FEEDS:
            threading.Thread(target=feed_worker, args=(nm,), daemon=True).start()
        log.info("early2 logger started (BTC+ETH, 2bp+3bp)")
    return types.SimpleNamespace(**dict(locals()))   # start, handle, st, ... (everything defined above)


def main():
    init_db()
    for fn in (resolver_worker, pm_worker, bn_worker, settler, telegram_worker, balance_worker, keep_warm):
        threading.Thread(target=fn, daemon=True).start()
    if EARLY_LOG:
        for nm in EARLY_FEEDS:
            threading.Thread(target=early_feed_worker, args=(nm,), daemon=True).start()
        threading.Thread(target=chainlink_worker, daemon=True).start()
        threading.Thread(target=deribit_worker, daemon=True).start()
        if FLOW_LOG:
            for v in FLOW_WS:
                threading.Thread(target=flow_worker, args=(v,), daemon=True).start()
            threading.Thread(target=coinbase_worker, daemon=True).start()
        if EXTRA_LOG:
            threading.Thread(target=alt_worker, daemon=True).start()
            threading.Thread(target=extra_recorder, daemon=True).start()
            threading.Thread(target=twap_worker, daemon=True).start()
            for v in lead_px:
                threading.Thread(target=lead_worker, args=(v,), daemon=True).start()
    if DERIBIT_LOG:
        threading.Thread(target=deribit_logger, daemon=True).start()
        threading.Thread(target=close_worker, daemon=True).start()
        threading.Thread(target=close_outcomes, daemon=True).start()
    if D_ON:
        threading.Thread(target=d_settler, daemon=True).start()
    global E2_ON
    if os.environ.get("EARLY2_LOG", "true").strip().lower() in ("1", "true", "yes", "on"):
        try:                                           # second early logger: BTC+ETH, 2bp+3bp (paper only)
            global early2
            early2 = _make_early2()
            early2.start(db_exec, db_query, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
            E2_ON = True
        except Exception as e:
            log.warning(f"early2 not started: {e}")
    bal = ""
    if LIVE_ENABLED or (D_ON and D_LIVE):
        try:
            bal = f" · USDC ${usdc_balance():,.2f} · orders {ORDER_TYPE}"
        except Exception as e:
            bal = f" · ⚠️ couldn't connect to Polymarket: {str(e)[:150]}"
    tg(f"⚡ 50secmaker started — old jump sniper {'OFF' if not JUMPER_ON else ('🟢 LIVE' if LIVE_ENABLED else '📝 PAPER')} · "
       f"${LIVE_STAKE:g}/trade{bal}. Send /status."
       + (f"\n🎯 D rules: {'🟢 LIVE' if D_LIVE else '📝 paper'} · 5m {D_RULE[5]} · 15m {D_RULE[15]} · ${D_STAKE:g} · /dstatus" if D_ON else ""))
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
