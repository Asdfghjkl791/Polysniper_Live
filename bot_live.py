#!/usr/bin/env python3
# LONG-MARKET JUMP LOGGER (paper, Oct 26 2026) - places NO orders.
# Replaces the final-minute logger in the worker service (that one found no edge).
#
# Same test as the main bot's 5m/15m jump logger, but on the LONGER up/down markets:
# when BTC/ETH/SOL moves >= JL_BPS within JL_WINDOW seconds on Binance, snapshot the
# Polymarket books of that coin's live 1h, 4h and daily markets at +0, 0.1, 0.25, 0.5, 1,
# 2, 5 and 10 seconds (best bid/ask both sides, $ on offer, top 6 levels) plus the model
# fair price at the jump. Settled outcomes are logged too, so "hold to the end" can be checked.
#
# DIP LOGGER (added Oct 26 8:30pm): watches the same coins' 5m/15m/1h/4h/daily books for sudden dumps -
# a side's best bid drops >= DIP_CENTS within DIP_WINDOW seconds while the coin itself is flat on Binance
# (moved < DIP_FLAT_BPS). Each dip is snapshotted at +0 .. +30s (both sides, top 6 levels, fair price,
# $ of trades in the window) to test "buy the dip, sell the bounce a few seconds later".
#
# Commands (Telegram, this bot's own token):
#   /jumps        paper sniper versions per coin per market length (last 24h)
#   /jumpexport   raw jump snapshots + settled outcomes (gzipped CSV)
#   /dips         paper dip-buyer versions (last 24h)
#   /dipexport    raw dip snapshots (gzipped CSV)
#   /clexport     the old final-minute logger data (kept in the same database)
#   /oldstats /export /exporttrail /exportsnaps   data from the older last-minute bot
#   /help
#
# ENV (required): TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
# ENV (optional): ASSETS=BTC,ETH,SOL  TFS=60,240,1440  JL_BPS=5  JL_WINDOW=2  JL_COOLDOWN=10
#   JL_STAKE=50  DB_PATH=final_minute.db  KEEP_DAYS=30

import os, time, json, sqlite3, logging, threading, requests, csv, io, gzip, math
from collections import deque
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import websocket

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
DB_PATH = os.environ.get("DB_PATH", "final_minute.db")
ASSETS = [a.strip().upper() for a in os.environ.get("ASSETS", "BTC,ETH,SOL").split(",") if a.strip()]
TFS = [int(x) for x in os.environ.get("TFS", "60,240,1440").split(",") if x.strip()]          # jump logger
DIP_TFS = [int(x) for x in os.environ.get("DIP_TFS", "5,15,60,240,1440").split(",") if x.strip()]  # dip logger
ALL_TFS = sorted(set(TFS) | set(DIP_TFS))
DIP_CENTS = float(os.environ.get("DIP_CENTS", "5"))
DIP_WINDOW = float(os.environ.get("DIP_WINDOW", "2"))
DIP_FLAT_BPS = float(os.environ.get("DIP_FLAT_BPS", "1.5"))
DIP_COOLDOWN = float(os.environ.get("DIP_COOLDOWN", "30"))
DIP_OFFSETS = (0.0, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0)
JL_BPS = float(os.environ.get("JL_BPS", "5"))
JL_WINDOW = float(os.environ.get("JL_WINDOW", "2"))
JL_COOLDOWN = float(os.environ.get("JL_COOLDOWN", "10"))
JL_STAKE = float(os.environ.get("JL_STAKE", "50"))
KEEP_DAYS = float(os.environ.get("KEEP_DAYS", "30"))
JL_OFFSETS = (0.0, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0)
FEE_RATE = 0.07
GAMMA = "https://gamma-api.polymarket.com"
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS = ("wss://data-stream.binance.vision/stream?streams=" +
              "/".join(f"{a.lower()}usdt@bookTicker" for a in ASSETS))
BINANCE_REST = os.environ.get("BINANCE_REST", "https://data-api.binance.vision/api/v3/klines")
ET = ZoneInfo("America/New_York")
FULLNAME = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "XRP": "xrp", "DOGE": "dogecoin",
            "BNB": "bnb", "HYPE": "hype"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("long-jumps")

lock = threading.RLock()          # books / markets
jlock = threading.Lock()          # jump detection state
db_lock = threading.Lock()
books = {}                        # token -> {"bids": {px: sz}, "asks": {px: sz}}
markets = {}                      # (asset, tf, open_ts) -> (up_tok, dn_tok)
prices = {}                       # asset -> latest Binance mid
ticks = {a: deque(maxlen=5000) for a in ASSETS}     # (ts, px) every update, last JL_WINDOW s
px1s = {a: deque(maxlen=1200) for a in ASSETS}      # (ts, px) ~1/s, for the volatility estimate
opens = {}                        # (asset, tf, open_ts) -> window start price
last_jump = {a: 0.0 for a in ASSETS}
pending = []
trades = {}                       # token -> deque[(ts, price_c, size, side)] from last_trade_price
bid_hist = {}                     # token -> deque[(ts, best_bid)] sampled every 0.1s
dip_last = {}                     # token -> time of last dip
book_reset = {}                   # token -> time of the last full book snapshot (resubscribe)
dip_pending = []
stats = {"dips": 0, "bn_msgs": 0, "pm_msgs": 0, "jumps": {a: 0 for a in ASSETS}, "rows": 0, "errors": 0,
         "started": time.time()}


def fee(shares, p):
    return shares * FEE_RATE * p * (1 - p)


def money(x):
    return f"{'+' if x >= 0 else chr(0x2212)}${abs(x):.2f}"


def label(tf):
    return {60: "1h", 240: "4h", 1440: "1d"}.get(tf, f"{tf}m")


# ── database ────────────────────────────────────────────────────────────────
def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with db_lock:
        c = db()
        c.execute("""CREATE TABLE IF NOT EXISTS lj_jumps (jid INTEGER, t0 REAL, bps REAL, tf INTEGER,
            open_ts INTEGER, off REAL, real_dt REAL, btc REAL, up_bid REAL, up_ask REAL, dn_bid REAL,
            dn_ask REAL, secs_left REAL, fair_up REAL, up_ask_usd REAL, dn_ask_usd REAL, asset TEXT,
            ladder TEXT, strike REAL)""")
        c.execute("CREATE INDEX IF NOT EXISTS lj_t0 ON lj_jumps(t0)")
        c.execute("""CREATE TABLE IF NOT EXISTS lj_dips (did INTEGER, t0 REAL, asset TEXT, tf INTEGER,
            open_ts INTEGER, side TEXT, drop_c REAL, bid_before REAL, coin_bps REAL, sell_usd REAL, buy_usd REAL,
            off REAL, real_dt REAL, coin REAL, up_bid REAL, up_ask REAL, dn_bid REAL, dn_ask REAL,
            secs_left REAL, fair_up REAL, up_ask_usd REAL, dn_ask_usd REAL, ladder TEXT)""")
        c.execute("CREATE INDEX IF NOT EXISTS ld_t0 ON lj_dips(t0)")
        c.execute("""CREATE TABLE IF NOT EXISTS lj_mkts (asset TEXT, tf INTEGER, open_ts INTEGER, outcome TEXT,
            strike REAL, PRIMARY KEY (asset, tf, open_ts))""")
        c.commit()
        c.close()


def db_exec(sql, args=(), many=False):
    with db_lock:
        c = db()
        (c.executemany if many else c.execute)(sql, args)
        c.commit()
        c.close()


def db_query(sql, args=()):
    with db_lock:
        c = db()
        r = c.execute(sql, args).fetchall()
        c.close()
        return r


# ── telegram ────────────────────────────────────────────────────────────────
def tg(msg):
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4000], "parse_mode": "HTML"}, timeout=8)
    except Exception as e:
        log.error(f"tg: {e}")


def tg_document(name, data, caption=""):
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
                          data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:1000]},
                          files={"document": (name, data, "application/gzip")}, timeout=300)
        if r.status_code != 200:
            tg(f"⚠️ upload failed ({r.status_code})")
    except Exception as e:
        tg(f"⚠️ upload failed: {e}")


def rows_csv_gz(rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    if rows:
        w.writerow(rows[0].keys())
        w.writerows([list(r) for r in rows])
    return gzip.compress(buf.getvalue().encode())


# ── markets ─────────────────────────────────────────────────────────────────
def window(tf, now):
    """(open_ts, close_ts, secs_left) of the live window; 1h/4h on ET clock hours, daily noon ET -> noon ET"""
    now_et = datetime.fromtimestamp(now, tz=ET)
    if tf == 1440:
        st = now_et.replace(hour=12, minute=0, second=0, microsecond=0)
        if now_et < st:
            st = st - timedelta(days=1)
        en = (st + timedelta(days=1)).replace(hour=12)
        o, c = int(st.timestamp()), int(en.timestamp())
        return o, c, c - now
    if tf == 60:
        o = int(now_et.replace(minute=0, second=0, microsecond=0).timestamp())
        return o, o + 3600, o + 3600 - now
    if tf == 240:
        o = int(now_et.replace(hour=(now_et.hour // 4) * 4, minute=0, second=0, microsecond=0).timestamp())
        return o, o + 14400, o + 14400 - now
    L = tf * 60
    o = int(now // L) * L
    return o, o + L, o + L - now


def close_of(tf, open_ts):
    return window(tf, open_ts + 1)[1]


def slug(asset, tf, open_ts):
    if tf == 1440:
        d = datetime.fromtimestamp(open_ts, tz=ET) + timedelta(days=1)
        return f"{FULLNAME[asset]}-up-or-down-on-{d.strftime('%B').lower()}-{d.day}-{d.year}"
    if tf == 240:
        return f"{asset.lower()}-updown-4h-{open_ts}"
    if tf == 60:
        e = datetime.fromtimestamp(open_ts, tz=ET)
        h = e.strftime("%I").lstrip("0") or "12"
        return f"{FULLNAME[asset]}-up-or-down-{e.strftime('%B').lower()}-{e.day}-{e.year}-{h}{e.strftime('%p').lower()}-et"
    return f"{asset.lower()}-updown-{tf}m-{open_ts}"


def gamma_market(asset, tf, open_ts):
    r = requests.get(f"{GAMMA}/events", params={"slug": slug(asset, tf, open_ts)}, timeout=10)
    arr = r.json()
    ev = arr[0] if isinstance(arr, list) and arr else arr
    mk = ev.get("markets", []) if isinstance(ev, dict) else []
    return mk[0] if mk else None


def resolve(asset, tf, open_ts):
    try:
        m = gamma_market(asset, tf, open_ts)
        if m:
            t = json.loads(m.get("clobTokenIds", "[]"))
            if len(t) == 2:
                return str(t[0]), str(t[1])
    except Exception as e:
        log.debug(f"resolve {asset} {tf} {open_ts}: {e}")
    return None


def outcome(asset, tf, open_ts):
    try:
        m = gamma_market(asset, tf, open_ts)
        if not m:
            return None
        op = m.get("outcomePrices")
        op = json.loads(op) if isinstance(op, str) else op
        if op and len(op) >= 2:
            if float(op[0]) >= 0.99:
                return "UP"
            if float(op[1]) >= 0.99:
                return "DOWN"
    except Exception as e:
        log.warning(f"outcome {asset} {tf} {open_ts}: {e}")
    return None


def fetch_open(asset, tf, open_ts):
    """window start price = Binance 1m open at the window start (fetched once, cached)"""
    k = (asset, tf, open_ts)
    if opens.get(k):
        return
    try:
        r = requests.get(BINANCE_REST, params=dict(symbol=f"{asset}USDT", interval="1m",
                                                   startTime=int(open_ts) * 1000,
                                                   endTime=int(open_ts) * 1000 + 59000, limit=1), timeout=8)
        j = r.json()
        if isinstance(j, list) and j:
            opens[k] = float(j[0][1])
            db_exec("INSERT OR IGNORE INTO lj_mkts (asset, tf, open_ts, strike) VALUES (?,?,?,?)",
                    (asset, tf, open_ts, opens[k]))
    except Exception as e:
        log.warning(f"open price {k}: {e}")


def resolver_worker():
    while True:
        try:
            now = time.time()
            for a in ASSETS:
                for tf in ALL_TFS:
                    cur = window(tf, now)[0]
                    if now >= cur + 60:
                        fetch_open(a, tf, cur)
                    for o in (cur, window(tf, now + 120)[0]):        # current + next
                        if (a, tf, o) in markets:
                            continue
                        t = resolve(a, tf, o)
                        if t:
                            with lock:
                                markets[(a, tf, o)] = t
            with lock:
                for k in [k for k in markets if close_of(k[1], k[2]) < now - 300]:
                    for t in markets.pop(k):
                        books.pop(t, None)
            for k in [k for k in opens if close_of(k[1], k[2]) < now - 86400]:
                opens.pop(k, None)
        except Exception as e:
            stats["errors"] += 1
            log.error(f"resolver: {e}")
        time.sleep(5)


def settler():
    """fill in outcomes of finished windows (for the hold-to-the-end check)"""
    while True:
        try:
            now = time.time()
            rows = db_query("SELECT asset, tf, open_ts FROM lj_mkts WHERE outcome IS NULL")
            for r in rows:
                if close_of(r["tf"], r["open_ts"]) > now - 120:
                    continue
                o = outcome(r["asset"], r["tf"], r["open_ts"])
                if o:
                    db_exec("UPDATE lj_mkts SET outcome=? WHERE asset=? AND tf=? AND open_ts=?",
                            (o, r["asset"], r["tf"], r["open_ts"]))
            db_exec("DELETE FROM lj_jumps WHERE t0 < ?", (now - KEEP_DAYS * 86400,))
            db_exec("DELETE FROM lj_dips WHERE t0 < ?", (now - KEEP_DAYS * 86400,))
        except Exception as e:
            log.warning(f"settler: {e}")
        time.sleep(120)


# ── feeds ───────────────────────────────────────────────────────────────────
def pm_handle(raw):
    try:
        data = json.loads(raw)
    except Exception:
        return
    for m in (data if isinstance(data, list) else [data]):
        if not isinstance(m, dict):
            continue
        stats["pm_msgs"] += 1
        et = m.get("event_type")
        with lock:
            if et == "book":
                tok = str(m.get("asset_id"))
                book_reset[tok] = time.time()
                books[tok] = {"bids": {round(float(x["price"]) * 100, 3): float(x["size"]) for x in (m.get("bids") or m.get("buys") or [])},
                              "asks": {round(float(x["price"]) * 100, 3): float(x["size"]) for x in (m.get("asks") or m.get("sells") or [])}}
            elif et == "last_trade_price":
                try:
                    tok = str(m.get("asset_id"))
                    trades.setdefault(tok, deque(maxlen=500)).append(
                        (time.time(), round(float(m["price"]) * 100, 3), float(m.get("size", 0)),
                         str(m.get("side", "")).upper()))
                except Exception:
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
                if cur != toks:                     # new windows -> resubscribe
                    ws.close()

            ws = websocket.WebSocketApp(MARKET_WS, on_open=on_open, on_message=on_message)
            ws.run_forever(ping_interval=20, ping_timeout=10)
            stop.set()
        except Exception as e:
            stats["errors"] += 1
            log.warning(f"pm ws: {e}")
        time.sleep(1)


def on_tick(a, now, px):
    with jlock:
        prices[a] = px
        p1 = px1s[a]
        if not p1 or now - p1[-1][0] >= 1.0:
            p1.append((now, px))
        dq = ticks[a]
        dq.append((now, px))
        while dq and dq[0][0] < now - JL_WINDOW:
            dq.popleft()
        if now - last_jump[a] < JL_COOLDOWN or len(dq) < 2:
            return
        bps = (px / dq[0][1] - 1) * 1e4
        if abs(bps) < JL_BPS:
            return
        last_jump[a] = now
        stats["jumps"][a] += 1
        pending.append({"id": int(now * 1000), "asset": a, "t0": now, "bps": round(bps, 2), "done": set()})


def bn_worker():
    while True:
        try:
            ws = websocket.create_connection(BINANCE_WS, timeout=10)
            ws.settimeout(30)
            log.info("binance connected")
            while True:
                d = json.loads(ws.recv()).get("data", {})
                a = str(d.get("s", "")).replace("USDT", "")
                if a in ticks:
                    b, k = float(d.get("b", 0)), float(d.get("a", 0))
                    if b > 0 and k > 0:
                        stats["bn_msgs"] += 1
                        on_tick(a, time.time(), (b + k) / 2)
        except Exception as e:
            log.warning(f"binance: {e}")
        time.sleep(3)


# ── fair price ──────────────────────────────────────────────────────────────
def norm_cdf(z):
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def sigma(a):
    """per-second vol from ~1s samples over the last 15 min (same recipe as the main bot)"""
    pts = [p for p in px1s[a] if p[0] >= time.time() - 900]
    r = [(math.log(p1 / p0)) ** 2 / (t1 - t0) for (t0, p0), (t1, p1) in zip(pts, pts[1:]) if t1 > t0 and p0 > 0 and p1 > 0]
    if len(r) < 30:
        return 6e-5
    return max(2e-5, math.sqrt(sum(r) / len(r)))


def fair_up(a, tf, open_ts, now, px):
    op = opens.get((a, tf, open_ts))
    left = close_of(tf, open_ts) - now
    if not op or not px or left <= 0:
        return None
    return 100.0 * norm_cdf(math.log(px / op) / (sigma(a) * math.sqrt(left)))


# ── snapshots ───────────────────────────────────────────────────────────────
def _best(b, kind):
    if not b or not b[kind]:
        return None
    return max(b[kind]) if kind == "bids" else min(b[kind])


def _depth(b, best, cents=2.0):
    if not b or best is None:
        return None
    return round(sum(px * sz / 100.0 for px, sz in b["asks"].items() if px <= best + cents + 1e-9), 2)


def _ladder(bu, bd, n=6):
    def lv(b, kind):
        if not b or not b[kind]:
            return []
        return [[k, round(b[kind][k], 1)] for k in sorted(b[kind], reverse=(kind == "bids"))[:n]]
    return json.dumps({"ua": lv(bu, "asks"), "ub": lv(bu, "bids"), "da": lv(bd, "asks"), "db": lv(bd, "bids")},
                      separators=(",", ":"))


def books_now(a, now):
    out = []
    with lock:
        for (a2, tf, o), (u, d) in markets.items():
            if a2 != a or tf not in TFS or not (o <= now < close_of(tf, o)):
                continue
            bu, bd = books.get(u), books.get(d)
            ua, da = _best(bu, "asks"), _best(bd, "asks")
            out.append((tf, o, _best(bu, "bids"), ua, _best(bd, "bids"), da, _depth(bu, ua), _depth(bd, da),
                        _ladder(bu, bd)))
    return out


def jl_worker():
    while True:
        try:
            time.sleep(0.02)
            now = time.time()
            with jlock:
                todo = []
                for ev in pending:
                    for off in JL_OFFSETS:
                        if off not in ev["done"] and now >= ev["t0"] + off:
                            ev["done"].add(off)
                            todo.append((ev, off))
                pending[:] = [e for e in pending if len(e["done"]) < len(JL_OFFSETS)]
            if not todo:
                continue
            rows, cache = [], {}
            for ev, off in todo:
                a = ev["asset"]
                if a not in cache:
                    cache[a] = (prices.get(a), books_now(a, now))
                px, bks = cache[a]
                for tf, o, ub, ua, dbid, da, uad, dad, lad in bks:
                    fu = fair_up(a, tf, o, ev["t0"], px) if off == 0 else None
                    rows.append((ev["id"], ev["t0"], ev["bps"], tf, o, off, round(now - ev["t0"], 3), px,
                                 ub, ua, dbid, da, round(close_of(tf, o) - ev["t0"], 1),
                                 round(fu, 2) if fu is not None else None, uad, dad, a, lad,
                                 opens.get((a, tf, o))))
            if rows:
                db_exec("INSERT INTO lj_jumps (jid,t0,bps,tf,open_ts,off,real_dt,btc,up_bid,up_ask,dn_bid,dn_ask,"
                        "secs_left,fair_up,up_ask_usd,dn_ask_usd,asset,ladder,strike) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows, many=True)
                stats["rows"] += len(rows)
        except Exception as e:
            stats["errors"] += 1
            log.error(f"jl: {e}")
            time.sleep(1)


# ── dips ────────────────────────────────────────────────────────────────────
def coin_move_bps(a, secs):
    with jlock:
        dq = ticks[a]
        if not dq:
            return None
        now_px = dq[-1][1]
        old = [p for ts, p in dq if ts >= time.time() - secs]
        if not old:
            return 0.0
        return (now_px / old[0] - 1) * 1e4


def trade_usd(tok, since):
    s = b = 0.0
    for ts, px, sz, side in list(trades.get(tok, ())):
        if ts >= since:
            if side == "SELL":
                s += px * sz / 100
            else:
                b += px * sz / 100
    return round(s, 2), round(b, 2)


def dip_snapshot(ev, off, now):
    a, tf, o = ev["asset"], ev["tf"], ev["open_ts"]
    with lock:
        t = markets.get((a, tf, o))
        if not t:
            return None
        bu, bd = books.get(t[0]), books.get(t[1])
        ua, da = _best(bu, "asks"), _best(bd, "asks")
        row = (_best(bu, "bids"), ua, _best(bd, "bids"), da, _depth(bu, ua), _depth(bd, da), _ladder(bu, bd))
    px = prices.get(a)
    fu = fair_up(a, tf, o, ev["t0"], px) if off == 0 else None
    return (ev["id"], ev["t0"], a, tf, o, ev["side"], ev["drop"], ev["before"], ev["bps"], ev["sell"], ev["buy"],
            off, round(now - ev["t0"], 3), px) + row[:4] + (round(close_of(tf, o) - ev["t0"], 1),
            round(fu, 2) if fu is not None else None) + row[4:]


def dip_worker():
    last_sample = 0.0
    while True:
        try:
            time.sleep(0.02)
            now = time.time()
            if now - last_sample >= 0.1:                      # sample best bids, look for sudden drops
                last_sample = now
                found = []
                with lock:
                    live = [(k, t) for k, t in markets.items()
                            if k[1] in DIP_TFS and k[2] <= now < close_of(k[1], k[2])]
                    for (a, tf, o), (u, d) in live:
                        for side, tok in (("UP", u), ("DOWN", d)):
                            bb = _best(books.get(tok), "bids")
                            h = bid_hist.setdefault(tok, deque(maxlen=200))
                            if bb is None or now - book_reset.get(tok, 0) < 3:
                                h.clear()                     # ignore jumps caused by a fresh book snapshot
                                continue
                            h.append((now, bb))
                            while h and h[0][0] < now - DIP_WINDOW:
                                h.popleft()
                            hi = max(x for _, x in h)
                            if hi - bb >= DIP_CENTS and 5 <= bb <= 95 and now - dip_last.get(tok, 0) >= DIP_COOLDOWN:
                                found.append((a, tf, o, side, tok, hi, bb))
                for a, tf, o, side, tok, hi, bb in found:
                    mv = coin_move_bps(a, DIP_WINDOW + 0.5)
                    if mv is None or abs(mv) >= DIP_FLAT_BPS:
                        continue                              # the coin moved: real news, not a dump
                    dip_last[tok] = now
                    stats["dips"] += 1
                    sell, buy = trade_usd(tok, now - DIP_WINDOW - 0.5)
                    dip_pending.append({"id": int(now * 1000) * 10 + len(dip_pending) % 10, "t0": now, "asset": a,
                                        "tf": tf, "open_ts": o, "side": side, "drop": round(hi - bb, 2),
                                        "before": hi, "bps": round(mv, 2), "sell": sell, "buy": buy,
                                        "done": set()})
            todo = []
            for ev in dip_pending:
                for off in DIP_OFFSETS:
                    if off not in ev["done"] and now >= ev["t0"] + off:
                        ev["done"].add(off)
                        todo.append((ev, off))
            dip_pending[:] = [e for e in dip_pending if len(e["done"]) < len(DIP_OFFSETS)]
            rows = [r for r in (dip_snapshot(ev, off, now) for ev, off in todo) if r]
            if rows:
                db_exec("INSERT INTO lj_dips (did,t0,asset,tf,open_ts,side,drop_c,bid_before,coin_bps,sell_usd,buy_usd,"
                        "off,real_dt,coin,up_bid,up_ask,dn_bid,dn_ask,secs_left,fair_up,up_ask_usd,dn_ask_usd,ladder) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows, many=True)
        except Exception as e:
            stats["errors"] += 1
            log.error(f"dip: {e}")
            time.sleep(1)


def dips_text():
    now = time.time()
    L = [f"📉 <b>DIP LOGGER</b> · {','.join(ASSETS)} {'/'.join(label(t) for t in DIP_TFS)} · bid drops "
         f"≥{DIP_CENTS:g}¢ in {DIP_WINDOW:g}s while the coin moves <{DIP_FLAT_BPS:g}bp",
         f"dips since restart: {stats['dips']}"]
    rows = db_query("SELECT * FROM lj_dips WHERE t0 >= ? ORDER BY did, off", (now - 86400,))
    if not rows:
        L.append("no dips logged yet")
        return "\n".join(L)
    ev = {}
    for r in rows:
        ev.setdefault(r["did"], {})[r["off"]] = r
    vers = [("every dip", lambda e, s: True), ("edge 10+", lambda e, s: e is not None and e >= 10),
            ("real dump ($50+ sold)", lambda e, s: s >= 50),
            ("edge 10+ & $50+ sold", lambda e, s: e is not None and e >= 10 and s >= 50)]
    tot = {}
    for did, s in ev.items():
        z, a_s = s.get(0.0), s.get(0.25)
        if not z or not a_s:
            continue
        up = z["side"] == "UP"
        x = a_s["up_ask"] if up else a_s["dn_ask"]
        dep = a_s["up_ask_usd"] if up else a_s["dn_ask_usd"]
        if x is None or not (5 <= x <= 95):
            continue
        fu = z["fair_up"]
        e = None if fu is None else (fu if up else 100 - fu) - x
        st = min(JL_STAKE, dep) if dep else JL_STAKE
        sh = st / (x / 100.0)
        for exit_off in (5.0, 30.0):
            b_s = s.get(exit_off)
            b = (b_s["up_bid"] if up else b_s["dn_bid"]) if b_s else None
            if b is None:
                continue
            pnl = sh * b / 100.0 - st - fee(sh, x / 100.0) - fee(sh, b / 100.0)
            for k, f in vers:
                if f(e, z["sell_usd"] or 0):
                    t = tot.setdefault((z["tf"], k, exit_off), [0.0, 0, 0])
                    t[0] += pnl
                    t[1] += 1
                    t[2] += pnl > 0
    L.append(f"<b>paper dip-buyer</b> · last 24h · buy the dipped side at the +0.25s ask, sell at the +5s / +30s bid, "
             f"${JL_STAKE:g}, after fees:")
    for tf in DIP_TFS:
        for k, _ in vers:
            a5, a30 = tot.get((tf, k, 5.0)), tot.get((tf, k, 30.0))
            if a5 or a30:
                L.append(f"{label(tf)} {k}: " + " · ".join(
                    f"@{int(o)}s {n} tr {money(v)} ({w * 100 // n}% win)"
                    for o, q in ((5, a5), (30, a30)) if q for v, n, w in [q]))
    return "\n".join(L)


# ── /jumps ──────────────────────────────────────────────────────────────────
VERS = [("every jump", lambda x, e, g: True),
        ("calm 5+ min + <85c", lambda x, e, g: g > 300 and x < 85),
        ("edge 10+ + <85c", lambda x, e, g: e is not None and e >= 10 and x < 85),
        ("edge 20+", lambda x, e, g: e is not None and e >= 20)]


def jumps_text():
    now = time.time()
    with lock:
        nm = len(markets)
    L = [f"⚡ <b>LONG-MARKET JUMP LOGGER</b> · {','.join(ASSETS)} {'/'.join(label(t) for t in TFS)} · "
         f"moves ≥{JL_BPS:g}bp in {JL_WINDOW:g}s",
         f"up {(now - stats['started']) / 3600:.1f}h · markets watched {nm} · binance {stats['bn_msgs']} · "
         f"book msgs {stats['pm_msgs']} · errors {stats['errors']}",
         "jumps since restart: " + " · ".join(f"{a} {n}" for a, n in stats["jumps"].items())]
    rows = db_query("SELECT * FROM lj_jumps WHERE t0 >= ? ORDER BY jid, tf, off", (now - 86400,))
    if not rows:
        L.append("no jumps logged yet")
        return "\n".join(L)
    ev = {}
    for r in rows:
        ev.setdefault((r["asset"], r["jid"], r["tf"]), {})[r["off"]] = r
    t0s = {}
    for (a, jid, tf), s in ev.items():
        if 0.0 in s:
            t0s.setdefault(a, set()).add((s[0.0]["t0"], jid))
    gaps = {}
    for a, s in t0s.items():
        prev = None
        for t0, jid in sorted(s):
            gaps[(a, jid)] = (t0 - prev) if prev is not None else 99999
            prev = t0
    tot = {}
    for (a, jid, tf), s in ev.items():
        a_s, b_s, z_s = s.get(0.25), s.get(5.0), s.get(0.0)
        if not a_s or not b_s or not z_s:
            continue
        up = a_s["bps"] > 0
        x = a_s["up_ask"] if up else a_s["dn_ask"]
        b = b_s["up_bid"] if up else b_s["dn_bid"]
        dep = a_s["up_ask_usd"] if up else a_s["dn_ask_usd"]
        if x is None or b is None or not (5 <= x <= 95):
            continue
        fu = z_s["fair_up"]
        e = None if fu is None else (fu if up else 100 - fu) - x
        g = gaps.get((a, jid), 99999)
        st = min(JL_STAKE, dep) if dep else JL_STAKE
        sh = st / (x / 100.0)
        pnl = sh * b / 100.0 - st - fee(sh, x / 100.0) - fee(sh, b / 100.0)
        for k, f in VERS:
            if f(x, e, g):
                t = tot.setdefault((a, tf, k), [0.0, 0, 0])
                t[0] += pnl
                t[1] += 1
                t[2] += pnl > 0
    L.append(f"<b>paper sniper</b> · last 24h · buy at +0.25s ask, sell at +5s bid, ${JL_STAKE:g}, after fees:")
    for a in ASSETS:
        for tf in TFS:
            if not any((a, tf, k) in tot for k, _ in VERS):
                continue
            L.append(f"{a} {label(tf)}: " + " · ".join(
                f"{k} {n} tr {money(v)}" + (f" ({w * 100 // n}% win)" if n else "")
                for k, _ in VERS for v, n, w in [tot.get((a, tf, k), [0.0, 0, 0])]))
    return "\n".join(L)


# ── old data kept in the same database ──────────────────────────────────────
OLD_TABLES = {"/export": ("trades", "lastmin_trades.csv.gz"), "/exporttrail": ("trail", "trail_trades.csv.gz"),
              "/exportsnaps": ("snaps", "lastmin_snaps.csv.gz")}


def old_export(cmd):
    have = {r[0] for r in db_query("SELECT name FROM sqlite_master WHERE type='table'")}
    if cmd == "/clexport":
        sent = False
        for tb, name in (("samples", "fm_samples.csv.gz"), ("mkts", "fm_mkts.csv.gz")):
            if tb in have:
                rows = db_query(f"SELECT * FROM {tb}")
                if rows:
                    tg_document(name, rows_csv_gz(rows), f"{len(rows)} rows (old final-minute logger)")
                    sent = True
        if not sent:
            tg("no final-minute data in this database")
        return
    if cmd == "/oldstats":
        L = ["📦 <b>old bots' data</b> (kept in the database)"]
        for tb in ("trades", "trail", "snaps", "samples", "mkts"):
            L.append(f"{tb}: {db_query(f'SELECT COUNT(*) FROM {tb}')[0][0] if tb in have else 'no table'} rows")
        tg("\n".join(L))
        return
    tb, name = OLD_TABLES[cmd]
    if tb not in have:
        tg(f"no '{tb}' table in this database")
        return
    rows = db_query(f"SELECT * FROM {tb}")
    if not rows:
        tg(f"'{tb}' is empty")
        return
    tg_document(name, rows_csv_gz(rows), f"{len(rows)} rows from the old bot's '{tb}' table")


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
                if t == "/jumps":
                    tg(jumps_text())
                elif t == "/jumpexport":
                    j = db_query("SELECT * FROM lj_jumps ORDER BY jid, tf, off")
                    m = db_query("SELECT * FROM lj_mkts ORDER BY open_ts")
                    if j:
                        tg_document("lj_jumps.csv.gz", rows_csv_gz(j), f"{len(j)} jump snapshots (1h/4h/1d)")
                    if m:
                        tg_document("lj_mkts.csv.gz", rows_csv_gz(m), f"{len(m)} markets with start price + outcome")
                    if not j and not m:
                        tg("no jumps logged yet")
                elif t == "/dips":
                    tg(dips_text())
                elif t == "/dipexport":
                    d = db_query("SELECT * FROM lj_dips ORDER BY did, off")
                    if d:
                        tg_document("lj_dips.csv.gz", rows_csv_gz(d), f"{len(d)} dip snapshots")
                    else:
                        tg("no dips logged yet")
                elif t in ("/clexport", "/export", "/exporttrail", "/exportsnaps", "/oldstats"):
                    old_export(t)
                elif t in ("/help", "/start"):
                    tg("⚡ long-market jump logger (paper, no orders)\n"
                       "/jumps — paper sniper per coin per market length (last 24h)\n"
                       "/jumpexport — raw jump snapshots + outcomes (gz CSV)\n"
                       "/dips — paper dip-buyer (sudden dumps while the coin is flat) · /dipexport — raw dip snapshots\n"
                       "old data: /clexport (final-minute logger) · /oldstats · /export · /exporttrail · /exportsnaps")
        except Exception as e:
            log.warning(f"telegram: {e}")
            time.sleep(5)


def main():
    init_db()
    for fn in (resolver_worker, pm_worker, bn_worker, jl_worker, dip_worker, settler, telegram_worker):
        threading.Thread(target=fn, daemon=True).start()
    log.info(f"long-market jump logger ON: {ASSETS} {TFS} ≥{JL_BPS}bp in {JL_WINDOW}s")
    tg(f"⚡ long-market jump logger + 📉 dip logger started ({','.join(ASSETS)} · jumps {'/'.join(label(t) for t in TFS)}"
       f" · dips {'/'.join(label(t) for t in DIP_TFS)}, paper, no orders). Send /jumps or /dips in a few hours.")
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
