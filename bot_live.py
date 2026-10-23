#!/usr/bin/env python3
# FINAL-MINUTE LOGGER (paper, Oct 23 2026) - places NO orders.
#
# Question it answers: in the last ~90 seconds of each BTC/ETH 5m and 15m up/down market,
# does Polymarket's order book lag the price the market SETTLES on (the Chainlink-style
# reference price Polymarket publishes)? If the book is slow, buying the side the reference
# price says is winning could be an edge.
#
# What it records
#   - the reference price from Polymarket's live-data socket (two versions are tried:
#     v2 = wss://ws-live-v2.polymarket.com/ws  (needs API credentials)
#     v1 = wss://ws-live-data.polymarket.com   (crypto_prices_chainlink topic, no login))
#   - Binance BTC/ETH price (bookTicker)
#   - the Polymarket order book of each live market (best bid/ask both sides + $ on offer)
#   - every SAMPLE_SECS (0.25s) during the last LAST_SECS (90s) of each window
#   - after each window: the real outcome, and what the reference price and Binance said
#
# Commands (Telegram, use a NEW bot token from @BotFather - not the scanner's):
#   /cl          status + does the reference price match settlement? + paper edge check
#   /clexport    raw samples + settled markets (gzipped CSV)
#   /help
#
# ENV (required): TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
# ENV (for the v2 socket, optional): POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE
#   or POLY_PRIVATE_KEY (credentials are derived from it; read-only use, nothing is traded)
# ENV (optional): ASSETS=BTC,ETH  TFS=5,15  LAST_SECS=90  SAMPLE_SECS=0.25  STAKE=50
#   RTDS_MODE=both|v2|v1  DB_PATH=final_minute.db  KEEP_DAYS=14

import os, time, json, sqlite3, logging, threading, requests, csv, io, gzip
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo
import websocket

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
DB_PATH = os.environ.get("DB_PATH", "final_minute.db")
ASSETS = [a.strip().upper() for a in os.environ.get("ASSETS", "BTC,ETH").split(",") if a.strip()]
TFS = [int(x) for x in os.environ.get("TFS", "5,15").split(",") if x.strip()]
LAST_SECS = float(os.environ.get("LAST_SECS", "90"))
SAMPLE_SECS = float(os.environ.get("SAMPLE_SECS", "0.25"))
STAKE = float(os.environ.get("STAKE", "50"))
RTDS_MODE = os.environ.get("RTDS_MODE", "both").lower()
KEEP_DAYS = float(os.environ.get("KEEP_DAYS", "14"))
FEE_RATE = 0.07
GAMMA = "https://gamma-api.polymarket.com"
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RTDS_V2 = os.environ.get("RTDS_V2_URL", "wss://ws-live-v2.polymarket.com/ws")
RTDS_V1 = os.environ.get("RTDS_V1_URL", "wss://ws-live-data.polymarket.com")
BINANCE_WS = ("wss://data-stream.binance.vision/stream?streams=" +
              "/".join(f"{a.lower()}usdt@bookTicker" for a in ASSETS))
ET = ZoneInfo("America/New_York")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("final-minute")

lock = threading.RLock()
db_lock = threading.Lock()
# reference prices: src -> asset -> deque[(oracle_ts_seconds, value, received_at)]
ref = {"v2": {a: deque(maxlen=20000) for a in ASSETS}, "v1": {a: deque(maxlen=20000) for a in ASSETS}}
bn = {a: deque(maxlen=200000) for a in ASSETS}          # (ts, mid)
books = {}                                               # token -> {"bids": {px: sz}, "asks": {px: sz}}
tokmap = {}                                              # token -> (asset, tf, open_ts, side)
markets = {}                                             # (asset, tf, open_ts) -> (up_tok, dn_tok)
stats = {"v2_msgs": 0, "v1_msgs": 0, "bn_msgs": 0, "pm_msgs": 0, "v2_state": "off", "v1_state": "off",
         "samples": 0, "errors": 0, "started": time.time()}
raw_seen = {"v2": 0, "v1": 0}


def fee(shares, p):
    return shares * FEE_RATE * p * (1 - p)


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
        c.execute("""CREATE TABLE IF NOT EXISTS samples (ts REAL, asset TEXT, tf INTEGER, open_ts INTEGER,
            secs_left REAL, ref_v2 REAL, ref_v2_age REAL, ref_v1 REAL, ref_v1_age REAL, binance REAL,
            strike_v2 REAL, strike_v1 REAL, strike_bn REAL, up_bid REAL, up_ask REAL, dn_bid REAL,
            dn_ask REAL, up_ask_usd REAL, dn_ask_usd REAL)""")
        c.execute("CREATE INDEX IF NOT EXISTS s_mkt ON samples(asset, tf, open_ts)")
        c.execute("CREATE INDEX IF NOT EXISTS s_ts ON samples(ts)")
        c.execute("""CREATE TABLE IF NOT EXISTS mkts (asset TEXT, tf INTEGER, open_ts INTEGER, outcome TEXT,
            v2_open REAL, v2_close REAL, v1_open REAL, v1_close REAL, bn_open REAL, bn_close REAL,
            PRIMARY KEY (asset, tf, open_ts))""")
        c.execute("CREATE TABLE IF NOT EXISTS raw (ts REAL, src TEXT, msg TEXT)")
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
    L = tf * 60
    o = int(now // L) * L
    return o, o + L, o + L - now


def slug(asset, tf, open_ts):
    return f"{asset.lower()}-updown-{tf}m-{open_ts}"


def resolve(asset, tf, open_ts):
    try:
        r = requests.get(f"{GAMMA}/events", params={"slug": slug(asset, tf, open_ts)}, timeout=8)
        arr = r.json()
        ev = arr[0] if isinstance(arr, list) and arr else arr
        mk = ev.get("markets", []) if isinstance(ev, dict) else []
        if mk:
            t = json.loads(mk[0].get("clobTokenIds", "[]"))
            if len(t) == 2:
                return t[0], t[1]
    except Exception as e:
        log.debug(f"resolve {asset} {tf} {open_ts}: {e}")
    return None


def outcome(asset, tf, open_ts):
    try:
        r = requests.get(f"{GAMMA}/events", params={"slug": slug(asset, tf, open_ts)}, timeout=10)
        d = r.json()
        mk = d[0].get("markets", []) if isinstance(d, list) and d else []
        if not mk:
            return None
        op = mk[0].get("outcomePrices")
        op = json.loads(op) if isinstance(op, str) else op
        if not op or len(op) < 2:
            return None
        if float(op[0]) >= 0.99:
            return "UP"
        if float(op[1]) >= 0.99:
            return "DOWN"
    except Exception as e:
        log.warning(f"outcome {asset} {tf} {open_ts}: {e}")
    return None


def resolver_worker():
    while True:
        try:
            now = time.time()
            for a in ASSETS:
                for tf in TFS:
                    for o in (window(tf, now)[0], window(tf, now + 60)[0]):   # current + next
                        k = (a, tf, o)
                        if k in markets:
                            continue
                        t = resolve(a, tf, o)
                        if t:
                            with lock:
                                markets[k] = t
                                tokmap[t[0]] = (a, tf, o, "UP")
                                tokmap[t[1]] = (a, tf, o, "DOWN")
            with lock:
                for k in [k for k in markets if k[2] + k[1] * 60 < now - 600]:
                    up, dn = markets.pop(k)
                    for t in (up, dn):
                        tokmap.pop(t, None)
                        books.pop(t, None)
        except Exception as e:
            stats["errors"] += 1
            log.error(f"resolver: {e}")
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
        stats["pm_msgs"] += 1
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
                toks = tuple(sorted(tokmap))
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
                    cur = tuple(sorted(tokmap))
                if cur != toks:
                    ws.close()

            ws = websocket.WebSocketApp(MARKET_WS, on_open=on_open, on_message=on_message)
            ws.run_forever(ping_interval=20, ping_timeout=10)
            stop.set()
        except Exception as e:
            stats["errors"] += 1
            log.warning(f"pm ws: {e}")
        time.sleep(1)


def bn_worker():
    while True:
        try:
            ws = websocket.create_connection(BINANCE_WS, timeout=10)
            ws.settimeout(30)
            log.info("binance connected")
            while True:
                d = json.loads(ws.recv()).get("data", {})
                a = str(d.get("s", "")).replace("USDT", "")
                if a in bn:
                    b, k = float(d.get("b", 0)), float(d.get("a", 0))
                    if b > 0 and k > 0:
                        bn[a].append((time.time(), (b + k) / 2))
                        stats["bn_msgs"] += 1
        except Exception as e:
            log.warning(f"binance: {e}")
        time.sleep(3)


SYM = {"btcusd": "BTC", "btc/usd": "BTC", "btc": "BTC", "ethusd": "ETH", "eth/usd": "ETH", "eth": "ETH"}


def ref_points(obj, out):
    """walk any JSON shape and collect (asset, ts_seconds, value) price points"""
    if isinstance(obj, list):
        for x in obj:
            ref_points(x, out)
        return
    if not isinstance(obj, dict):
        return
    sym = obj.get("symbol")
    a = SYM.get(str(sym).lower()) if sym else None
    if a and "value" in obj:
        try:
            t = float(obj.get("timestamp") or obj.get("ts") or 0)
            out.append((a, t / 1000.0 if t > 1e11 else t, float(obj["value"])))
        except Exception:
            pass
    if a and isinstance(obj.get("data"), list):          # snapshot: {"symbol":..,"data":[{timestamp,value}]}
        for p in obj["data"]:
            if isinstance(p, dict) and "value" in p:
                try:
                    t = float(p.get("timestamp") or 0)
                    out.append((a, t / 1000.0 if t > 1e11 else t, float(p["value"])))
                except Exception:
                    pass
    for k in ("payload", "data", "result"):
        v = obj.get(k)
        if isinstance(v, (dict, list)) and not (a and k == "data"):
            ref_points(v, out)


def ref_handle(src, raw):
    now = time.time()
    if raw_seen[src] < 30:                                 # keep the first messages for debugging
        raw_seen[src] += 1
        try:
            db_exec("INSERT INTO raw VALUES (?,?,?)", (now, src, str(raw)[:3000]))
        except Exception:
            pass
    try:
        data = json.loads(raw)
    except Exception:
        return
    if isinstance(data, dict) and data.get("op") == "authed":
        stats[f"{src}_state"] = "authed"
    pts = []
    ref_points(data, pts)
    if pts:
        stats[f"{src}_msgs"] += 1
        stats[f"{src}_state"] = "streaming"
        with lock:
            for a, t, v in pts:
                dq = ref[src][a]
                if t > 0 and (not dq or t > dq[-1][0]):
                    dq.append((t, v, now))


def api_creds():
    k, s, p = (os.environ.get(x) for x in ("POLY_API_KEY", "POLY_API_SECRET", "POLY_API_PASSPHRASE"))
    if k and s and p:
        return k, s, p
    pk = os.environ.get("POLY_PRIVATE_KEY")
    if not pk:
        return None
    try:
        from py_clob_client.client import ClobClient
        c = ClobClient("https://clob.polymarket.com", key=pk, chain_id=137)
        cr = c.create_or_derive_api_creds()
        return cr.api_key, cr.api_secret, cr.api_passphrase
    except Exception as e:
        log.error(f"could not derive API credentials: {e}")
        return None


def v2_worker():
    creds = api_creds()
    if not creds:
        stats["v2_state"] = "no credentials"
        return
    subs = [{"channel": "price.crypto", "filter": {"symbol": f"{a.lower()}usd"}} for a in ASSETS]
    while True:
        try:
            stats["v2_state"] = "connecting"

            def on_open(ws):
                ws.send(json.dumps({"op": "auth", "rid": "a1",
                                    "auth": {"apiKey": creds[0], "secret": creds[1], "passphrase": creds[2]}}))

            def on_message(ws, msg):
                try:
                    d = json.loads(msg)
                    if isinstance(d, dict) and d.get("op") == "authed":
                        ws.send(json.dumps({"op": "subscribe", "rid": "s1", "subscriptions": subs}))
                except Exception:
                    pass
                ref_handle("v2", msg)

            ws = websocket.WebSocketApp(RTDS_V2, on_open=on_open, on_message=on_message)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            log.warning(f"v2: {e}")
        stats["v2_state"] = "reconnecting"
        time.sleep(5)


def v1_worker():
    subs = [{"topic": "crypto_prices_chainlink", "type": "*", "filters": json.dumps({"symbol": f"{a.lower()}/usd"})}
            for a in ASSETS]
    while True:
        try:
            stats["v1_state"] = "connecting"
            stop = threading.Event()

            def on_open(ws):
                ws.send(json.dumps({"action": "subscribe", "subscriptions": subs}))

                def pinger():
                    while not stop.is_set():
                        time.sleep(5)
                        try:
                            ws.send("PING")
                        except Exception:
                            return
                threading.Thread(target=pinger, daemon=True).start()

            def on_message(ws, msg):
                if msg in ("PONG", "pong", ""):
                    return
                ref_handle("v1", msg)

            ws = websocket.WebSocketApp(RTDS_V1, on_open=on_open, on_message=on_message)
            ws.run_forever(ping_interval=20, ping_timeout=10)
            stop.set()
        except Exception as e:
            log.warning(f"v1: {e}")
        stats["v1_state"] = "reconnecting"
        time.sleep(5)


# ── sampling ────────────────────────────────────────────────────────────────
def latest(dq):
    return dq[-1] if dq else None


def price_at(dq, t):
    """value of the first point at or after time t (scans back from the newest point)"""
    hit = None
    for i in range(len(dq) - 1, -1, -1):
        p = dq[i]
        if p[0] < t:
            break
        hit = p[1]
    return hit


strikes = {}      # (src, asset, tf, open_ts) -> first price at/after the open


def strike(src, a, tf, o):
    k = (src, a, tf, o)
    if k not in strikes:
        dq = bn[a] if src == "bn" else ref[src][a]
        if dq and dq[-1][0] >= o:
            v = price_at(dq, o)
            if v is not None and (not dq or dq[0][0] < o):   # only trust it if we saw the moment of the open
                strikes[k] = v
            elif v is not None:
                strikes[k] = None
        if len(strikes) > 5000:
            for x in sorted(strikes, key=lambda z: z[3])[:2500]:
                strikes.pop(x, None)
    return strikes.get(k)


def strike_worker():
    """grab each window's opening price right after the open, while it's cheap to find"""
    while True:
        try:
            now = time.time()
            with lock:
                for a in ASSETS:
                    for tf in TFS:
                        o = window(tf, now)[0]
                        for src in ("v2", "v1", "bn"):
                            strike(src, a, tf, o)
        except Exception as e:
            stats["errors"] += 1
            log.error(f"strike: {e}")
        time.sleep(0.2)


def best(tok, kind):
    b = books.get(tok)
    if not b or not b[kind]:
        return None
    return max(b[kind]) if kind == "bids" else min(b[kind])


def ask_usd(tok, a, cents=2.0):
    b = books.get(tok)
    if not b or a is None:
        return None
    return round(sum(px * sz / 100.0 for px, sz in b["asks"].items() if px <= a + cents + 1e-9), 2)


seen_windows = set()


def sampler():
    buf, last_flush = [], time.time()
    while True:
        try:
            t0 = time.time()
            with lock:
                for a in ASSETS:
                    for tf in TFS:
                        o, c, left = window(tf, t0)
                        if left > LAST_SECS:
                            continue
                        mk = markets.get((a, tf, o))
                        if not mk:
                            continue
                        seen_windows.add((a, tf, o))
                        up, dn = mk
                        ua, da = best(up, "asks"), best(dn, "asks")
                        r2, r1, b = latest(ref["v2"][a]), latest(ref["v1"][a]), latest(bn[a])
                        buf.append((t0, a, tf, o, round(left, 2),
                                    r2[1] if r2 else None, round(t0 - r2[0], 2) if r2 else None,
                                    r1[1] if r1 else None, round(t0 - r1[0], 2) if r1 else None,
                                    b[1] if b else None,
                                    strikes.get(("v2", a, tf, o)), strikes.get(("v1", a, tf, o)), strikes.get(("bn", a, tf, o)),
                                    best(up, "bids"), ua, best(dn, "bids"), da, ask_usd(up, ua), ask_usd(dn, da)))
            if buf and time.time() - last_flush >= 2:
                db_exec("INSERT INTO samples VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", buf, many=True)
                stats["samples"] += len(buf)
                buf, last_flush = [], time.time()
            time.sleep(max(0.0, SAMPLE_SECS - (time.time() - t0)))
        except Exception as e:
            stats["errors"] += 1
            log.error(f"sampler: {e}")
            time.sleep(1)


def settler():
    while True:
        try:
            now = time.time()
            for k in sorted(seen_windows):
                a, tf, o = k
                c = o + tf * 60
                if now < c + 45:
                    continue
                oc = outcome(a, tf, o)
                if oc is None and now < c + 3600:
                    continue
                with lock:
                    vals = (strikes.get(("v2", a, tf, o)), price_at(ref["v2"][a], c),
                            strikes.get(("v1", a, tf, o)), price_at(ref["v1"][a], c),
                            strikes.get(("bn", a, tf, o)), price_at(bn[a], c))
                db_exec("INSERT OR REPLACE INTO mkts VALUES (?,?,?,?,?,?,?,?,?,?)", (a, tf, o, oc or "VOID") + vals)
                seen_windows.discard(k)
            cut = now - KEEP_DAYS * 86400
            if int(now) % 3600 < 31:
                db_exec("DELETE FROM samples WHERE ts < ?", (cut,))
        except Exception as e:
            stats["errors"] += 1
            log.error(f"settler: {e}")
        time.sleep(30)


# ── report ──────────────────────────────────────────────────────────────────
def pred(o, c):
    if o is None or c is None:
        return None
    return "UP" if c >= o else "DOWN"


def cl_text():
    now = time.time()
    L = [f"🔗 <b>FINAL-MINUTE LOGGER</b> · {','.join(ASSETS)} {'/'.join(f'{t}m' for t in TFS)} · last {LAST_SECS:g}s",
         f"feeds: ref v2 {stats['v2_state']} ({stats['v2_msgs']} msgs) · ref v1 {stats['v1_state']} ({stats['v1_msgs']}) · "
         f"binance {stats['bn_msgs']} · book msgs {stats['pm_msgs']} · samples {stats['samples']} · errors {stats['errors']}"]
    for src in ("v2", "v1"):
        for a in ASSETS:
            p = latest(ref[src][a])
            if p:
                L.append(f"  {src} {a}: {p[1]:,.2f} (age {now - p[0]:.1f}s, arrives {p[2] - p[0]:+.2f}s after its timestamp)")
    rows = db_query("SELECT * FROM mkts WHERE outcome != 'VOID'")
    if not rows:
        L.append("no settled markets yet")
        return "\n".join(L)
    for lab, oc, cc in (("ref v2", "v2_open", "v2_close"), ("ref v1", "v1_open", "v1_close"), ("Binance", "bn_open", "bn_close")):
        ok = [r for r in rows if pred(r[oc], r[cc])]
        if ok:
            m = sum(1 for r in ok if pred(r[oc], r[cc]) == r["outcome"])
            L.append(f"{lab} open→close says the winner: {m}/{len(ok)} ({100 * m / len(ok):.1f}%)")
    # paper edge: in the final 30s, buy the side the reference price says is winning, at the ask, when
    # the market still prices it at <= 90c (i.e. the book disagrees / is slow). Once per market.
    src = "ref_v2" if any(r["v2_open"] for r in rows) else "ref_v1"
    strike = "strike_v2" if src == "ref_v2" else "strike_v1"
    out = {(r["asset"], r["tf"], r["open_ts"]): r["outcome"] for r in rows}
    S = db_query(f"SELECT asset, tf, open_ts, secs_left, {src} AS px, {strike} AS k, up_ask, dn_ask, up_ask_usd, dn_ask_usd "
                 f"FROM samples WHERE secs_left <= 30 AND {src} IS NOT NULL AND {strike} IS NOT NULL ORDER BY ts")
    done, n, w, pnl = set(), 0, 0, 0.0
    for s in S:
        k = (s["asset"], s["tf"], s["open_ts"])
        if k in done or k not in out:
            continue
        side = "UP" if s["px"] >= s["k"] else "DOWN"
        a = s["up_ask"] if side == "UP" else s["dn_ask"]
        if a is None or not (5 <= a <= 90):
            continue
        dep = s["up_ask_usd"] if side == "UP" else s["dn_ask_usd"]
        stake = min(STAKE, dep) if dep else STAKE
        sh = stake / (a / 100)
        win = out[k] == side
        pnl += (sh if win else 0) - stake - fee(sh, a / 100)
        n, w = n + 1, w + win
        done.add(k)
    L.append(f"paper edge (last 30s, buy the side {src} says wins when it's ≤90¢, ${STAKE:g}): "
             f"{n} trades · {w} wins · {money(pnl)}")
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
                if t == "/cl":
                    tg(cl_text())
                elif t == "/clexport":
                    s = db_query("SELECT * FROM samples ORDER BY ts")
                    m = db_query("SELECT * FROM mkts ORDER BY open_ts")
                    raw = db_query("SELECT * FROM raw ORDER BY ts")
                    if s:
                        tg_document("fm_samples.csv.gz", rows_csv_gz(s), f"{len(s)} samples")
                    if m:
                        tg_document("fm_mkts.csv.gz", rows_csv_gz(m), f"{len(m)} settled markets")
                    if raw:
                        tg_document("fm_raw.csv.gz", rows_csv_gz(raw), "first raw messages per feed")
                    if not s and not m:
                        tg("nothing logged yet")
                elif t in ("/help", "/start"):
                    tg("🔗 final-minute logger (paper, no orders)\n/cl — status + match rates + paper edge\n"
                       "/clexport — raw data (gz CSV)")
        except Exception as e:
            log.warning(f"telegram: {e}")
            time.sleep(5)


def main():
    init_db()
    for fn in (resolver_worker, pm_worker, bn_worker, strike_worker, sampler, settler, telegram_worker):
        threading.Thread(target=fn, daemon=True).start()
    if RTDS_MODE in ("both", "v2"):
        threading.Thread(target=v2_worker, daemon=True).start()
    if RTDS_MODE in ("both", "v1"):
        threading.Thread(target=v1_worker, daemon=True).start()
    log.info(f"final-minute logger ON: {ASSETS} {TFS} last {LAST_SECS}s every {SAMPLE_SECS}s, ref {RTDS_MODE}")
    tg(f"🔗 final-minute logger started ({','.join(ASSETS)} {'/'.join(f'{t}m' for t in TFS)}, paper, no orders). "
       f"Send /cl in ~30 min.")
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
