#!/usr/bin/env python3
# PAPER LAST-MINUTE LEADER — buys the side that's ahead in the final ~90s of
# each window and holds to settlement (no money)
#
# THE IDEA
#   Two weeks of favorite-dip data showed that mid-window prices are fair, but
#   in the LAST MINUTE of 15m markets the leading side (priced 50-90c) won
#   ~7-8c more often than its price implied — on the original data AND again
#   on fresh data it was never fitted to. Two minutes earlier the gap was only
#   ~1.5c, so the effect lives right at the end.
#
# WHAT THIS BOT DOES
#   - In each window, once secs_left <= ENTRY_SECS_LEFT, it reads BOTH books.
#   - The side with the higher mid is the "leader". If the leader's best ask
#     is between MIN_CENTS and MAX_CENTS, it paper-buys $STAKE of it — once
#     per market, never both sides.
#   - Fill = walks the REAL ask book for the full stake (VWAP), not just the
#     top price, so thin books cost what they'd really cost.
#   - Real Polymarket taker fee is charged on every fill:
#       fee = shares * FEE_RATE * price * (1 - price)
#   - Holds to settlement. No ladder, no early exit.
#
# HONEST LIMITS
#   - UNTESTED LIVE. The edge was measured on 1-minute MID prices; real asks
#     in the last minute may sit higher. That's exactly what this run tests.
#   - All coins in a window tend to settle the same way, so 7 trades in one
#     window are really ~1 bet. Judge by number of WINDOWS, not trades.
#   - Do not draw conclusions before ~300+ windows (~3 days of 15m).
#
# ENV (required): TELEGRAM_TOKEN, TELEGRAM_CHAT_ID  (use a DIFFERENT bot token
#   from the favorite-dip bot — two programs can't share one bot's updates)
# ENV (optional): STAKE=5, TIMEFRAMES=15, COINS=BTC,ETH,SOL,DOGE,BNB,XRP,HYPE,
#   ENTRY_SECS_LEFT=90, MIN_SECS_LEFT=5, MIN_CENTS=50, MAX_CENTS=90, ONE_LOOK=true,
#   MAX_SPREAD_CENTS=100, FEE_RATE=0.07, POLL_SECS=1.5, SETTLE_POLL_SECS=15,
#   SETTLE_TIMEOUT_SECS=1800, DB_PATH=paper_last_minute.db, STATS_TZ=America/Thunder_Bay

import os, time, json, sqlite3, logging, threading, requests, csv, io, gzip
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
STAKE            = float(os.environ.get("STAKE", "5"))
DB_PATH          = os.environ.get("DB_PATH", "paper_last_minute.db")
TFS = [int(x) for x in os.environ.get("TIMEFRAMES", "15").split(",") if x.strip()]
COINS = [c.strip().upper() for c in
         os.environ.get("COINS", "BTC,ETH,SOL,DOGE,BNB,XRP,HYPE").split(",") if c.strip()]

ENTRY_SECS_LEFT  = float(os.environ.get("ENTRY_SECS_LEFT", "90"))
MIN_SECS_LEFT    = float(os.environ.get("MIN_SECS_LEFT", "5"))
MIN_CENTS        = float(os.environ.get("MIN_CENTS", "50"))
MAX_CENTS        = float(os.environ.get("MAX_CENTS", "90"))
MAX_SPREAD_CENTS = float(os.environ.get("MAX_SPREAD_CENTS", "100"))  # 100 = no filter
# ONE_LOOK=true: decide ONCE per market at the first check (≈ENTRY_SECS_LEFT).
# If the leader isn't in the price band right then, skip that market — no
# buying late wobbles. false = old behaviour (keep checking down to MIN_SECS_LEFT).
ONE_LOOK = os.environ.get("ONE_LOOK", "true").strip().lower() in ("1", "true", "yes", "on")
FEE_RATE         = float(os.environ.get("FEE_RATE", "0.07"))
POLL_SECS           = float(os.environ.get("POLL_SECS", "1.5"))
SETTLE_POLL_SECS    = float(os.environ.get("SETTLE_POLL_SECS", "15"))
SETTLE_TIMEOUT_SECS = float(os.environ.get("SETTLE_TIMEOUT_SECS", "1800"))
STATS_TZ = ZoneInfo(os.environ.get("STATS_TZ", "America/Thunder_Bay"))
# Shadow snapshots: at each of these seconds-left checkpoints, record what
# the leader costs for EVERY coin (no trade). After settlement we know which
# entry time actually works best. /timing shows it.
SNAP_SECS = sorted({int(x) for x in os.environ.get(
    "SNAP_SECS", "180,120,90,60,45,30,15").split(",") if x.strip()}, reverse=True)

# TRAILING-SIDE test (second paper strategy, separate table + /trailstats):
# with TRAIL_SECS_LEFT left, take ONE look at each market; if the side that is
# BEHIND costs TRAIL_MIN..TRAIL_MAX cents, paper-buy $STAKE of it (real book
# fill, real fee) and hold to settlement. Snapshot data (Oct 2-4) showed the
# trailing side at ~2 min left winning more often than its price.
TRAIL_ENABLED   = os.environ.get("TRAIL_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
TRAIL_SECS_LEFT = float(os.environ.get("TRAIL_SECS_LEFT", "120"))
TRAIL_MIN_CENTS = float(os.environ.get("TRAIL_MIN_CENTS", "10"))
TRAIL_MAX_CENTS = float(os.environ.get("TRAIL_MAX_CENTS", "50"))
# skip a trailing-side buy when that side's book is wider than this (cents)
TRAIL_MAX_SPREAD_CENTS = float(os.environ.get("TRAIL_MAX_SPREAD_CENTS", "8"))
TRAIL_TFS = [int(x) for x in os.environ.get("TRAIL_TIMEFRAMES", "15").split(",") if x.strip()]
TRAIL_COINS = [c.strip().upper() for c in
               os.environ.get("TRAIL_COINS", "BTC,ETH,SOL,DOGE,BNB,XRP").split(",") if c.strip()]

ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡",
               "BNB": "🟨", "XRP": "⚪", "HYPE": "🟢"}
ASSET_FULLNAME = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
                  "DOGE": "dogecoin", "BNB": "bnb", "XRP": "xrp", "HYPE": "hype"}
CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"
ET = ZoneInfo("America/New_York")
BINANCE_WS = ("wss://data-stream.binance.vision/stream?streams=" +
              "/".join(f"{s}usdt@bookTicker" for s in
                       ["btc", "eth", "sol", "doge", "bnb", "xrp"]))
BINANCE_SYM_TO_ASSET = {f"{s.upper()}USDT": s.upper()
                        for s in ["btc", "eth", "sol", "doge", "bnb", "xrp"]}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("paper-last-minute")

prices_ref = {}
ref_last = {}
muted = False


def money(x):
    return f"{'+' if x >= 0 else chr(0x2212)}${abs(x):.2f}"


def label_for(tf):
    return "4h" if tf == 240 else "1h" if tf == 60 else f"{tf}m"


def fee_for(shares, price_frac):
    return shares * FEE_RATE * price_frac * (1.0 - price_frac)


# ── reference feed (only used for fallback grading + analysis logging) ──────
def binance_ref_worker():
    while True:
        ws = None
        try:
            ws = websocket.create_connection(BINANCE_WS, timeout=10)
            ws.settimeout(30)
            log.info("[REF] Binance.vision reference feed connected")
            while True:
                msg = ws.recv()
                if not msg:
                    continue
                d = json.loads(msg).get("data", {})
                a = BINANCE_SYM_TO_ASSET.get(d.get("s"))
                if a:
                    b, k = float(d.get("b", 0)), float(d.get("a", 0))
                    if b > 0 and k > 0:
                        prices_ref[a] = (b + k) / 2.0
                        ref_last[a] = time.time()
        except Exception as e:
            log.warning(f"[REF] error: {e} — reconnecting")
        finally:
            try:
                ws and ws.close()
            except Exception:
                pass
        time.sleep(3)


# ── market plumbing (same as the favorite-dip bot) ──────────────────────────
def window_times(tf):
    now = time.time()
    if tf == 60:
        now_et = datetime.now(timezone.utc).astimezone(ET)
        o = int(now_et.replace(minute=0, second=0, microsecond=0).timestamp())
        return o, o + 3600, o + 3600 - now
    if tf == 240:
        now_et = datetime.now(timezone.utc).astimezone(ET)
        bh = (now_et.hour // 4) * 4
        o = int(now_et.replace(hour=bh, minute=0, second=0, microsecond=0).timestamp())
        return o, o + 14400, o + 14400 - now
    L = tf * 60
    o = int(now // L) * L
    return o, o + L, o + L - now


def build_slug(asset, tf, open_ts):
    if tf == 240:
        return f"{asset.lower()}-updown-4h-{open_ts}"
    if tf == 60:
        dt_et = datetime.fromtimestamp(open_ts, tz=ET)
        month = dt_et.strftime("%B").lower()
        hour12 = dt_et.strftime("%I").lstrip("0") or "12"
        ampm = dt_et.strftime("%p").lower()
        return (f"{ASSET_FULLNAME[asset]}-up-or-down-{month}-{dt_et.day}-"
                f"{dt_et.year}-{hour12}{ampm}-et")
    return f"{asset.lower()}-updown-{tf}m-{open_ts}"


_market_cache = {}

def resolve_tokens(asset, tf, open_ts):
    key = (asset, tf, open_ts)
    if key in _market_cache and _market_cache[key] is not None:
        return _market_cache[key]
    slug = build_slug(asset, tf, open_ts)
    try:
        r = requests.get(f"{GAMMA_BASE}/events", params={"slug": slug}, timeout=8)
        arr = r.json()
        ev = arr[0] if isinstance(arr, list) and arr else arr
        markets = ev.get("markets", []) if isinstance(ev, dict) else []
        if markets:
            toks = json.loads(markets[0].get("clobTokenIds", "[]"))
            if len(toks) == 2:
                _market_cache[key] = (toks[0], toks[1])
                return _market_cache[key]
    except Exception as e:
        log.debug(f"[RESOLVE] {slug}: {e}")
    return None   # not cached as None: retried next poll (market may appear late)


def get_book(token_id):
    """-> dict(bid, ask in cents, asks=[(price_cents, size_shares), ...] ascending)
    or None."""
    try:
        r = requests.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=6)
        b = r.json()
        asks = sorted((float(a["price"]) * 100.0, float(a["size"]))
                      for a in b.get("asks", []) if float(a.get("size", 0)) > 0)
        bids = [float(x["price"]) * 100.0 for x in b.get("bids", [])
                if float(x.get("size", 0)) > 0]
        if not asks or not bids:
            return None
        return {"ask": asks[0][0], "bid": max(bids), "asks": asks}
    except Exception:
        return None


def vwap_fill(asks, dollars):
    """Walk the ask book to spend `dollars`. -> (vwap_cents, shares, spent, levels)"""
    spent = shares = 0.0
    levels = 0
    for px, sz in asks:
        if spent >= dollars - 1e-9:
            break
        cost_lvl = px / 100.0 * sz
        take = min(cost_lvl, dollars - spent)
        shares += take / (px / 100.0)
        spent += take
        levels += 1
    if shares <= 0:
        return None
    return spent / shares * 100.0, shares, spent, levels


def fetch_polymarket_outcome(asset, tf, open_ts):
    slug = build_slug(asset, tf, open_ts)
    try:
        r = requests.get(f"{GAMMA_BASE}/events", params={"slug": slug}, timeout=10)
        data = r.json()
        if not data or not isinstance(data, list):
            return None
        markets = data[0].get("markets", [])
        if not markets:
            return None
        op = markets[0].get("outcomePrices")
        if isinstance(op, str):
            try:
                op = json.loads(op)
            except Exception:
                pass
        if not op or len(op) < 2:
            return None
        up_p, down_p = float(op[0]), float(op[1])
        if up_p >= 0.99:
            return "UP"
        if down_p >= 0.99:
            return "DOWN"
        return None
    except Exception as e:
        log.warning(f"[OUTCOME] {slug}: {e}")
        return None


# ── database ────────────────────────────────────────────────────────────────
COLS = """id INTEGER PRIMARY KEY AUTOINCREMENT, created TEXT, asset TEXT, tf INTEGER,
    direction TEXT, open_ts INTEGER, close_ts INTEGER, secs_left REAL,
    ask REAL, bid REAL, mid REAL, opp_ask REAL, opp_bid REAL,
    vwap REAL, shares REAL, stake REAL, fee REAL, levels INTEGER,
    top_size REAL, partial INTEGER, ref_open REAL, ref_entry REAL,
    result TEXT, graded_by TEXT, pnl_hold REAL, pnl_net REAL,
    peak_bid_after REAL, min_bid_after REAL"""


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(f"CREATE TABLE IF NOT EXISTS trades ({COLS})")
    conn.execute(f"CREATE TABLE IF NOT EXISTS trail ({COLS})")
    conn.execute("UPDATE trail SET result='VOID' WHERE result='PENDING'")
    for col in ("peak_bid_after REAL", "min_bid_after REAL"):
        try:
            conn.execute(f"ALTER TABLE trades ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass
    conn.execute("""CREATE TABLE IF NOT EXISTS snaps (
        id INTEGER PRIMARY KEY AUTOINCREMENT, created TEXT, asset TEXT, tf INTEGER,
        open_ts INTEGER, close_ts INTEGER, cp INTEGER, secs_left REAL,
        direction TEXT, ask REAL, bid REAL, opp_ask REAL, vwap REAL, shares REAL,
        stake REAL, fee REAL, result TEXT, pnl_net REAL)""")
    conn.execute("UPDATE trades SET result='VOID' WHERE result='PENDING'")
    conn.commit()
    conn.close()


def db_insert(row, table="trades"):
    conn = sqlite3.connect(DB_PATH)
    keys = list(row.keys())
    c = conn.cursor()
    c.execute(f"INSERT INTO {table} ({','.join(keys)}) VALUES ({','.join('?' * len(keys))})",
              [row[k] for k in keys])
    rid = c.lastrowid
    conn.commit()
    conn.close()
    return rid


def db_resolve(rid, result, graded_by, pnl_hold, pnl_net, peak=None, low=None,
               table="trades"):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(f"""UPDATE {table} SET result=?, graded_by=?, pnl_hold=?, pnl_net=?,
                    peak_bid_after=?, min_bid_after=? WHERE id=?""",
                 (result, graded_by, pnl_hold, pnl_net, peak, low, rid))
    conn.commit()
    conn.close()


def _since(mode):
    if mode == "all":
        return None
    now = datetime.now(STATS_TZ)
    return now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()


def db_rows(since_ts=None, table="trades"):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(f"SELECT * FROM {table} WHERE result IN ('WIN','LOSS')").fetchall()
    conn.close()
    if since_ts is not None:
        rows = [r for r in rows
                if datetime.fromisoformat(r["created"]).timestamp() >= since_ts]
    return rows


def stats_text(mode="today", table="trades"):
    rows = db_rows(_since(mode), table)
    scope = "all time" if mode == "all" else "today"
    head = (f"⏱ <b>LAST-MINUTE · PAPER</b> · {scope}" if table == "trades" else
            f"🔁 <b>TRAILING SIDE @{TRAIL_SECS_LEFT:.0f}s · PAPER</b> · {scope}")
    if not rows:
        return f"{head}\nno settled trades yet"
    n = len(rows)
    w = sum(1 for r in rows if r["result"] == "WIN")
    hold = sum(r["pnl_hold"] or 0 for r in rows)
    net = sum(r["pnl_net"] or 0 for r in rows)
    fees = sum(r["fee"] or 0 for r in rows)
    px = sum(r["vwap"] for r in rows) / n
    windows = len({(r["tf"], r["open_ts"]) for r in rows})
    lines = [head,
             f"{n} trades · {windows} windows · {w / n * 100:.1f}% win · avg @{px:.1f}¢",
             f"before fees {money(hold)} · <b>after fees {money(net)}</b>",
             f"fees paid ${fees:.2f}", "━━━━━━━━━━"]
    by_tf = {}
    for r in rows:
        by_tf.setdefault(r["tf"], []).append(r)
    for tf, rs in sorted(by_tf.items()):
        k = len(rs)
        lines.append(f"<b>{label_for(tf)}</b> {k} · "
                     f"{sum(1 for r in rs if r['result'] == 'WIN') / k * 100:.0f}% win · "
                     f"net {money(sum(r['pnl_net'] or 0 for r in rs))} "
                     f"({sum(r['pnl_net'] or 0 for r in rs) / k / STAKE * 100:+.1f}%/trade)")
    lines.append("━━━━━━━━━━")
    bands = (((50, 60), (60, 70), (70, 80), (80, 90.01)) if table == "trades" else
             ((0, 20), (20, 30), (30, 40), (40, 50.01)))
    for lo, hi in bands:
        rs = [r for r in rows if lo <= r["ask"] < hi]
        if rs:
            k = len(rs)
            wr = sum(1 for r in rs if r["result"] == "WIN") / k * 100
            avg = sum(r["vwap"] for r in rs) / k
            lines.append(f"{(str(lo) + 's') if table == 'trades' else (str(lo) + '–' + str(int(hi)) + '¢')}: {k} · won {wr:.0f}% vs paid {avg:.0f}¢ · "
                         f"net {money(sum(r['pnl_net'] or 0 for r in rs))}")
    slip = sum((r["vwap"] - r["ask"]) for r in rows) / n
    spr = sum((r["ask"] - r["bid"]) for r in rows) / n
    lines.append(f"avg spread {spr:.1f}¢ · avg fill above ask {slip:.2f}¢")
    return "\n".join(lines)


def build_csv(table="trades"):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(f"SELECT * FROM {table} ORDER BY id").fetchall()
    conn.close()
    if not rows:
        return None, 0
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(rows[0].keys())
    for r in rows:
        w.writerow(list(r))
    return buf.getvalue(), len(rows)


# ── coin price history (Binance 1-minute candles) for offline analysis ──────
BINANCE_REST = "https://data-api.binance.vision/api/v3/klines"
_prices_lock = threading.Lock()


def export_prices_worker(days):
    """1-minute candles for every Binance-listed coin (HYPE isn't on Binance)
    over the last `days` days -> gzipped CSV sent to Telegram."""
    if not _prices_lock.acquire(blocking=False):
        tg("⏳ price export already running", force=True)
        return
    try:
        tg(f"⏳ downloading {days} day(s) of 1-minute coin prices…", force=True)
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - int(days * 86400 * 1000)
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["asset", "t", "open", "high", "low", "close", "volume"])
        rows = 0
        for asset in [a for a in BINANCE_SYM_TO_ASSET.values()]:
            cur = start_ms
            while cur < end_ms:
                r = requests.get(BINANCE_REST, params={"symbol": f"{asset}USDT",
                                 "interval": "1m", "startTime": cur, "limit": 1000},
                                 timeout=15)
                k = r.json() if r.status_code == 200 else []
                if not k:
                    break
                for c in k:
                    w.writerow([asset, int(c[0]) // 1000, c[1], c[2], c[3], c[4], c[5]])
                rows += len(k)
                cur = int(k[-1][0]) + 60_000
                time.sleep(0.2)
        data = gzip.compress(buf.getvalue().encode())
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
                          data={"chat_id": TELEGRAM_CHAT_ID,
                                "caption": f"{rows} one-minute candles · {days} day(s)"},
                          files={"document": (f"coin_prices_{days}d.csv.gz", data,
                                              "application/gzip")}, timeout=300)
        if r.status_code != 200:
            tg(f"⚠️ price upload failed ({r.status_code})", force=True)
    except Exception as e:
        tg(f"⚠️ price export failed: {e}", force=True)
    finally:
        _prices_lock.release()


# ── telegram ────────────────────────────────────────────────────────────────
def tg(msg, force=False):
    if muted and not force:
        return
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                                "parse_mode": "HTML"}, timeout=8)
        if r.status_code != 200:
            log.error(f"[TG] {r.status_code}: {r.text[:150]}")
    except Exception as e:
        log.error(f"TG error: {e}")


def tg_document(filename, text, caption=""):
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
            data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:1000]},
            files={"document": (filename, text.encode("utf-8"), "text/csv")},
            timeout=120)
        if r.status_code != 200:
            tg(f"⚠️ upload failed ({r.status_code})", force=True)
    except Exception as e:
        tg(f"⚠️ upload failed: {e}", force=True)


# ── /backtest: replay old 5m markets from Polymarket's real trade history ────
# Tests the 5m 9–13¢ early-buy setup on past days. For every 5m market of a day it
# pulls the market's actual trades and finds the first moment (first BT_SECS)
# a side could be BOUGHT at BT_LO..BT_HI¢:
#   - someone bought that side at p    -> its ask was p
#   - someone sold the other side at q -> this side's ask was about 100-q
# then grades it with the real result. One buy per market, fees applied.
# Runs in the background; does not touch the live trading.
DATA_BASE = "https://data-api.polymarket.com"
BT_LO   = float(os.environ.get("BT_LO", "9"))
BT_HI   = float(os.environ.get("BT_HI", "13"))
BT_SECS = float(os.environ.get("BT_SECS", "60"))          # recorded window
BT_REPORT_SECS = float(os.environ.get("BT_REPORT_SECS", "45"))
BT_WORKERS = int(os.environ.get("BT_WORKERS", "6"))
BT_COINS = [c.strip().upper() for c in
            os.environ.get("BT_COINS", "BTC,ETH,SOL,DOGE,BNB,XRP,HYPE").split(",") if c.strip()]
bt_running = False


def bt_sql(sql, args=(), many=False, fetch=False):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.executemany(sql, args) if many else conn.execute(sql, args)
        rows = cur.fetchall() if fetch else None
        conn.commit()
        return rows
    finally:
        conn.close()


def bt_init():
    bt_sql("""CREATE TABLE IF NOT EXISTS bt (
        asset TEXT, tf INTEGER, open_ts INTEGER, outcome TEXT, ntrades INTEGER,
        hit_secs REAL, side TEXT, px REAL, src TEXT, size REAL, min_up REAL,
        min_dn REAL, status TEXT, PRIMARY KEY (asset, tf, open_ts))""")


def bt_get(url, params, tries=4):
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * (i + 1))
                continue
            return None
        except Exception:
            time.sleep(1.5 * (i + 1))
    return None


def bt_market(asset, open_ts):
    """-> dict(cond, up, dn, outcome) or None"""
    arr = bt_get(f"{GAMMA_BASE}/events", {"slug": build_slug(asset, 5, open_ts)})
    ev = arr[0] if isinstance(arr, list) and arr else None
    mk = (ev or {}).get("markets") or []
    if not mk:
        return None
    m = mk[0]
    try:
        toks = json.loads(m.get("clobTokenIds", "[]"))
        op = m.get("outcomePrices")
        op = json.loads(op) if isinstance(op, str) else op
        up_p, dn_p = float(op[0]), float(op[1])
    except Exception:
        return None
    outcome = "UP" if up_p >= 0.99 else "DOWN" if dn_p >= 0.99 else None
    return {"cond": m.get("conditionId"), "up": str(toks[0]) if toks else None,
            "dn": str(toks[1]) if len(toks) > 1 else None, "outcome": outcome}


def bt_trades(cond, open_ts):
    """taker trades in the first BT_SECS of the market, oldest first"""
    out, offset, lim = [], 0, 500
    while offset <= 20000:
        page = bt_get(f"{DATA_BASE}/trades", {"market": cond, "limit": lim,
                                              "offset": offset, "takerOnly": "true"})
        if not isinstance(page, list) or not page:
            break
        stamps = []
        for t in page:
            try:
                ts = float(t.get("timestamp", 0))
            except Exception:
                continue
            stamps.append(ts)
            if open_ts <= ts <= open_ts + BT_SECS:
                out.append(t)
        if len(page) < lim or (stamps and min(stamps) < open_ts):
            break
        offset += lim
    out.sort(key=lambda t: float(t.get("timestamp", 0)))
    return out


def bt_one(asset, open_ts):
    try:
        m = bt_market(asset, open_ts)
        if not m or not m["cond"]:
            return (asset, 5, open_ts, None, 0, None, None, None, None, None, None, None,
                    "nomarket")
        trs = bt_trades(m["cond"], open_ts)
        hit = None
        mins = {"UP": None, "DOWN": None}
        for t in trs:
            try:
                p = float(t["price"]) * 100.0
                sz = float(t.get("size", 0) or 0)
                ts = float(t["timestamp"])
            except Exception:
                continue
            tok = str(t.get("asset", ""))
            side_of = ("UP" if tok == m["up"] else "DOWN" if tok == m["dn"] else
                       ("UP" if str(t.get("outcomeIndex")) == "0" else "DOWN"))
            other = "DOWN" if side_of == "UP" else "UP"
            if str(t.get("side", "")).upper() == "BUY":
                s, ask, src = side_of, p, "buy"
            else:
                s, ask, src = other, 100.0 - p, "sell"
            ask = round(ask, 2)
            if ts - open_ts <= BT_REPORT_SECS and (mins[s] is None or ask < mins[s]):
                mins[s] = ask
            if hit is None and BT_LO <= ask <= BT_HI:
                hit = (round(ts - open_ts, 1), s, ask, src, sz)
        st = "ok" if m["outcome"] else "unresolved"
        h = hit or (None, None, None, None, None)
        return (asset, 5, open_ts, m["outcome"], len(trs), *h, mins["UP"], mins["DOWN"], st)
    except Exception as e:
        log.warning(f"[BT] {asset} {open_ts}: {e}")
        return (asset, 5, open_ts, None, 0, None, None, None, None, None, None, None, "error")


def bt_day_opens(day):
    d0 = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=STATS_TZ)
    start = int(d0.timestamp())
    return [start + 300 * i for i in range(288)]


def bt_summary(rows, extra_cents=0.0):
    res = {"all": [0, 0, 0.0], "nobnb": [0, 0, 0.0]}
    for r in rows:
        if r["hit_secs"] is None or r["hit_secs"] > BT_REPORT_SECS or not r["outcome"]:
            continue
        p = (r["px"] + extra_cents) / 100.0
        sh = STAKE / p
        win = r["side"] == r["outcome"]
        net = (sh - STAKE if win else -STAKE) - fee_for(sh, p)
        for k in ("all", "nobnb"):
            if k == "nobnb" and r["asset"] == "BNB":
                continue
            res[k][0] += 1
            res[k][1] += int(win)
            res[k][2] += net
    return res


def bt_run(days):
    global bt_running
    try:
        bt_init()
        lines = []
        for day in days:
            opens = [o for o in bt_day_opens(day) if o + 300 < time.time()]
            if not opens:
                continue
            done = {(r["asset"], r["open_ts"]) for r in bt_sql(
                "SELECT asset, open_ts FROM bt WHERE tf=5 AND status='ok' "
                "AND open_ts>=? AND open_ts<=?", (opens[0], opens[-1]), fetch=True)}
            jobs = [(a, o) for o in opens for a in BT_COINS if (a, o) not in done]
            t0 = time.time()
            with ThreadPoolExecutor(max_workers=BT_WORKERS) as ex:
                got = list(ex.map(lambda j: bt_one(*j), jobs))
            if got:
                bt_sql("INSERT OR REPLACE INTO bt VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                       got, many=True)
            rows = bt_sql("SELECT * FROM bt WHERE tf=5 AND open_ts>=? AND open_ts<=?",
                          (opens[0], opens[-1]), fetch=True)
            ok = sum(1 for r in rows if r["status"] == "ok")
            s0, s1 = bt_summary(rows), bt_summary(rows, 1.0)
            a, nb, nb1 = s0["all"], s0["nobnb"], s1["nobnb"]
            line = (f"<b>{day}</b> · {ok} mkts\n"
                    f"  no BNB: {nb[0]} trades · {nb[1]} wins · <b>{money(nb[2])}</b> "
                    f"(1¢ worse fills {money(nb1[2])})\n"
                    f"  all coins: {a[0]} · {a[1]} wins · {money(a[2])}")
            lines.append(line)
            tg(f"🧪 {line}\n  ({time.time() - t0:.0f}s)", force=True)
        tg("🧪 <b>BACKTEST done</b> · 5m {:g}–{:g}¢ · first {:g}s · ${:g}/trade · after fees\n\n"
           .format(BT_LO, BT_HI, BT_REPORT_SECS, STAKE) + "\n".join(lines) +
           "\n\n/exportbt for the file", force=True)
    except Exception as e:
        tg(f"⚠️ backtest failed: {e}", force=True)
    finally:
        bt_running = False


def bt_start(args):
    global bt_running
    if bt_running:
        tg("🧪 a backtest is already running", force=True)
        return
    try:
        d1 = datetime.strptime(args[0], "%Y-%m-%d").date()
        d2 = datetime.strptime(args[1], "%Y-%m-%d").date() if len(args) > 1 else d1
    except Exception:
        tg("usage: <code>/backtest 2026-10-04</code> or "
           "<code>/backtest 2026-09-20 2026-10-05</code>", force=True)
        return
    if d2 < d1 or (d2 - d1).days > 30:
        tg("pick up to 31 days, start date first", force=True)
        return
    days = [(d1 + timedelta(days=i)).isoformat() for i in range((d2 - d1).days + 1)]
    bt_running = True
    tg(f"🧪 backtest started: {len(days)} day(s) · 5m {BT_LO:g}–{BT_HI:g}¢ · "
       f"first {BT_REPORT_SECS:g}s · a few minutes per day · live bot keeps running",
       force=True)
    threading.Thread(target=bt_run, args=(days,), daemon=True).start()


def bt_export():
    bt_init()
    rs = bt_sql("SELECT * FROM bt ORDER BY open_ts, asset", fetch=True)
    if not rs:
        tg("no backtest data yet — try /backtest 2026-10-04", force=True)
        return
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(rs[0].keys())
    w.writerows([list(r) for r in rs])
    tg_document("lastmin_backtest.csv", buf.getvalue(), f"{len(rs)} markets")


# ── 5m depth recorder (side test for the 5m 9–13¢ longshot idea) ──────────────
# Every DEPTH_STEP seconds in the first DEPTH_UNTIL seconds of each 5m market,
# saves the FULL ask book of the cheap side (when its ask <= DEPTH_MAX_CENTS) and
# what $1/$2/$3/$5/$10/$20/$50 would really cost. Records only — never trades.
DEPTH_ENABLED  = os.environ.get("DEPTH_ENABLED", "true").lower() in ("1", "true", "yes")
DEPTH_COINS    = [c.strip().upper() for c in
                  os.environ.get("DEPTH_COINS", "BTC,ETH,SOL,DOGE,BNB,XRP,HYPE").split(",") if c.strip()]
DEPTH_MAX_CENTS = float(os.environ.get("DEPTH_MAX_CENTS", "25"))
DEPTH_STEP     = int(os.environ.get("DEPTH_STEP", "10"))
DEPTH_UNTIL    = int(os.environ.get("DEPTH_UNTIL", "60"))
DEPTH_STAKES   = [1, 2, 3, 5, 10, 20, 50]
depth_done = set()


def depth_init():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("""CREATE TABLE IF NOT EXISTS depth5 (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, asset TEXT, open_ts INTEGER,
        secs_into REAL, side TEXT, ask REAL, bid REAL, opp_ask REAL, opp_bid REAL,
        sh_1c REAL, sh_2c REAL, sh_5c REAL, book_shares REAL,
        v1 REAL, v2 REAL, v3 REAL, v5 REAL, v10 REAL, v20 REAL, v50 REAL,
        asks TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS depth5_mkts (
        asset TEXT, open_ts INTEGER, outcome TEXT, PRIMARY KEY (asset, open_ts))""")
    conn.commit()
    conn.close()


def depth_one(asset, open_ts, secs_into):
    tk = resolve_tokens(asset, 5, open_ts)
    if not tk:
        return None
    up, dn = get_book(tk[0]), get_book(tk[1])
    if not up or not dn:
        return None
    side, bk, ot = ("UP", up, dn) if up["ask"] <= dn["ask"] else ("DOWN", dn, up)
    if bk["ask"] > DEPTH_MAX_CENTS:
        return None
    a0 = bk["ask"]
    vw = []
    for st in DEPTH_STAKES:
        f = vwap_fill(bk["asks"], st)
        # None if the whole book can't fill this stake
        vw.append(round(f[0], 4) if f and f[2] >= st - 1e-6 else None)
    lv = [(round(p, 2), round(s, 2)) for p, s in bk["asks"] if p <= a0 + 30][:60]
    return (time.time(), asset, open_ts, round(secs_into, 1), side, a0, bk["bid"],
            ot["ask"], ot["bid"],
            round(sum(s for p, s in bk["asks"] if p <= a0 + 1), 1),
            round(sum(s for p, s in bk["asks"] if p <= a0 + 2), 1),
            round(sum(s for p, s in bk["asks"] if p <= a0 + 5), 1),
            round(sum(s for p, s in bk["asks"]), 1), *vw, json.dumps(lv))


def depth_worker():
    depth_init()
    last_grade = 0
    while True:
        try:
            time.sleep(1)
            now = time.time()
            o = int(now // 300) * 300
            into = now - o
            step = int(into // DEPTH_STEP) * DEPTH_STEP
            if into <= DEPTH_UNTIL + 2 and (o, step) not in depth_done:
                depth_done.add((o, step))
                futs = [_pool.submit(depth_one, a, o, into) for a in DEPTH_COINS]
                rows = [r for r in (f.result() for f in futs) if r]
                if rows:
                    conn = sqlite3.connect(DB_PATH, timeout=30)
                    conn.executemany(f"INSERT INTO depth5 (ts,asset,open_ts,secs_into,side,ask,bid,"
                                     f"opp_ask,opp_bid,sh_1c,sh_2c,sh_5c,book_shares,v1,v2,v3,v5,"
                                     f"v10,v20,v50,asks) VALUES ({','.join('?' * 21)})", rows)
                    conn.commit()
                    conn.close()
            if len(depth_done) > 2000:
                depth_done.clear()
            if now - last_grade > 60:
                last_grade = now
                conn = sqlite3.connect(DB_PATH, timeout=30)
                todo = conn.execute("""SELECT DISTINCT d.asset, d.open_ts FROM depth5 d
                    LEFT JOIN depth5_mkts m ON m.asset=d.asset AND m.open_ts=d.open_ts
                    WHERE m.outcome IS NULL AND d.open_ts < ? AND d.open_ts > ?""",
                    (now - 360, now - 86400 * 3)).fetchall()
                conn.close()
                got = []
                for a, ot in todo[:60]:
                    res = fetch_polymarket_outcome(a, 5, ot)
                    if res:
                        got.append((a, ot, res))
                if got:
                    conn = sqlite3.connect(DB_PATH, timeout=30)
                    conn.executemany("INSERT OR REPLACE INTO depth5_mkts VALUES (?,?,?)", got)
                    conn.commit()
                    conn.close()
        except Exception as e:
            log.error(f"[DEPTH] {e}")


def depth_export(days):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    rs = conn.execute("""SELECT d.*, m.outcome FROM depth5 d LEFT JOIN depth5_mkts m
                         ON m.asset=d.asset AND m.open_ts=d.open_ts
                         WHERE d.ts >= ? ORDER BY d.id""", (time.time() - days * 86400,)).fetchall()
    conn.close()
    if not rs:
        tg("no depth data yet", force=True)
        return
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(rs[0].keys())
    w.writerows([list(r) for r in rs])
    data = gzip.compress(buf.getvalue().encode())
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
                          data={"chat_id": TELEGRAM_CHAT_ID,
                                "caption": f"{len(rs)} depth snapshots · {days:g} day(s)"},
                          files={"document": (f"lastmin_depth_{days:g}d.csv.gz", data,
                                              "application/gzip")}, timeout=300)
        if r.status_code != 200:
            tg(f"⚠️ upload failed ({r.status_code}) — try fewer days", force=True)
    except Exception as e:
        tg(f"⚠️ upload failed: {e}", force=True)


_upd = None

def handle_commands():
    global _upd, muted
    try:
        p = {"timeout": 1}
        if _upd:
            p["offset"] = _upd
        for u in requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                              params=p, timeout=5).json().get("result", []):
            _upd = u["update_id"] + 1
            if str(u.get("message", {}).get("chat", {}).get("id")) != str(TELEGRAM_CHAT_ID):
                continue
            parts = u.get("message", {}).get("text", "").strip().lower().split()
            if not parts:
                continue
            t = parts[0].split("@")[0]
            arg = parts[1] if len(parts) > 1 else ""
            if t == "/stats":
                tg(stats_text("all" if arg == "all" else "today"), force=True)
            elif t == "/status":
                with pending_lock:
                    snap = list(pending)
                if not snap:
                    tg("⏱ no open positions", force=True)
                else:
                    lines = [f"⏱ <b>{len(snap)} open</b>"]
                    for s in snap:
                        lines.append(f"{ASSET_EMOJI.get(s['asset'], '')}{s['asset']} "
                                     f"{label_for(s['tf'])} "
                                     f"{'↑' if s['direction'] == 'UP' else '↓'} "
                                     f"@{s['vwap']:.0f}¢")
                    tg("\n".join(lines), force=True)
            elif t == "/export":
                text, n = build_csv()
                if text:
                    tg_document("lastmin_trades.csv", text, f"{n} trades")
                else:
                    tg("no trades yet", force=True)
            elif t == "/trailstats":
                tg(stats_text("all" if arg == "all" else "today", "trail"), force=True)
            elif t == "/exporttrail":
                text, n = build_csv("trail")
                if text:
                    tg_document("trail_trades.csv", text, f"{n} trailing-side trades")
                else:
                    tg("no trailing-side trades yet", force=True)
            elif t == "/timing":
                tg(timing_text(), force=True)
            elif t == "/exportsnaps":
                conn = sqlite3.connect(DB_PATH)
                conn.row_factory = sqlite3.Row
                rs = conn.execute("SELECT * FROM snaps ORDER BY id").fetchall()
                conn.close()
                if rs:
                    buf = io.StringIO()
                    w = csv.writer(buf)
                    w.writerow(rs[0].keys())
                    w.writerows([list(r) for r in rs])
                    tg_document("lastmin_snaps.csv", buf.getvalue(), f"{len(rs)} snapshots")
                else:
                    tg("no snapshots yet", force=True)
            elif t == "/exportprices":
                try:
                    days = max(1, min(30, int(arg))) if arg else 3
                except ValueError:
                    days = 3
                threading.Thread(target=export_prices_worker, args=(days,),
                                 daemon=True).start()
            elif t == "/exportdepth":
                try:
                    days = max(0.1, min(7.0, float(arg))) if arg else 1.0
                except ValueError:
                    days = 1.0
                threading.Thread(target=depth_export, args=(days,), daemon=True).start()
            elif t == "/backtest":
                bt_start(parts[1:])
            elif t == "/exportbt":
                bt_export()
            elif t == "/mute":
                muted = True
                tg("🔇 muted — still trading and recording. /unmute to turn messages back on",
                   force=True)
            elif t == "/unmute":
                muted = False
                tg("🔔 unmuted")
            elif t == "/help":
                tg("📖 <b>commands</b>\n/stats — today (after fees)\n/stats all — all time\n"
                   "/status — open positions\n/export — all trades as CSV\n"
                   "/exportprices [days] — 1-minute coin prices (default 3 days)\n"
                   "/timing — which entry time works best (shadow test)\n"
                   "/exportsnaps — the timing snapshots as CSV\n"
                   "/trailstats [all] — trailing-side test (buys the side BEHIND at ~2 min left)\n"
                   "/exporttrail — trailing-side trades as CSV\n"
                   "/backtest 2026-10-04 [2026-10-05] — replay 5m 9–13¢ from real trade history\n"
                   "/exportbt — backtest results as CSV\n"
                   "/exportdepth [days] — 5m cheap-side order books (default 1 day)\n"
                   "/mute · /unmute — window messages off/on\n/help — this list",
                   force=True)
    except Exception:
        pass


# ── trading loop ────────────────────────────────────────────────────────────
entered = set()       # (asset, tf, open_ts)  — bought, or already looked (ONE_LOOK)
open_ref = {}         # (tf, open_ts) -> {asset: ref price at window start}
pending = []
pending_lock = threading.Lock()
_pool = ThreadPoolExecutor(max_workers=16)


def try_entries(tf, open_ts, close_ts, secs_left):
    todo = [a for a in COINS if (a, tf, open_ts) not in entered]
    if not todo:
        return
    toks = {a: resolve_tokens(a, tf, open_ts) for a in todo}
    jobs = {}
    for a, tk in toks.items():
        if tk:
            jobs[(a, "UP")] = _pool.submit(get_book, tk[0])
            jobs[(a, "DOWN")] = _pool.submit(get_book, tk[1])
    for a in todo:
        tk = toks.get(a)
        if not tk:
            continue
        up, dn = jobs[(a, "UP")].result(), jobs[(a, "DOWN")].result()
        if not up or not dn:
            continue          # no book yet — try again next poll
        if ONE_LOOK:
            entered.add((a, tf, open_ts))   # this is our one look at this market
        mid_up = (up["bid"] + up["ask"]) / 2
        mid_dn = (dn["bid"] + dn["ask"]) / 2
        direction, lead, opp, tok = (("UP", up, dn, tk[0]) if mid_up >= mid_dn
                                     else ("DOWN", dn, up, tk[1]))
        if not (MIN_CENTS <= lead["ask"] <= MAX_CENTS):
            continue          # ONE_LOOK: skip this market · else keep watching
        if lead["ask"] - lead["bid"] > MAX_SPREAD_CENTS:
            continue
        fill = vwap_fill(lead["asks"], STAKE)
        if not fill:
            continue
        vwap, shares, spent, levels = fill
        fee = fee_for(shares, vwap / 100.0)
        entered.add((a, tf, open_ts))
        row = dict(created=datetime.now(timezone.utc).isoformat(), asset=a, tf=tf,
                   direction=direction, open_ts=open_ts, close_ts=close_ts,
                   secs_left=round(secs_left, 1), ask=lead["ask"], bid=lead["bid"],
                   mid=(lead["ask"] + lead["bid"]) / 2, opp_ask=opp["ask"],
                   opp_bid=opp["bid"], vwap=round(vwap, 4), shares=round(shares, 6),
                   stake=round(spent, 6), fee=round(fee, 6), levels=levels,
                   top_size=lead["asks"][0][1], partial=int(spent < STAKE - 1e-6),
                   ref_open=open_ref.get((tf, open_ts), {}).get(a),
                   ref_entry=prices_ref.get(a), result="PENDING")
        rid = db_insert(row)
        with pending_lock:
            pending.append(dict(row, rid=rid, token=tok,
                                peak_bid=lead["bid"], min_bid=lead["bid"]))
        log.info(f"[ENTRY] {a} {label_for(tf)} {direction} vwap {vwap:.2f}c "
                 f"(ask {lead['ask']:.0f}) {secs_left:.0f}s left fee ${fee:.3f}")


trail_done = set()     # (tf, open_ts) — one look per window


def try_trail(tf, open_ts, close_ts, secs_left):
    """One look per market: buy the side that is BEHIND if it costs TRAIL_MIN..MAX."""
    toks = {a: resolve_tokens(a, tf, open_ts) for a in TRAIL_COINS}
    jobs = {a: (_pool.submit(get_book, tk[0]), _pool.submit(get_book, tk[1]))
            for a, tk in toks.items() if tk}
    for a, (fu, fd) in jobs.items():
        up, dn = fu.result(), fd.result()
        if not up or not dn:
            continue
        tk = toks[a]
        lead_up = (up["bid"] + up["ask"]) >= (dn["bid"] + dn["ask"])
        direction, trail, lead, tok = (("DOWN", dn, up, tk[1]) if lead_up
                                       else ("UP", up, dn, tk[0]))
        if not (TRAIL_MIN_CENTS <= trail["ask"] <= TRAIL_MAX_CENTS):
            continue
        if trail["ask"] - trail["bid"] > TRAIL_MAX_SPREAD_CENTS:
            log.info(f"[TRAIL] skip {a}: spread {trail['ask'] - trail['bid']:.1f}c")
            continue
        fill = vwap_fill(trail["asks"], STAKE)
        if not fill:
            continue
        vwap, shares, spent, levels = fill
        fee = fee_for(shares, vwap / 100.0)
        row = dict(created=datetime.now(timezone.utc).isoformat(), asset=a, tf=tf,
                   direction=direction, open_ts=open_ts, close_ts=close_ts,
                   secs_left=round(secs_left, 1), ask=trail["ask"], bid=trail["bid"],
                   mid=(trail["ask"] + trail["bid"]) / 2, opp_ask=lead["ask"],
                   opp_bid=lead["bid"], vwap=round(vwap, 4), shares=round(shares, 6),
                   stake=round(spent, 6), fee=round(fee, 6), levels=levels,
                   top_size=trail["asks"][0][1], partial=int(spent < STAKE - 1e-6),
                   ref_open=open_ref.get((tf, open_ts), {}).get(a),
                   ref_entry=prices_ref.get(a), result="PENDING")
        rid = db_insert(row, "trail")
        with pending_lock:
            pending.append(dict(row, rid=rid, token=tok, table="trail",
                                peak_bid=trail["bid"], min_bid=trail["bid"]))
        log.info(f"[TRAIL] {a} {label_for(tf)} {direction} vwap {vwap:.2f}c "
                 f"{secs_left:.0f}s left fee ${fee:.3f}")


def track_bids():
    """Record highest/lowest bid after entry for every open position, so any
    ladder / early-exit idea can be tested later on real bids. Never trades."""
    now = time.time()
    with pending_lock:
        live = [s for s in pending if now < s["close_ts"]]
    if not live:
        return
    futs = [(s, _pool.submit(get_book, s["token"])) for s in live]
    for s, f in futs:
        bk = f.result()
        if not bk:
            continue
        s["peak_bid"] = max(s["peak_bid"], bk["bid"])
        s["min_bid"] = min(s["min_bid"], bk["bid"])


snap_done = set()      # (tf, open_ts, cp)
snap_markets = {}      # (asset, tf, open_ts) -> close_ts, awaiting outcome
snap_lock = threading.Lock()


def take_snapshots(tf, open_ts, close_ts, secs_left, cp):
    toks = {a: resolve_tokens(a, tf, open_ts) for a in COINS}
    jobs = {}
    for a, tk in toks.items():
        if tk:
            jobs[a] = (_pool.submit(get_book, tk[0]), _pool.submit(get_book, tk[1]))
    rows = []
    for a, (fu, fd) in jobs.items():
        up, dn = fu.result(), fd.result()
        if not up or not dn:
            continue
        direction, lead, opp = (("UP", up, dn) if (up["bid"] + up["ask"]) >=
                                (dn["bid"] + dn["ask"]) else ("DOWN", dn, up))
        fill = vwap_fill(lead["asks"], STAKE)
        if not fill:
            continue
        vwap, shares, spent, _lv = fill
        rows.append((datetime.now(timezone.utc).isoformat(), a, tf, open_ts, close_ts,
                     cp, round(secs_left, 1), direction, lead["ask"], lead["bid"],
                     opp["ask"], round(vwap, 4), round(shares, 6), round(spent, 6),
                     round(fee_for(shares, vwap / 100.0), 6), "PENDING"))
    if not rows:
        return
    conn = sqlite3.connect(DB_PATH)
    conn.executemany("""INSERT INTO snaps (created,asset,tf,open_ts,close_ts,cp,secs_left,
        direction,ask,bid,opp_ask,vwap,shares,stake,fee,result)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
    conn.commit()
    conn.close()
    with snap_lock:
        for r in rows:
            snap_markets[(r[1], tf, open_ts)] = close_ts


def snap_scorer():
    """Grade every recorded snapshot once its market settles."""
    while True:
        try:
            time.sleep(SETTLE_POLL_SECS)
            now = time.time()
            with snap_lock:
                due = [(k, c) for k, c in snap_markets.items() if now >= c + 2]
            for (asset, tf, open_ts), close_ts in due:
                outcome = fetch_polymarket_outcome(asset, tf, open_ts)
                conn = sqlite3.connect(DB_PATH)
                if outcome is None:
                    if now <= close_ts + SETTLE_TIMEOUT_SECS:
                        conn.close()
                        continue
                    conn.execute("""UPDATE snaps SET result='VOID', pnl_net=0
                                    WHERE asset=? AND tf=? AND open_ts=?""",
                                 (asset, tf, open_ts))
                else:
                    conn.execute("""UPDATE snaps SET
                        result = CASE WHEN direction=? THEN 'WIN' ELSE 'LOSS' END,
                        pnl_net = CASE WHEN direction=? THEN shares - stake - fee
                                       ELSE -stake - fee END
                        WHERE asset=? AND tf=? AND open_ts=? AND result='PENDING'""",
                                 (outcome, outcome, asset, tf, open_ts))
                conn.commit()
                conn.close()
                with snap_lock:
                    snap_markets.pop((asset, tf, open_ts), None)
        except Exception as e:
            log.error(f"[SNAPS] {e}")


def timing_text():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""SELECT tf, cp, result, ask, vwap, pnl_net, open_ts FROM snaps
                           WHERE result IN ('WIN','LOSS') AND ask BETWEEN ? AND ?""",
                        (MIN_CENTS, MAX_CENTS)).fetchall()
    conn.close()
    if not rows:
        return "⏱ <b>ENTRY TIMING</b>\nno settled snapshots yet"
    out = {}
    for tf, cp, res, ask, vwap, net, o in rows:
        d = out.setdefault((tf, cp), {"n": 0, "w": 0, "px": 0.0, "net": 0.0, "win": set()})
        d["n"] += 1
        d["w"] += res == "WIN"
        d["px"] += vwap
        d["net"] += net
        d["win"].add(o)
    lines = [f"⏱ <b>ENTRY TIMING</b> (shadow, all time · leader {MIN_CENTS:.0f}–"
             f"{MAX_CENTS:.0f}¢ · $5 · after fees)"]
    for tf in sorted({k[0] for k in out}):
        lines.append(f"━━ <b>{label_for(tf)}</b>")
        for cp in sorted({k[1] for k in out if k[0] == tf}, reverse=True):
            d = out[(tf, cp)]
            n = d["n"]
            lines.append(f"{cp:>3d}s left: {n} · {len(d['win'])} windows · {d['w'] / n * 100:.0f}% won vs "
                         f"paid {d['px'] / n:.0f}¢ · net {money(d['net'])}")
    return "\n".join(lines)


def monitor():
    while True:
        try:
            time.sleep(POLL_SECS)
            for tf in sorted(set(TFS) | (set(TRAIL_TFS) if TRAIL_ENABLED else set())):
                open_ts, close_ts, secs_left = window_times(tf)
                if (tf, open_ts) not in open_ref:
                    open_ref[(tf, open_ts)] = dict(prices_ref)
                    cutoff = time.time() - 6 * 3600
                    for k in [k for k in open_ref if k[1] < cutoff]:
                        del open_ref[k]
                    entered.difference_update([k for k in entered if k[2] < cutoff])
                    trail_done.difference_update([k for k in trail_done if k[1] < cutoff])
                if tf in TFS and MIN_SECS_LEFT <= secs_left <= ENTRY_SECS_LEFT:
                    try_entries(tf, open_ts, close_ts, secs_left)
                if (TRAIL_ENABLED and tf in TRAIL_TFS and (tf, open_ts) not in trail_done
                        and TRAIL_SECS_LEFT - 10 < secs_left <= TRAIL_SECS_LEFT):
                    trail_done.add((tf, open_ts))
                    try_trail(tf, open_ts, close_ts, secs_left)
                for cp in (SNAP_SECS if tf in TFS else []):
                    key = (tf, open_ts, cp)
                    if key not in snap_done and cp - 10 < secs_left <= cp:
                        snap_done.add(key)
                        take_snapshots(tf, open_ts, close_ts, secs_left, cp)
                if len(snap_done) > 5000:
                    old = time.time() - 6 * 3600
                    snap_done.difference_update([k for k in snap_done if k[1] < old])
            track_bids()
        except Exception as e:
            log.error(f"[MONITOR] {e}")


def window_summary(tf, open_ts):
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""SELECT result, pnl_net FROM trades WHERE tf=? AND open_ts=?
                           AND result IN ('WIN','LOSS')""", (tf, open_ts)).fetchall()
    conn.close()
    if not rows:
        return
    w = sum(1 for r in rows if r[0] == "WIN")
    net = sum(r[1] or 0 for r in rows)
    day = sum(r["pnl_net"] or 0 for r in db_rows(_since("today")))
    t = datetime.fromtimestamp(open_ts, tz=STATS_TZ).strftime("%H:%M")
    tg(f"{'✅' if net >= 0 else '❌'} {label_for(tf)} {t} · {w}/{len(rows)} won · "
       f"{money(net)}\n━ today {money(day)} after fees")


def scorer():
    while True:
        try:
            time.sleep(1.0)
            now = time.time()
            with pending_lock:
                items = list(pending)
            for s in items:
                if now < s["close_ts"] + 2 or now - s.get("last_chk", 0) < SETTLE_POLL_SECS:
                    continue
                s["last_chk"] = now
                outcome = fetch_polymarket_outcome(s["asset"], s["tf"], s["open_ts"])
                graded = "settlement"
                if outcome is None:
                    if now <= s["close_ts"] + SETTLE_TIMEOUT_SECS:
                        continue
                    op, px = s.get("ref_open"), prices_ref.get(s["asset"])
                    if op is None or px is None or abs((px - op) / op) < 1e-6:
                        db_resolve(s["rid"], "VOID", "none", 0, 0,
                                   s.get("peak_bid"), s.get("min_bid"),
                                   s.get("table", "trades"))
                        with pending_lock:
                            s in pending and pending.remove(s)
                        continue
                    outcome = "UP" if px > op else "DOWN"
                    graded = "feed-fallback"
                won = s["direction"] == outcome
                hold = (s["shares"] - s["stake"]) if won else -s["stake"]
                net = hold - s["fee"]
                db_resolve(s["rid"], "WIN" if won else "LOSS", graded,
                           round(hold, 6), round(net, 6),
                           s.get("peak_bid"), s.get("min_bid"),
                           s.get("table", "trades"))
                with pending_lock:
                    s in pending and pending.remove(s)
                    left = any(p["tf"] == s["tf"] and p["open_ts"] == s["open_ts"]
                               and p.get("table", "trades") == "trades" for p in pending)
                if not left and s.get("table", "trades") == "trades":
                    window_summary(s["tf"], s["open_ts"])
        except Exception as e:
            log.error(f"[SCORER] {e}")


def main():
    if not WEBSOCKET_AVAILABLE:
        log.error("websocket-client not installed")
        return
    init_db()
    threading.Thread(target=binance_ref_worker, daemon=True).start()
    tg(f"⏱ <b>LAST-MINUTE LEADER · PAPER</b> — no money\n"
       + (f"one look at {ENTRY_SECS_LEFT:.0f}s left: buys the leader if its ask is "
          if ONE_LOOK else f"buys the leading side in the last {ENTRY_SECS_LEFT:.0f}s if its ask is ")
       + f"{MIN_CENTS:.0f}–{MAX_CENTS:.0f}¢ · holds to settle\n"
       f"fills walk the real book · {FEE_RATE:.0%} taker-fee formula applied\n"
       f"tf={','.join(label_for(t) for t in TFS)} · {len(COINS)} coins · ${STAKE:g}/trade\n"
       + (f"🔁 trailing-side test ON: {TRAIL_SECS_LEFT:.0f}s left · behind side "
          f"{TRAIL_MIN_CENTS:.0f}–{TRAIL_MAX_CENTS:.0f}¢ · spread ≤{TRAIL_MAX_SPREAD_CENTS:.0f}¢ · "
          f"{len(TRAIL_COINS)} coins · /trailstats\n"
          if TRAIL_ENABLED else "")
       + (f"📚 5m depth recorder ON: cheap side ≤{DEPTH_MAX_CENTS:.0f}¢, first {DEPTH_UNTIL}s · /exportdepth\n"
          if DEPTH_ENABLED else "")
       + "/stats · /help", force=True)
    threading.Thread(target=monitor, daemon=True).start()
    threading.Thread(target=scorer, daemon=True).start()
    threading.Thread(target=snap_scorer, daemon=True).start()
    if DEPTH_ENABLED:
        threading.Thread(target=depth_worker, daemon=True).start()
    while True:
        try:
            handle_commands()
        except Exception as e:
            log.error(f"main: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
