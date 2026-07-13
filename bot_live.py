#!/usr/bin/env python3
# PAPER LONGSHOT — buys 1-5c deep-underdog side early in each window (no money)
# Supports 5m, 15m, and 1h (60) timeframes. 5m/15m use timestamp slugs; hourly
# uses the date-based slug (bitcoin-up-or-down-july-12-2026-8pm-et). A startup
# probe confirms hourly markets resolve before trusting them.
import os, time, json, sqlite3, logging, threading, requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
PAPER_STAKE      = float(os.environ.get("PAPER_STAKE", "5"))
TAKER_MAX_ASK_CENTS = float(os.environ.get("TAKER_MAX_ASK_CENTS", "5"))
TAKER_MIN_ASK_CENTS = float(os.environ.get("TAKER_MIN_ASK_CENTS", "1"))
DB_PATH          = os.environ.get("DB_PATH", "paper_longshot.db")
SEND_EACH        = os.environ.get("SEND_EACH", "true").lower() == "true"
SETTLE_POLL_SECS    = float(os.environ.get("SETTLE_POLL_SECS", "15"))
SETTLE_TIMEOUT_SECS = float(os.environ.get("SETTLE_TIMEOUT_SECS", "1800"))
MAX_STACK           = int(os.environ.get("MAX_STACK", "1"))
# Timeframes: 5, 15, 60 (60 = hourly). 30 is NOT a real Polymarket timeframe.
TFS              = [int(x) for x in os.environ.get("TIMEFRAMES", "5,15,60").split(",")]
# Entry windows per timeframe (seconds from window open).
ENTRY_FIRST_SECS     = float(os.environ.get("ENTRY_FIRST_SECS", "60"))       # 5m: 1 min
ENTRY_FIRST_SECS_15M = float(os.environ.get("ENTRY_FIRST_SECS_15M", "180"))  # 15m: 3 min
ENTRY_FIRST_SECS_60M = float(os.environ.get("ENTRY_FIRST_SECS_60M", "900"))  # 1h: 15 min
ENTRY_FIRST_SECS_240M = float(os.environ.get("ENTRY_FIRST_SECS_240M", "3600"))  # 4h: first hour
OPEN_CAPTURE_GRACE   = float(os.environ.get("OPEN_CAPTURE_GRACE", "3"))

ASSET_LIST = ["BTC", "ETH", "SOL", "DOGE", "BNB", "XRP", "HYPE"]
ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡",
               "BNB": "🟨", "XRP": "⚪", "HYPE": "🟢"}
ASSET_FULLNAME = {"BTC":"bitcoin","ETH":"ethereum","SOL":"solana",
                  "DOGE":"dogecoin","BNB":"bnb","XRP":"xrp","HYPE":"hype"}
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
log = logging.getLogger("paper-longshot")

prices_ref = {}
ref_last = {}


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


def window_times(tf):
    """For 5/15: timestamp-aligned. For 60 (hourly): aligned to top of ET hour."""
    now = time.time()
    if tf == 60:
        now_et = datetime.now(timezone.utc).astimezone(ET)
        start_et = now_et.replace(minute=0, second=0, microsecond=0)
        o = int(start_et.timestamp())
        return o, o + 3600, o + 3600 - now
    if tf == 240:
        now_et = datetime.now(timezone.utc).astimezone(ET)
        block_hour = (now_et.hour // 4) * 4   # 0,4,8,12,16,20
        start_et = now_et.replace(hour=block_hour, minute=0, second=0, microsecond=0)
        o = int(start_et.timestamp())
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
        day = dt_et.day
        year = dt_et.year
        hour12 = dt_et.strftime("%I").lstrip("0") or "12"
        ampm = dt_et.strftime("%p").lower()
        return f"{ASSET_FULLNAME[asset]}-up-or-down-{month}-{day}-{year}-{hour12}{ampm}-et"
    return f"{asset.lower()}-updown-{tf}m-{open_ts}"


_market_cache = {}

def resolve_tokens(asset, tf, open_ts):
    key = (asset, tf, open_ts)
    if key in _market_cache:
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
    _market_cache[key] = None
    return None


def best_ask_cents(token_id):
    try:
        r = requests.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=6)
        b = r.json()
        prices = [float(a["price"]) for a in b.get("asks", [])
                  if float(a.get("size", 0)) > 0]
        return min(prices) * 100.0 if prices else None
    except Exception:
        return None


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


def entry_window_secs(tf):
    if tf == 240:
        return ENTRY_FIRST_SECS_240M
    if tf == 60:
        return ENTRY_FIRST_SECS_60M
    if tf == 15:
        return ENTRY_FIRST_SECS_15M
    return ENTRY_FIRST_SECS


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS paper (
        id INTEGER PRIMARY KEY AUTOINCREMENT, created TEXT, asset TEXT, tf INTEGER,
        direction TEXT, open_ts INTEGER, close_ts INTEGER, secs_left REAL,
        move_pct REAL, ask_cents REAL, open_price REAL, settle_price REAL,
        result TEXT, pnl REAL)""")
    conn.execute("UPDATE paper SET result='VOID' WHERE result='PENDING'")
    conn.commit()
    conn.close()


def db_insert(asset, tf, direction, open_ts, close_ts, secs_left, move, ask, op):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""INSERT INTO paper (created,asset,tf,direction,open_ts,close_ts,
                 secs_left,move_pct,ask_cents,open_price,result)
                 VALUES (?,?,?,?,?,?,?,?,?,?, 'PENDING')""",
              (datetime.now(timezone.utc).isoformat(), asset, tf, direction,
               open_ts, close_ts, secs_left, move, ask, op))
    rid = c.lastrowid
    conn.commit()
    conn.close()
    return rid


def db_resolve(rid, settle_price, result, pnl):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE paper SET settle_price=?, result=?, pnl=? WHERE id=?",
                 (settle_price, result, pnl, rid))
    conn.commit()
    conn.close()


def db_scoreboard():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT result, pnl, ask_cents, tf FROM paper WHERE result IN ('WIN','LOSS')")
    rows = c.fetchall()
    conn.close()
    wins = sum(1 for r in rows if r[0] == "WIN")
    pnl = sum(r[1] or 0 for r in rows)
    avg = (sum(r[2] or 0 for r in rows) / len(rows)) if rows else 0
    wr = (wins / len(rows) * 100) if rows else None
    by_tf = {}
    for r in rows:
        d = by_tf.setdefault(r[3], {"n": 0, "w": 0, "pnl": 0.0})
        d["n"] += 1
        d["w"] += 1 if r[0] == "WIN" else 0
        d["pnl"] += r[1] or 0
    return {"n": len(rows), "wins": wins, "wr": wr, "pnl": pnl,
            "avg_ask": avg, "by_tf": by_tf}


def tg(msg):
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                                "parse_mode": "HTML"}, timeout=8)
        body = {}
        try:
            body = r.json()
        except Exception:
            pass
        if getattr(r, "status_code", 200) != 200 or not body.get("ok", False):
            log.error(f"[TG] REJECTED {getattr(r,'status_code','?')}: {str(body)[:150]}")
            return
        log.info(f"[TG] {msg[:80]}")
    except Exception as e:
        log.error(f"TG error: {e}")


_upd = None

def handle_commands():
    global _upd
    try:
        p = {"timeout": 1}
        if _upd:
            p["offset"] = _upd
        for u in requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                              params=p, timeout=5).json().get("result", []):
            _upd = u["update_id"] + 1
            t = u.get("message", {}).get("text", "").strip().lower()
            if str(u.get("message", {}).get("chat", {}).get("id")) != str(TELEGRAM_CHAT_ID):
                continue
            if t == "/stats":
                s = db_scoreboard()
                wr = f"{s['wr']:.1f}%" if s["wr"] is not None else "—"
                tf_lines = []
                for tf in sorted(s["by_tf"]):
                    d = s["by_tf"][tf]
                    label = "4h" if tf == 240 else "1h" if tf == 60 else f"{tf}m"
                    twr = d["w"]/d["n"]*100 if d["n"] else 0
                    tf_lines.append(f"  {label}: {d['w']}/{d['n']} ({twr:.0f}%) ${d['pnl']:+.2f}")
                tg(f"🎰 <b>PAPER LONGSHOT scoreboard</b>\n"
                   f"trades: {s['n']} · win rate: <b>{wr}</b>\n"
                   f"avg ask: {s['avg_ask']:.1f}¢\n"
                   + ("\n".join(tf_lines) if tf_lines else "") +
                   f"\nP&L: <b>${s['pnl']:+.2f}</b> (${PAPER_STAKE:g}/trade)")
    except Exception:
        pass


open_windows = {}
pending = []
pending_lock = threading.Lock()
fired_count = {}
fired_last = {}


def probe():
    """Confirm hourly markets resolve (only if 60 is in TFS)."""
    if 60 not in TFS:
        return
    open_ts, close_ts, secs_left = window_times(60)
    found = []
    for a in ASSET_LIST:
        toks = resolve_tokens(a, 60, open_ts)
        if toks:
            found.append(a)
        _market_cache.pop((a, 60, open_ts), None)
    if found:
        tg(f"🔎 <b>Hourly probe</b>: resolved {len(found)}/7 "
           f"({', '.join(found)})\nslug: <code>{build_slug('BTC',60,open_ts)}</code>")
    else:
        tg(f"⚠️ <b>Hourly probe FAILED</b>: 0/7 resolved\n"
           f"tried <code>{build_slug('BTC',60,open_ts)}</code> — 1h won't fire")
    log.info(f"[PROBE] hourly found {found}")


def engine():
    while True:
        try:
            time.sleep(0.5)
            for tf in TFS:
                open_ts, close_ts, secs_left = window_times(tf)
                first_secs = entry_window_secs(tf)
                for asset in ASSET_LIST:
                    ref = prices_ref.get(asset)
                    if ref is None:
                        continue
                    wkey = (asset, tf, open_ts)
                    if wkey not in open_windows:
                        elapsed = (tf * 60) - secs_left
                        # For hourly, capture whenever first seen (grace is generous)
                        if elapsed <= OPEN_CAPTURE_GRACE or tf in (60, 240):
                            open_windows[wkey] = ref
                        else:
                            open_windows[wkey] = None
                        continue
                    op = open_windows[wkey]
                    if op is None:
                        continue
                    move = (ref - op) / op * 100.0
                    if fired_count.get(wkey, 0) >= MAX_STACK:
                        continue
                    elapsed = (tf * 60) - secs_left
                    if elapsed > first_secs:
                        continue
                    toks = resolve_tokens(asset, tf, open_ts)
                    if not toks:
                        continue
                    up_ask = best_ask_cents(toks[0])
                    down_ask = best_ask_cents(toks[1])
                    cand = []
                    if up_ask is not None and TAKER_MIN_ASK_CENTS <= up_ask <= TAKER_MAX_ASK_CENTS:
                        cand.append(("UP", up_ask))
                    if down_ask is not None and TAKER_MIN_ASK_CENTS <= down_ask <= TAKER_MAX_ASK_CENTS:
                        cand.append(("DOWN", down_ask))
                    if not cand:
                        continue
                    direction, ask = min(cand, key=lambda x: x[1])
                    fired_last[wkey] = time.time()
                    rid = db_insert(asset, tf, direction, open_ts, close_ts,
                                    secs_left, move, ask, op)
                    fired_count[wkey] = fired_count.get(wkey, 0) + 1
                    with pending_lock:
                        pending.append({"rid": rid, "asset": asset, "tf": tf,
                                        "direction": direction, "open_ts": open_ts,
                                        "close_ts": close_ts, "open_price": op,
                                        "ask": ask})
                    if SEND_EACH:
                        arrow = "⬆️" if direction == "UP" else "⬇️"
                        shares = PAPER_STAKE / (ask / 100.0)
                        label = "4h" if tf == 240 else "1h" if tf == 60 else f"{tf}m"
                        tg(f"🎰 <b>LONGSHOT {arrow} {ASSET_EMOJI.get(asset,'')}{asset} "
                           f"{label} {direction}</b>\n"
                           f"ask <b>{ask:.1f}¢</b> ({shares:.0f} sh) · "
                           f"elapsed {elapsed:.0f}s ≤ {first_secs:.0f}s\n"
                           f"win +${shares*1.0-PAPER_STAKE:.2f} / lose -${PAPER_STAKE:.2f}")
                    log.info(f"[LONGSHOT] {asset} {tf} {direction} ask {ask:.1f}¢ "
                             f"elapsed {elapsed:.0f}s")
        except Exception as e:
            log.error(f"[ENGINE] {e}")


def scorer():
    while True:
        try:
            time.sleep(0.5)
            now = time.time()
            with pending_lock:
                items = list(pending)
            for s in items:
                if now < s["close_ts"] + 2:
                    continue
                if now - s.get("last_chk", 0) < SETTLE_POLL_SECS:
                    continue
                s["last_chk"] = now
                outcome = fetch_polymarket_outcome(s["asset"], s["tf"], s["open_ts"])
                graded_by = "settlement"
                if outcome is None:
                    if now <= s["close_ts"] + SETTLE_TIMEOUT_SECS:
                        continue
                    settle_px = prices_ref.get(s["asset"])
                    op = s["open_price"]
                    if settle_px is None or op is None or abs((settle_px-op)/op) < 1e-6:
                        db_resolve(s["rid"], None, "VOID", 0)
                        with pending_lock:
                            s in pending and pending.remove(s)
                        continue
                    outcome = "UP" if settle_px > op else "DOWN"
                    graded_by = "feed-fallback"
                won = (s["direction"] == outcome)
                shares = PAPER_STAKE / (s["ask"] / 100.0)
                pnl = (shares * 1.0 - PAPER_STAKE) if won else -PAPER_STAKE
                result = "WIN" if won else "LOSS"
                db_resolve(s["rid"], None, result, round(pnl, 4))
                with pending_lock:
                    s in pending and pending.remove(s)
                sb = db_scoreboard()
                emoji = "\u2705" if won else "\u274c"
                wr = f"{sb['wr']:.1f}%" if sb["wr"] is not None else "\u2014"
                tag = "" if graded_by == "settlement" else " \u26a0\ufe0f feed-graded"
                label = "4h" if s["tf"] == 240 else "1h" if s["tf"] == 60 else f"{s['tf']}m"
                tg(f"{emoji} LONGSHOT {ASSET_EMOJI.get(s['asset'],'')}{s['asset']} "
                   f"{label} {s['direction']} <b>{result}</b> ${pnl:+.2f} "
                   f"\u00b7 {s['ask']:.1f}\u00a2{tag}\n"
                   f"\U0001f3b0 {sb['n']} trades \u00b7 {wr} win \u00b7 P&L ${sb['pnl']:+.2f}")
        except Exception as e:
            log.error(f"[SCORER] {e}")


def main():
    if not WEBSOCKET_AVAILABLE:
        log.error("websocket-client not installed")
        return
    init_db()
    threading.Thread(target=binance_ref_worker, daemon=True).start()
    labels = ", ".join("1h" if t == 60 else f"{t}m" for t in TFS)
    tg(f"🎰 <b>PAPER LONGSHOT live</b> — no money\n"
       f"buys 1-5¢ side · 5m: first {ENTRY_FIRST_SECS:.0f}s · "
       f"15m: first {ENTRY_FIRST_SECS_15M:.0f}s · 1h: first {ENTRY_FIRST_SECS_60M:.0f}s · "
       f"4h: first {ENTRY_FIRST_SECS_240M:.0f}s\n"
       f"tf={labels} · stake ${PAPER_STAKE:g}\n/stats")
    time.sleep(3)
    probe()
    threading.Thread(target=engine, daemon=True).start()
    threading.Thread(target=scorer, daemon=True).start()
    while True:
        try:
            handle_commands()
        except Exception as e:
            log.error(f"main: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
