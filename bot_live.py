#!/usr/bin/env python3
# PAPER LONGSHOT — buys 1-5c deep-underdog side in the first minute (no money)
import os, time, json, sqlite3, logging, threading, requests
from datetime import datetime, timezone
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
SETTLE_TIMEOUT_SECS = float(os.environ.get("SETTLE_TIMEOUT_SECS", "900"))
MAX_STACK           = int(os.environ.get("MAX_STACK", "1"))
STACK_COOLDOWN_SECS = float(os.environ.get("STACK_COOLDOWN_SECS", "8"))
TFS              = [int(x) for x in os.environ.get("TIMEFRAMES", "5,15").split(",")]
# LONGSHOT entry windows — per timeframe. 5m: first 60s. 15m: first 180s (3 min).
ENTRY_FIRST_SECS     = float(os.environ.get("ENTRY_FIRST_SECS", "60"))
ENTRY_FIRST_SECS_15M = float(os.environ.get("ENTRY_FIRST_SECS_15M", "180"))
ENTRY_FIRST_SECS_30M = float(os.environ.get("ENTRY_FIRST_SECS_30M", "600"))  # 30m: first 10 min
OPEN_CAPTURE_GRACE   = float(os.environ.get("OPEN_CAPTURE_GRACE", "3"))

ASSET_LIST = ["BTC", "ETH", "SOL", "DOGE", "BNB", "XRP", "HYPE"]
ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡",
               "BNB": "🟨", "XRP": "⚪", "HYPE": "🟢"}
CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"
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


_market_cache = {}

def resolve_tokens(asset, tf, open_ts):
    key = (asset, tf, open_ts)
    if key in _market_cache:
        return _market_cache[key]
    slug = f"{asset.lower()}-updown-{tf}m-{open_ts}"
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
    slug = f"{asset.lower()}-updown-{tf}m-{open_ts}"
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


def window_times(tf):
    now = int(time.time())
    L = tf * 60
    o = (now // L) * L
    return o, o + L, o + L - now


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
    c.execute("SELECT result, pnl, ask_cents FROM paper WHERE result IN ('WIN','LOSS')")
    rows = c.fetchall()
    conn.close()
    wins = sum(1 for r in rows if r[0] == "WIN")
    pnl = sum(r[1] or 0 for r in rows)
    avg = (sum(r[2] or 0 for r in rows) / len(rows)) if rows else 0
    wr = (wins / len(rows) * 100) if rows else None
    return {"n": len(rows), "wins": wins, "wr": wr, "pnl": pnl, "avg_ask": avg}


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
                tg(f"🎰 <b>PAPER LONGSHOT scoreboard</b>\n"
                   f"trades: {s['n']} · win rate: <b>{wr}</b>\n"
                   f"avg ask: {s['avg_ask']:.1f}¢\n"
                   f"P&L: <b>${s['pnl']:+.2f}</b> (${PAPER_STAKE:g}/trade)")
    except Exception:
        pass


open_windows = {}
pending = []
pending_lock = threading.Lock()
fired_count = {}
fired_last = {}


def engine():
    while True:
        try:
            time.sleep(0.5)
            for tf in TFS:
                open_ts, close_ts, secs_left = window_times(tf)
                first_secs = (ENTRY_FIRST_SECS_30M if tf == 30 else
                              ENTRY_FIRST_SECS_15M if tf == 15 else ENTRY_FIRST_SECS)
                for asset in ASSET_LIST:
                    ref = prices_ref.get(asset)
                    if ref is None:
                        continue
                    wkey = (asset, tf, open_ts)
                    if wkey not in open_windows:
                        elapsed = (tf * 60) - secs_left
                        if elapsed <= OPEN_CAPTURE_GRACE:
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
                        continue  # past this timeframe's entry window
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
                        tg(f"🎰 <b>LONGSHOT {arrow} {ASSET_EMOJI.get(asset,'')}{asset} "
                           f"{tf}m {direction}</b>\n"
                           f"ask <b>{ask:.1f}¢</b> ({shares:.0f} sh) · {secs_left:.0f}s left "
                           f"(elapsed {elapsed:.0f}s ≤ {first_secs:.0f}s)\n"
                           f"win +${shares*1.0-PAPER_STAKE:.2f} / lose -${PAPER_STAKE:.2f}")
                    log.info(f"[LONGSHOT] {asset} {tf}m {direction} ask {ask:.1f}¢ "
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
                tg(f"{emoji} LONGSHOT {ASSET_EMOJI.get(s['asset'],'')}{s['asset']} "
                   f"{s['tf']}m {s['direction']} <b>{result}</b> ${pnl:+.2f} "
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
    threading.Thread(target=engine, daemon=True).start()
    threading.Thread(target=scorer, daemon=True).start()
    tg(f"🎰 <b>PAPER LONGSHOT live</b> — no money\n"
       f"buys 1-5¢ side · 5m: first {ENTRY_FIRST_SECS:.0f}s · "
       f"15m: first {ENTRY_FIRST_SECS_15M:.0f}s · 30m: first {ENTRY_FIRST_SECS_30M:.0f}s\n"
       f"tf={TFS} · stake ${PAPER_STAKE:g}\n/stats")
    while True:
        try:
            handle_commands()
        except Exception as e:
            log.error(f"main: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
