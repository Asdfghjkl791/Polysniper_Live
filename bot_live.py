#!/usr/bin/env python3
# PAPER LONGSHOT v1.0 — buy the CHEAP side (1-5¢) in the FIRST MINUTE (no money)
#
# THE QUESTION
#   The taker/maker bots buy the near-certain 99¢ side. This bot tests the exact
#   opposite: buy the CHEAP longshot side (1-5¢) right at the open, before the
#   window has resolved anything. At 1-5¢ you only need to be right ~1-5% of the
#   time to break even, and every win pays 20-100x. This bot asks, on paper:
#   does the market SYSTEMATICALLY misprice the cheap side early? If the true
#   flip rate on a 1-5¢ side is higher than the price implies, this prints; if
#   the market prices it fairly, it bleeds slowly. The scoreboard decides.
#
# THE STRATEGY (as specified)
#   • Enter ONLY in the first 60 seconds of a window (5m: secs_left 240-300;
#     15m: secs_left 840-900). Early = the outcome is still maximally uncertain,
#     so the cheap side is cheapest.
#   • NO move gate. The price band IS the filter.
#   • Buy EITHER side whose live ask is in [1¢, 5¢]. If BOTH sides are 1-5¢
#     (rare, only in dead-flat markets) it takes both.
#   • One entry per side per window (MAX_STACK slots shared across the window).
#
# WHY PAPER FIRST
#   Longshot betting is where naive strategies die fastest — the payout math
#   looks incredible (100x!) but if the win rate is even slightly below the
#   price, you bleed. So score it live on real Polymarket asks for a few hundred
#   windows before a cent goes in. Same discipline as every other bot.
#
# HONEST LIMITS
#   • Assumes the displayed ask is takeable for full size — UPPER bound on fills.
#   • A 1-5¢ side often has THIN size; real fills may be worse or partial.
#   • Grades off the reference feed direction at close (same as taker bot).
#   • No fees modeled.
#   • Selection reality: the cheap side is cheap BECAUSE it usually loses. This
#     bot only wins if the market OVER-charges for that near-certainty. Read the
#     scoreboard, not the payout fantasy.
#
# ENV (required): TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
# ENV (optional): PAPER_STAKE=5, ENTRY_MIN_CENTS=1, ENTRY_MAX_CENTS=5,
#   FIRST_MIN_SECS=60 (how long the entry window stays open after open),
#   BUY_BOTH_SIDES=true, DB_PATH=paper_longshot.db, TIMEFRAMES=5,15,
#   SEND_EACH=true

import os
import time
import json
import sqlite3
import logging
import threading
import requests
from datetime import datetime, timezone

try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
PAPER_STAKE      = float(os.environ.get("PAPER_STAKE", "5"))
# The entry band. Buy a side only if its live ask sits in [MIN, MAX] cents.
ENTRY_MIN_CENTS  = float(os.environ.get("ENTRY_MIN_CENTS", "1"))
ENTRY_MAX_CENTS  = float(os.environ.get("ENTRY_MAX_CENTS", "5"))
# Only enter during the first FIRST_MIN_SECS seconds after the window opens.
FIRST_MIN_SECS   = float(os.environ.get("FIRST_MIN_SECS", "60"))
# If both sides land in the band (dead-flat market), take both. Else take the
# cheaper one only.
BUY_BOTH_SIDES   = os.environ.get("BUY_BOTH_SIDES", "true").lower() == "true"
DB_PATH          = os.environ.get("DB_PATH", "paper_longshot.db")
SEND_EACH        = os.environ.get("SEND_EACH", "true").lower() == "true"
FAST_TIE_EPS_PCT = float(os.environ.get("FAST_TIE_EPS_PCT", "0.0005"))
TFS              = [int(x) for x in os.environ.get("TIMEFRAMES", "5,15").split(",")]

ASSET_LIST = ["BTC", "ETH", "SOL", "DOGE", "BNB", "XRP", "HYPE"]
ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡",
               "BNB": "🟨", "XRP": "⚪", "HYPE": "🟢"}

CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler()])
log = logging.getLogger("paper-longshot")

# ── REFERENCE FEED (for grading only — NOT used to gate entries) ─────────────
BINANCE_WS = ("wss://data-stream.binance.vision/stream?streams=" +
              "/".join(f"{s}usdt@bookTicker" for s in
                       ["btc", "eth", "sol", "doge", "bnb", "xrp"]))
BINANCE_SYM_TO_ASSET = {f"{s.upper()}USDT": s.upper()
                        for s in ["btc", "eth", "sol", "doge", "bnb", "xrp"]}
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


# ── POLYMARKET MARKET RESOLUTION + ASK READ ─────────────────────────────────
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
                _market_cache[key] = (toks[0], toks[1])  # [UP, DOWN]
                return _market_cache[key]
    except Exception as e:
        log.debug(f"[RESOLVE] {slug}: {e}")
    _market_cache[key] = None
    return None


def best_ask_cents(token_id):
    """Live best ask on Polymarket for this token, in cents. None on failure."""
    try:
        r = requests.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=6)
        b = r.json()
        asks = b.get("asks", [])
        prices = [float(a["price"]) for a in asks if float(a.get("size", 0)) > 0]
        if not prices:
            return None
        return min(prices) * 100.0
    except Exception:
        return None


# ── WINDOW TIMING ────────────────────────────────────────────────────────────
def window_times(tf):
    now = int(time.time())
    length = tf * 60
    open_ts = (now // length) * length
    close_ts = open_ts + length
    return open_ts, close_ts, close_ts - now


# ── DB ───────────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS paper (
        id INTEGER PRIMARY KEY AUTOINCREMENT, created TEXT,
        asset TEXT, tf INTEGER, direction TEXT, open_ts INTEGER, close_ts INTEGER,
        secs_into REAL, ask_cents REAL, open_price REAL,
        settle_price REAL, result TEXT, pnl REAL)""")
    conn.execute("UPDATE paper SET result='VOID' WHERE result='PENDING'")
    conn.commit()
    conn.close()


def db_insert(asset, tf, direction, open_ts, close_ts, secs_into, ask, open_price):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""INSERT INTO paper (created,asset,tf,direction,open_ts,close_ts,
                 secs_into,ask_cents,open_price,result)
                 VALUES (?,?,?,?,?,?,?,?,?, 'PENDING')""",
              (datetime.now(timezone.utc).isoformat(), asset, tf, direction,
               open_ts, close_ts, secs_into, ask, open_price))
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
    c.execute("SELECT result, pnl, ask_cents FROM paper WHERE result IN ('WIN','LOSS','TIE')")
    rows = c.fetchall()
    conn.close()
    dec = [r for r in rows if r[0] != "TIE"]
    wins = sum(1 for r in dec if r[0] == "WIN")
    pnl = sum(r[1] or 0 for r in rows)
    avg_ask = (sum(r[2] or 0 for r in rows) / len(rows)) if rows else 0
    wr = (wins / len(dec) * 100) if dec else None
    # break-even win% for a longshot ≈ avg ask paid (cents). At 3¢ you need to
    # win ~3% of the time. The whole bet is: is real WR > this tiny number?
    be = avg_ask if avg_ask else ENTRY_MAX_CENTS
    return {"n": len(rows), "wins": wins, "wr": wr, "pnl": pnl,
            "avg_ask": avg_ask, "be": be}


# ── TELEGRAM ─────────────────────────────────────────────────────────────────
def tg(msg):
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                            "parse_mode": "HTML"}, timeout=8)
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
                verdict = ("ABOVE break-even ✅ (mispriced in our favor)"
                           if s["wr"] and s["wr"] > s["be"]
                           else "below break-even ⚠️ (market pricing it fairly)")
                tg(f"📄 <b>PAPER LONGSHOT scoreboard</b>\n"
                   f"simulated trades: {s['n']}\n"
                   f"win rate: <b>{wr}</b>\n"
                   f"avg ask paid: {s['avg_ask']:.2f}¢ → break-even ≈ {s['be']:.2f}%\n"
                   f"{verdict}\n"
                   f"paper P&L: <b>${s['pnl']:+.2f}</b> (${PAPER_STAKE:g}/trade)")
    except Exception:
        pass


# ── ENGINE ───────────────────────────────────────────────────────────────────
open_windows = {}     # (asset,tf,open_ts) -> open_price at window start
fired_sides = {}      # (asset,tf,open_ts) -> set of directions already taken
pending = []
pending_lock = threading.Lock()


def engine():
    while True:
        try:
            time.sleep(0.5)
            for tf in TFS:
                open_ts, close_ts, secs_left = window_times(tf)
                length = tf * 60
                secs_into = length - secs_left
                for asset in ASSET_LIST:
                    ref = prices_ref.get(asset)
                    if ref is None:
                        continue
                    wkey = (asset, tf, open_ts)
                    # capture open price once, the first time we see the window
                    if wkey not in open_windows:
                        open_windows[wkey] = ref
                    # ENTRY GATE 1: first minute only
                    if secs_into > FIRST_MIN_SECS:
                        continue
                    taken = fired_sides.setdefault(wkey, set())
                    if len(taken) >= 2:
                        continue  # both sides already taken this window
                    toks = resolve_tokens(asset, tf, open_ts)
                    if not toks:
                        continue
                    up_tok, down_tok = toks
                    op = open_windows[wkey]
                    # check each side's live ask against the band
                    candidates = []  # (direction, ask)
                    for direction, tok in (("UP", up_tok), ("DOWN", down_tok)):
                        if direction in taken:
                            continue
                        ask = best_ask_cents(tok)
                        if ask is None:
                            continue
                        if ENTRY_MIN_CENTS <= ask <= ENTRY_MAX_CENTS:
                            candidates.append((direction, ask))
                    if not candidates:
                        continue
                    # if not buying both, keep only the cheaper qualifying side
                    if not BUY_BOTH_SIDES and len(candidates) > 1:
                        candidates = [min(candidates, key=lambda x: x[1])]
                    for direction, ask in candidates:
                        rid = db_insert(asset, tf, direction, open_ts, close_ts,
                                        secs_into, ask, op)
                        taken.add(direction)
                        with pending_lock:
                            pending.append({"rid": rid, "asset": asset, "tf": tf,
                                            "direction": direction,
                                            "close_ts": close_ts, "open_price": op,
                                            "ask": ask})
                        if SEND_EACH:
                            arrow = "⬆️" if direction == "UP" else "⬇️"
                            payout = PAPER_STAKE * (100 - ask) / ask
                            tg(f"📄 <b>PAPER LONGSHOT {arrow} "
                               f"{ASSET_EMOJI.get(asset,'')}{asset} {tf}m {direction}</b>\n"
                               f"buy ask <b>{ask:.1f}¢</b> · {secs_into:.0f}s into window\n"
                               f"(stake ${PAPER_STAKE:g} → win +${payout:.2f} / "
                               f"lose -${PAPER_STAKE:.2f})")
                        log.info(f"[PAPER] BUY {asset} {tf}m {direction} ask {ask:.1f}¢ "
                                 f"{secs_into:.0f}s into window")
        except Exception as e:
            log.error(f"[ENGINE] {e}")


def scorer():
    """Grade off the reference feed direction at window close (~1s after)."""
    while True:
        try:
            time.sleep(0.5)
            now = time.time()
            with pending_lock:
                items = list(pending)
            for s in items:
                if now < s["close_ts"]:
                    continue
                settle = prices_ref.get(s["asset"])
                op = s["open_price"]
                if settle is None:
                    if now > s["close_ts"] + 30:
                        db_resolve(s["rid"], None, "VOID", 0)
                        with pending_lock:
                            s in pending and pending.remove(s)
                    continue
                move = (settle - op) / op * 100.0
                if abs(move) < FAST_TIE_EPS_PCT:
                    if now > s["close_ts"] + 30:
                        db_resolve(s["rid"], None, "VOID", 0)
                        with pending_lock:
                            s in pending and pending.remove(s)
                    continue
                won = (s["direction"] == ("UP" if move > 0 else "DOWN"))
                shares = PAPER_STAKE / (s["ask"] / 100.0)
                pnl = (shares * 1.0 - PAPER_STAKE) if won else -PAPER_STAKE
                result = "WIN" if won else "LOSS"
                db_resolve(s["rid"], None, result, round(pnl, 4))
                with pending_lock:
                    s in pending and pending.remove(s)
                sb = db_scoreboard()
                emoji = "\u2705" if result == "WIN" else "\u274c"
                wr = f"{sb['wr']:.1f}%" if sb["wr"] is not None else "\u2014"
                tg(f"{emoji} PAPER {ASSET_EMOJI.get(s['asset'],'')}{s['asset']} "
                   f"{s['tf']}m {s['direction']} <b>{result}</b> ${pnl:+.2f} "
                   f"({move:+.3f}%)\n"
                   f"\U0001f4c4 {sb['n']} trades \u00b7 {wr} (BE {sb['be']:.2f}%) \u00b7 "
                   f"P&L ${sb['pnl']:+.2f}")
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
    tg(f"📄 <b>PAPER LONGSHOT live</b> — no money\n"
       f"buys the CHEAP side ({ENTRY_MIN_CENTS:.0f}-{ENTRY_MAX_CENTS:.0f}¢) in the "
       f"first {FIRST_MIN_SECS:.0f}s of each window\n"
       f"tf={TFS} · stake ${PAPER_STAKE:g} · both sides={BUY_BOTH_SIDES}\n"
       f"the scoreboard decides: is the cheap side mispriced?\n/stats")
    while True:
        try:
            handle_commands()
        except Exception as e:
            log.error(f"main: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
