import asyncio
import threading
import json
import os
import requests
from collections import deque
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import websockets
import logging

# silence the default werkzeug request logging (GET/POST lines)
#logging.getLogger('werkzeug').setLevel(logging.ERROR)

# ---------------------------------------------------------------------------
# WHY ticker IS FASTER THAN trade
# ---------------------------------------------------------------------------
# The old `trade` channel only fires when two parties actually complete a deal.
# In thin markets that can be seconds apart — you're hearing about a price
# that was agreed in the past.
#
# The `ticker` channel fires whenever the BEST BID or BEST ASK in the order
# book changes, which happens far more often:
#   - A new limit order placed at or near the top of the book  → update
#   - An existing order cancelled at the top                   → update
#   - A partial fill that moves the best bid/ask               → update
#
# This gives us bid/ask on every quote change, not just on executed trades.
# The mid-price (bid+ask)/2 is what trading systems use as the "real" price.
#
# The `book` channel (full order book depth) fires even more often but sends
# every price level, not just the best bid/ask. It's extremely high bandwidth
# and is what arbitrage bots use when they need the full market depth picture.
# For tracking the current price, `ticker` is the right tool.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Shared state (written by ws threads, read by Flask)
# ---------------------------------------------------------------------------
latest_prices  = {}                  # pair -> {bid, ask, mid, last, vwap, spread_pct}
pair_seqs      = {}                  # pair -> seq at which it was last updated
global_seq     = 0                   # monotonically increasing write counter
recent_updates = deque(maxlen=500)   # rolling window of last 500 ticker updates
ticker_count   = 0                   # total ticker events received
conn_status    = {}                  # conn_id -> status string

recent_trades  = deque(maxlen=1000)  # rolling window of last 1000 executed trades
trade_count    = 0                   # total trade events received

# Trades at or above this notional USD value are flagged as whale activity.
# e.g. 50000 = flag any single trade worth $50k+
WHALE_THRESHOLD_USD = 50_000

# ---------------------------------------------------------------------------
# HOW DELTA UPDATES WORK
# ---------------------------------------------------------------------------
# Every time a ticker event arrives, global_seq is incremented and stored
# alongside that pair in pair_seqs.  The client remembers the last seq it
# received and sends it as ?since=<seq>.  The server returns ONLY the pairs
# whose seq is greater than `since`, plus the current global_seq so the
# client can use it on the next request.
#
# On a 500 ms poll with 1500 pairs where only ~50 changed, the response
# shrinks from 1500 objects to ~50 — roughly a 30× reduction in payload.
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, template_folder=BASE_DIR)
CORS(app)

CHUNK_SIZE = 200   # Kraken supports ~250 pairs per connection; stay under


# ---------------------------------------------------------------------------
# Fetch all tradeable pairs from Kraken REST API
# ---------------------------------------------------------------------------
def fetch_kraken_pairs():
    print("[REST] Fetching all Kraken pairs...")
    try:
        resp = requests.get("https://api.kraken.com/0/public/AssetPairs", timeout=10)
        data = resp.json()
        pairs = []
        for key, val in data["result"].items():
            wsname = val.get("wsname")
            # skip dark-pool entries (contain ".d")
            if wsname and ".d" not in key:
                pairs.append(wsname)
        pairs = sorted(set(pairs))
        print(f"[REST] {len(pairs)} tradeable pairs found")
        return pairs
    except Exception as e:
        print(f"[REST] Failed to fetch pairs: {e} — using fallback")
        return ["XBT/USD", "ETH/USD", "SOL/USD", "XRP/USD"]


# ---------------------------------------------------------------------------
# Ticker WebSocket listener — one coroutine per batch of pairs
# ---------------------------------------------------------------------------
async def ws_listener(conn_id, pairs):
    global ticker_count, global_seq, trade_count

    uri = "wss://ws.kraken.com"
    # Subscribe to BOTH ticker and trade on the same connection.
    # Kraken allows multiple subscriptions per connection — no need to
    # double the connection count.
    #
    # ticker message: [channelID, {fields}, "ticker", "PAIR"]
    # trade message:  [channelID, [[price, vol, time, side, type, misc], ...], "trade", "PAIR"]
    ticker_sub = {
        "event": "subscribe",
        "pair": pairs,
        "subscription": {"name": "ticker"}
    }
    trade_sub = {
        "event": "subscribe",
        "pair": pairs,
        "subscription": {"name": "trade"}
    }

    while True:
        try:
            conn_status[conn_id] = f"Connecting... ({len(pairs)} pairs)"
            print(f"[WS-{conn_id}] Connecting for {len(pairs)} pairs...")

            async with websockets.connect(uri, ping_interval=30) as ws:
                await ws.send(json.dumps(ticker_sub))
                await ws.send(json.dumps(trade_sub))
                conn_status[conn_id] = f"Connected ✓ ({len(pairs)} pairs)"
                print(f"[WS-{conn_id}] Connected and subscribed to ticker + trade!")

                async for message in ws:
                    data = json.loads(message)

                    # All data messages are lists; control messages (heartbeat etc.) are dicts
                    if not isinstance(data, list):
                        continue

                    channel = data[2]
                    pair    = data[3]

                    # ── Ticker ──────────────────────────────────────────────
                    if channel == "ticker":
                        fields = data[1]
                        bid  = float(fields["b"][0])
                        ask  = float(fields["a"][0])
                        mid  = (bid + ask) / 2
                        last = float(fields["c"][0])
                        vwap = float(fields["p"][0])

                        latest_prices[pair] = {
                            "bid":        bid,
                            "ask":        ask,
                            "mid":        mid,
                            "last":       last,
                            "vwap":       vwap,
                            "spread_pct": round((ask - bid) / mid * 100, 4) if mid else 0,
                        }
                        global_seq += 1
                        pair_seqs[pair] = global_seq
                        ticker_count += 1

                        recent_updates.append({
                            "pair": pair,
                            "bid":  bid,
                            "ask":  ask,
                            "mid":  mid,
                            "last": last,
                        })

                    # ── Trade ───────────────────────────────────────────────
                    elif channel == "trade":
                        for t in data[1]:
                            price    = float(t[0])
                            volume   = float(t[1])
                            ts       = float(t[2])
                            side     = "buy" if t[3] == "b" else "sell"
                            notional = price * volume   # USD value of this trade

                            trade_count += 1
                            recent_trades.append({
                                "pair":     pair,
                                "price":    price,
                                "volume":   volume,
                                "notional": notional,
                                "side":     side,
                                "time":     ts,
                                "whale":    notional >= WHALE_THRESHOLD_USD,
                            })

        except Exception as e:
            conn_status[conn_id] = f"Error — retrying: {str(e)[:50]}"
            print(f"[WS-{conn_id}] Error: {e}. Reconnecting in 5 s...")
            await asyncio.sleep(5)


async def run_all_listeners(all_pairs):
    chunks = [all_pairs[i:i + CHUNK_SIZE] for i in range(0, len(all_pairs), CHUNK_SIZE)]
    print(f"[WS] Spawning {len(chunks)} connection(s) for {len(all_pairs)} pairs")
    await asyncio.gather(*[ws_listener(i, chunk) for i, chunk in enumerate(chunks)])


def start_ws():
    all_pairs = fetch_kraken_pairs()
    # async is needed because each ws connection is an infinite loop that needs
    # to run concurrently — calling them sequentially would block on the first
    # one forever and the rest would never start.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_all_listeners(all_pairs))


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("websocket_live_viewer.html")


@app.route("/api/updates")
def get_updates():
    """Recent ticker events — newest bid/ask quote changes across all pairs."""
    return jsonify({
        "ticker_count": ticker_count,
        "trade_count":  trade_count,
        "connections":  conn_status,
        "updates":      list(recent_updates)[-100:],
    })


@app.route("/api/trades")
def get_trades():
    """
    Recent executed trades across all pairs, newest first.
    Query param:
      ?whale=1   only return trades flagged as whale activity
    """
    whale_only = request.args.get("whale", "0") == "1"
    trades = list(recent_trades)
    if whale_only:
        trades = [t for t in trades if t["whale"]]
    return jsonify({
        "trade_count": trade_count,
        "trades":      list(reversed(trades))[:200],
    })


@app.route("/api/prices")
def get_prices():
    """
    Delta-aware price snapshot.

    Query param:
      ?since=<int>   (default 0 = return everything)

    Returns only pairs whose data changed after `since`, plus the current
    global_seq so the client can pass it back on the next request.
    First call: ?since=0  → full snapshot of all pairs (cold start).
    Subsequent: ?since=<last_seq> → only pairs that changed since then.
    """
    since = int(request.args.get("since", 0))
    delta = {
        pair: latest_prices[pair]
        for pair, seq in pair_seqs.items()
        if seq > since and pair in latest_prices
    }
    return jsonify({
        "seq":        global_seq,   # client stores this, sends it next time
        "pair_count": len(latest_prices),
        "changed":    len(delta),
        "prices":     delta,
    })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Thread needed because the websocket listener is a blocking infinite loop,
    # but we also want Flask running in the main thread.
    # 2 threads total: main → Flask, background → websocket event loop.
    thread = threading.Thread(target=start_ws, daemon=True)
    thread.start()
    # use_reloader=False prevents Flask from spawning a duplicate ws thread
    app.run(debug=True, port=5555, use_reloader=False)
