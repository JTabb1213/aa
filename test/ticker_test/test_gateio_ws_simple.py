"""
Quick smoke-test for the Gate.io WebSocket connector.

Connects to Gate.io's live WebSocket, subscribes to the spot.tickers channel
for BTC/ETH/SOL/NEAR/FIL pairs, and prints bid/ask ticks for 20 seconds.

Usage:
    cd /Users/jacktabb/Desktop/aa/realtime
    python ../test/test_gateio_ws_simple.py

No API keys required — spot.tickers is a public channel.
"""

import asyncio
import json
import sys
import time

import websockets

WS_URL = "wss://api.gateio.ws/ws/v4/"
SYMBOLS = ["BTC_USDT", "ETH_USDT", "SOL_USDT", "NEAR_USDT", "FIL_USDT"]
DURATION_SECS = 20


async def run():
    print(f"\n{'='*60}")
    print("  Gate.io WebSocket — spot.tickers smoke-test")
    print(f"  Watching: {', '.join(SYMBOLS)}")
    print(f"  Duration: {DURATION_SECS}s")
    print(f"{'='*60}\n")

    async with websockets.connect(WS_URL, ping_interval=None) as ws:
        # Subscribe to spot.tickers for all symbols
        subscribe_msg = {
            "time": int(time.time()),
            "channel": "spot.tickers",
            "event": "subscribe",
            "payload": SYMBOLS,
        }
        await ws.send(json.dumps(subscribe_msg))
        print("✓ Subscription sent — waiting for data...\n")

        tick_count = 0
        deadline = time.time() + DURATION_SECS
        last_ping = time.time()

        while time.time() < deadline:
            # Send periodic ping every 10 seconds
            if time.time() - last_ping >= 10:
                await ws.send(json.dumps({
                    "time": int(time.time()),
                    "channel": "spot.ping",
                }))
                last_ping = time.time()

            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            except asyncio.TimeoutError:
                continue

            msg = json.loads(raw)
            channel = msg.get("channel", "")
            event = msg.get("event", "")

            # Skip pong and ack messages
            if channel == "spot.pong" or event in ("subscribe", "unsubscribe"):
                print(f"  [ACK] channel={channel!r} event={event!r}")
                continue

            if channel != "spot.tickers" or event != "update":
                continue

            result = msg.get("result", {})
            pair = result.get("currency_pair", "?")
            bid = result.get("highest_bid", "N/A")
            ask = result.get("lowest_ask", "N/A")
            last = result.get("last", "N/A")

            tick_count += 1
            spread = ""
            try:
                spread_val = (float(ask) - float(bid)) / float(bid) * 100
                spread = f"  spread={spread_val:.4f}%"
            except (ValueError, ZeroDivisionError):
                pass

            print(
                f"  [{pair:12s}]  bid={bid:>14s}  ask={ask:>14s}"
                f"  last={last:>14s}{spread}"
            )

        print(f"\n{'='*60}")
        print(f"  Received {tick_count} ticks over {DURATION_SECS}s")
        print(f"  {'✓ PASS' if tick_count > 0 else '✗ FAIL — no ticks received'}")
        print(f"{'='*60}\n")

        return tick_count > 0


if __name__ == "__main__":
    result = asyncio.run(run())
    sys.exit(0 if result else 1)
