"""
Quick smoke-test for the MEXC WebSocket connector.

Connects to MEXC's live WebSocket, subscribes to BTC/ETH/SOL bookTicker
streams, and prints the incoming bid/ask ticks for 20 seconds.

Usage:
    cd /Users/jacktabb/Desktop/aa/realtime
    python ../test/test_mexc_ws_simple.py

No API keys required — bookTicker is a public stream.
"""

import asyncio
import json
import sys
import time

import websockets

WS_URL = "wss://wbs.mexc.com/ws"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "NEARUSDT", "FILUSDT"]
DURATION_SECS = 20


async def run():
    print(f"\n{'='*60}")
    print("  MEXC WebSocket — bookTicker smoke-test")
    print(f"  Watching: {', '.join(SYMBOLS)}")
    print(f"  Duration: {DURATION_SECS}s")
    print(f"{'='*60}\n")

    async with websockets.connect(WS_URL, ping_interval=None) as ws:
        # Subscribe to bookTicker for each symbol
        params = [f"spot@public.bookTicker.v3.api@{sym}" for sym in SYMBOLS]
        await ws.send(json.dumps({"method": "SUBSCRIPTION", "params": params}))
        print("✓ Subscription sent — waiting for data...\n")

        tick_count = 0
        deadline = time.time() + DURATION_SECS

        while time.time() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            except asyncio.TimeoutError:
                # Send a PING to keep alive during quiet periods
                await ws.send(json.dumps({"method": "PING"}))
                continue

            msg = json.loads(raw)

            # Skip ACK / PONG messages
            if "code" in msg:
                code = msg.get("code")
                if code == 0:
                    print(f"  [ACK] {msg}")
                else:
                    print(f"  [WARN] {msg}", file=sys.stderr)
                continue

            d = msg.get("d", {})
            symbol = d.get("s", "?")
            bid = d.get("b", "N/A")
            ask = d.get("a", "N/A")
            ts = msg.get("t", 0)

            tick_count += 1
            spread = ""
            try:
                spread_val = (float(ask) - float(bid)) / float(bid) * 100
                spread = f"  spread={spread_val:.4f}%"
            except (ValueError, ZeroDivisionError):
                pass

            print(
                f"  [{symbol:10s}]  bid={bid:>14s}  ask={ask:>14s}{spread}"
            )

        print(f"\n{'='*60}")
        print(f"  Received {tick_count} ticks over {DURATION_SECS}s")
        print(f"  {'✓ PASS' if tick_count > 0 else '✗ FAIL — no ticks received'}")
        print(f"{'='*60}\n")

        return tick_count > 0


if __name__ == "__main__":
    result = asyncio.run(run())
    sys.exit(0 if result else 1)
