#!/usr/bin/env python3
"""
Simple Pionex WebSocket Test.

Connects to Pionex's public WebSocket API and prints live trade data.
Pionex's public stream only supports TRADE and DEPTH topics — there is
no TICKER topic. We subscribe to TRADE to get live price feeds.

Pionex WebSocket docs:
    https://pionex-doc.gitbook.io/apidocs/websocket/general-info
    https://pionex-doc.gitbook.io/apidocs/websocket/public-stream/trade

Usage:
    python test_pionex_ws_simple.py

Press Ctrl+C to stop.
"""

import asyncio
import json
import time
import websockets
from datetime import datetime


# Pionex WebSocket endpoint
PIONEX_WS_URL = "wss://ws.pionex.com/wsPub"

# Symbols to subscribe to (Pionex uses "BTC_USDT" format)
TEST_SYMBOLS = [
    "BTC_USDT",
    "ETH_USDT",
    "SOL_USDT",
    "XRP_USDT",
    "ADA_USDT",
    "DOGE_USDT",
    "AVAX_USDT",
    "DOT_USDT",
    "LINK_USDT",
    "LTC_USDT",
]


async def test_pionex_ws():
    print("=" * 70)
    print("  Pionex WebSocket Test")
    print("=" * 70)
    print(f"  URL: {PIONEX_WS_URL}")
    print(f"  Symbols: {TEST_SYMBOLS}")
    print("  Topic: TRADE (public stream; no TICKER topic available)")
    print("=" * 70)
    print()

    try:
        # Disable library-level ping — we handle Pionex's custom PING/PONG manually
        async with websockets.connect(PIONEX_WS_URL, ping_interval=None) as ws:
            print("✓ Connected to Pionex WebSocket")

            # Subscribe to each symbol's TRADE stream
            for sym in TEST_SYMBOLS:
                subscribe_msg = json.dumps({
                    "op": "SUBSCRIBE",
                    "topic": "TRADE",
                    "symbol": sym,
                })
                await ws.send(subscribe_msg)

            print(f"✓ Subscribed to {len(TEST_SYMBOLS)} TRADE streams")
            print()
            print("Waiting for trade updates (Ctrl+C to stop)...")
            print("-" * 70)

            tick_count = 0
            async for message in ws:
                data = json.loads(message)

                op = data.get("op", "")

                # --- Reply to server PING with PONG ---
                if op == "PING":
                    pong = json.dumps({"op": "PONG", "timestamp": int(time.time() * 1000)})
                    await ws.send(pong)
                    print(f"  ↔ PING received → PONG sent")
                    continue

                if op == "CLOSE":
                    print(f"  ✗ Server sent CLOSE")
                    break

                topic = data.get("topic", "")
                symbol = data.get("symbol", "")

                # Subscription ack
                if data.get("type") in ("SUBSCRIBED", "UNSUBSCRIBED"):
                    print(f"  ✓ {data['type']}: {symbol}")
                    continue

                if topic != "TRADE":
                    continue

                trades = data.get("data", [])
                for trade in trades:
                    price = float(trade.get("price", 0))
                    size = float(trade.get("size", 0))
                    side = trade.get("side", "").upper()
                    ts = trade.get("timestamp", 0)

                    tick_count += 1
                    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]

                    if side == "BUY":
                        color = "\033[92m"
                        arrow = "↑"
                    else:
                        color = "\033[91m"
                        arrow = "↓"
                    reset = "\033[0m"

                    print(
                        f"[{timestamp}] #{tick_count:>5}  "
                        f"{symbol:<12}  "
                        f"${price:>12,.4f}  "
                        f"size: {size:>10,.6f}  "
                        f"{color}{arrow} {side}{reset}"
                    )

    except websockets.exceptions.ConnectionClosed as e:
        print(f"\n✗ Connection closed: code={e.code} reason={e.reason}")
    except Exception as e:
        print(f"\n✗ Error: {e}")


if __name__ == "__main__":
    print()
    try:
        asyncio.run(test_pionex_ws())
    except KeyboardInterrupt:
        print("\n\n✓ Test stopped by user")
        print("\n\n✓ Test stopped by user")
