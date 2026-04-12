#!/usr/bin/env python3
"""
Simple Bybit WebSocket Test.

Connects to Bybit's v5 WebSocket API and prints live ticker data.
This verifies the connection works before integrating into the pipeline.

Bybit WebSocket docs:
    https://bybit-exchange.github.io/docs/v5/ws/connect

Usage:
    python test_bybit_ws_simple.py

Press Ctrl+C to stop.
"""

import asyncio
import json
import websockets
from datetime import datetime


# Bybit v5 spot WebSocket endpoint
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/spot"

# Symbols to subscribe to (Bybit uses "BTCUSDT" format)
TEST_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
    "DOTUSDT",
    "LINKUSDT",
    "LTCUSDT",
]


async def test_bybit_ws():
    print("=" * 70)
    print("  Bybit WebSocket Test")
    print("=" * 70)
    print(f"  URL: {BYBIT_WS_URL}")
    print(f"  Symbols: {TEST_SYMBOLS}")
    print("=" * 70)
    print()

    subscribe_msg = json.dumps({
        "op": "subscribe",
        "args": [f"tickers.{sym}" for sym in TEST_SYMBOLS],
    })

    try:
        async with websockets.connect(BYBIT_WS_URL, ping_interval=20) as ws:
            print("✓ Connected to Bybit WebSocket")
            await ws.send(subscribe_msg)
            print("✓ Subscription sent")
            print()
            print("Waiting for ticker updates (Ctrl+C to stop)...")
            print("-" * 70)

            tick_count = 0
            async for message in ws:
                data = json.loads(message)

                topic = data.get("topic", "")
                if not topic.startswith("tickers."):
                    # Handle subscription confirmations or pings
                    if data.get("success") is not None:
                        status = "✓" if data["success"] else "✗"
                        print(f"{status} Subscription response: {data.get('ret_msg', '')}")
                    continue

                ticker = data.get("data", {})
                symbol = ticker.get("symbol", "")
                last_price = ticker.get("lastPrice", "0")
                bid = ticker.get("bid1Price", "0")
                ask = ticker.get("ask1Price", "0")
                volume = ticker.get("volume24h", "0")
                price_change_pct = ticker.get("price24hPcnt", "0")

                tick_count += 1
                timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]

                try:
                    change = float(price_change_pct) * 100  # Bybit returns as decimal
                    if change > 0:
                        color = "\033[92m"  # green
                        arrow = "↑"
                    elif change < 0:
                        color = "\033[91m"  # red
                        arrow = "↓"
                    else:
                        color = "\033[0m"
                        arrow = "→"
                except ValueError:
                    color = "\033[0m"
                    arrow = "→"
                    change = 0

                reset = "\033[0m"

                print(
                    f"[{timestamp}] #{tick_count:>5}  "
                    f"{symbol:<12}  "
                    f"${float(last_price):>12,.4f}  "
                    f"bid: {float(bid):>12,.4f}  "
                    f"ask: {float(ask):>12,.4f}  "
                    f"{color}{arrow} {change:+.2f}%{reset}"
                )

    except websockets.exceptions.ConnectionClosed as e:
        print(f"\n✗ Connection closed: {e}")
    except Exception as e:
        print(f"\n✗ Error: {e}")


if __name__ == "__main__":
    print()
    try:
        asyncio.run(test_bybit_ws())
    except KeyboardInterrupt:
        print("\n\n✓ Test stopped by user")
