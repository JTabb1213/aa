#!/usr/bin/env python3
"""
Simple Binance WebSocket Test.

Connects to Binance's WebSocket API and prints live ticker data.
This verifies the connection works before integrating into the pipeline.

Binance WebSocket docs:
    https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-ticker-streams

Usage:
    python test_binance_ws_simple.py
    
Press Ctrl+C to stop.
"""

import asyncio
import json
import websockets
from datetime import datetime


# Binance WebSocket endpoint (combined streams)
BINANCE_WS_URL = "wss://stream.binance.us:9443"

# Symbols to subscribe to (Binance uses lowercase "btcusdt" format)
TEST_SYMBOLS = [
    "btcusdt",
    "ethusdt",
    "solusdt",
    "xrpusdt",
    "adausdt",
    "dogeusdt",
    "avaxusdt",
    "dotusdt",
    "linkusdt",
    "ltcusdt",
]


async def test_binance_ws():
    print("=" * 70)
    print("  Binance WebSocket Test")
    print("=" * 70)
    print(f"  URL: {BINANCE_WS_URL}")
    print(f"  Symbols: {TEST_SYMBOLS}")
    print("=" * 70)
    print()

    # Build combined streams URL
    streams = "/".join(f"{sym}@ticker" for sym in TEST_SYMBOLS)
    ws_url = f"{BINANCE_WS_URL}/stream?streams={streams}"

    try:
        async with websockets.connect(ws_url, ping_interval=30) as ws:
            print("✓ Connected to Binance WebSocket")
            print()
            print("Waiting for ticker updates (Ctrl+C to stop)...")
            print("-" * 70)

            tick_count = 0
            async for message in ws:
                data = json.loads(message)

                # Combined stream format: {"stream": "btcusdt@ticker", "data": {...}}
                ticker = data.get("data", data)
                event_type = ticker.get("e")

                # Skip non-ticker messages
                if event_type != "24hrTicker":
                    continue

                symbol = ticker.get("s", "")
                last_price = ticker.get("c", "0")
                bid = ticker.get("b", "0")
                ask = ticker.get("a", "0")
                volume = ticker.get("v", "0")
                price_change_pct = ticker.get("P", "0")

                tick_count += 1
                timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]

                # Color code based on price change
                try:
                    change = float(price_change_pct)
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
        asyncio.run(test_binance_ws())
    except KeyboardInterrupt:
        print("\n\n✓ Test stopped by user")
