#!/usr/bin/env python3
"""
Simple Pionex WebSocket Test.

Connects to Pionex's WebSocket API and prints live ticker data.
This verifies the connection works before integrating into the pipeline.

Pionex WebSocket docs:
    https://pionex-doc.gitbook.io/apidocs/websocket/market-data

Usage:
    python test_pionex_ws_simple.py

Press Ctrl+C to stop.
"""

import asyncio
import json
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
    print("=" * 70)
    print()

    try:
        async with websockets.connect(PIONEX_WS_URL, ping_interval=20) as ws:
            print("✓ Connected to Pionex WebSocket")

            # Subscribe to each symbol's ticker
            for sym in TEST_SYMBOLS:
                subscribe_msg = json.dumps({
                    "op": "SUBSCRIBE",
                    "topic": "TICKER",
                    "symbol": sym,
                })
                await ws.send(subscribe_msg)

            print(f"✓ Subscribed to {len(TEST_SYMBOLS)} tickers")
            print()
            print("Waiting for ticker updates (Ctrl+C to stop)...")
            print("-" * 70)

            tick_count = 0
            async for message in ws:
                data = json.loads(message)

                topic = data.get("topic", "")
                if topic != "TICKER":
                    # Log subscription acks or pings
                    if data.get("op"):
                        print(f"  ← {data.get('op')}: {data.get('message', 'ok')}")
                    continue

                ticker = data.get("data", {})
                symbol = ticker.get("symbol", "")
                last_price = ticker.get("close", "0")
                bid = ticker.get("bid", "0")
                ask = ticker.get("ask", "0")
                volume = ticker.get("volume", "0")

                # Compute change from open
                open_price = float(ticker.get("open", 0) or 0)
                last_f = float(last_price)
                change = ((last_f - open_price) / open_price * 100) if open_price > 0 else 0

                tick_count += 1
                timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]

                if change > 0:
                    color = "\033[92m"
                    arrow = "↑"
                elif change < 0:
                    color = "\033[91m"
                    arrow = "↓"
                else:
                    color = "\033[0m"
                    arrow = "→"
                reset = "\033[0m"

                print(
                    f"[{timestamp}] #{tick_count:>5}  "
                    f"{symbol:<12}  "
                    f"${last_f:>12,.4f}  "
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
        asyncio.run(test_pionex_ws())
    except KeyboardInterrupt:
        print("\n\n✓ Test stopped by user")
