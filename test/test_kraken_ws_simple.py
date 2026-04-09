#!/usr/bin/env python3
"""
Simple Kraken WebSocket v2 Test.

Connects to Kraken's WebSocket v2 API and prints live ticker data.
This verifies the connection works before integrating into the pipeline.

Kraken WebSocket v2 docs:
    https://docs.kraken.com/api/docs/websocket-v2/ticker

Usage:
    python test_kraken_ws_simple.py
    
Press Ctrl+C to stop.
"""

import asyncio
import json
import websockets
from datetime import datetime


# Kraken WebSocket v2 endpoint
KRAKEN_WS_URL = "wss://ws.kraken.com/v2"

# Pairs to subscribe to (Kraken v2 uses "BTC/USD" format)
TEST_PAIRS = [
    "BTC/USD",
    "ETH/USD",
    "SOL/USD",
    "XRP/USD",
    "ADA/USD",
    "DOGE/USD",
    "AVAX/USD",
    "LINK/USD",
    "DOT/USD",
    "LTC/USD",
]


async def test_kraken_ws():
    print("=" * 70)
    print("  Kraken WebSocket v2 Test")
    print("=" * 70)
    print(f"  URL: {KRAKEN_WS_URL}")
    print(f"  Pairs: {TEST_PAIRS}")
    print("=" * 70)
    print()

    try:
        async with websockets.connect(KRAKEN_WS_URL, ping_interval=30) as ws:
            # Subscribe to ticker channel (v2 format)
            subscribe_msg = {
                "method": "subscribe",
                "params": {
                    "channel": "ticker",
                    "symbol": TEST_PAIRS,
                },
            }
            await ws.send(json.dumps(subscribe_msg))
            print("✓ Sent subscribe message")
            print()
            print("Waiting for ticker updates (Ctrl+C to stop)...")
            print("-" * 70)

            tick_count = 0
            async for message in ws:
                data = json.loads(message)

                # Handle subscription confirmation
                if data.get("method") == "subscribe":
                    result = data.get("result", {})
                    if data.get("success"):
                        print(f"✓ Subscribed to {result.get('channel', 'ticker')} channel")
                    else:
                        print(f"✗ Subscription failed: {data.get('error')}")
                    continue

                # Handle heartbeat
                if data.get("channel") == "heartbeat":
                    continue

                # Handle ticker messages
                channel = data.get("channel")
                msg_type = data.get("type")

                if channel != "ticker":
                    continue

                if msg_type not in ("update", "snapshot"):
                    continue

                for item in data.get("data", []):
                    tick_count += 1
                    symbol = item.get("symbol", "???")
                    bid = item.get("bid", 0)
                    ask = item.get("ask", 0)
                    last = item.get("last", 0)
                    vwap = item.get("vwap", 0)
                    volume = item.get("volume", 0)

                    # Format timestamp
                    time_display = datetime.now().strftime("%H:%M:%S")

                    # Format price nicely
                    try:
                        price_float = float(last)
                        if price_float >= 1000:
                            price_str = f"${price_float:,.2f}"
                        elif price_float >= 1:
                            price_str = f"${price_float:.4f}"
                        else:
                            price_str = f"${price_float:.6f}"
                    except:
                        price_str = str(last)

                    print(
                        f"  [{time_display}] {symbol:12} │ "
                        f"Last: {price_str:>14} │ "
                        f"Bid: {float(bid):>12,.4f} │ "
                        f"Ask: {float(ask):>12,.4f} │ "
                        f"VWAP: {float(vwap):>12,.2f}"
                    )

                    # After 50 ticks, print summary and continue
                    if tick_count == 50:
                        print("-" * 70)
                        print(f"  Received {tick_count} ticks so far... (continuing)")
                        print("-" * 70)

    except websockets.ConnectionClosed as e:
        print(f"\n✗ Connection closed: {e}")
    except KeyboardInterrupt:
        print(f"\n\n✓ Test complete. Received {tick_count} ticks total.")


if __name__ == "__main__":
    asyncio.run(test_kraken_ws())
