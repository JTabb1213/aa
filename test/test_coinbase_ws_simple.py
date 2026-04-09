#!/usr/bin/env python3
"""
Simple Coinbase WebSocket Test.

Connects to Coinbase's WebSocket API and prints live ticker data.
This verifies the connection works before integrating into the pipeline.

Coinbase WebSocket docs:
    https://docs.cdp.coinbase.com/exchange/docs/websocket-channels#ticker-channel

Usage:
    python test_coinbase_ws_simple.py
    
Press Ctrl+C to stop.
"""

import asyncio
import json
import websockets
from datetime import datetime


# Coinbase WebSocket endpoint
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"

# Product IDs to subscribe to (Coinbase uses "BTC-USD" format)
TEST_PRODUCTS = [
    "BTC-USD",
    "ETH-USD",
    "SOL-USD",
    "XRP-USD",
    "ADA-USD",
    "DOGE-USD",
    "AVAX-USD",
    "LINK-USD",
    "DOT-USD",
    "LTC-USD",
]


async def test_coinbase_ws():
    print("=" * 70)
    print("  Coinbase WebSocket Test")
    print("=" * 70)
    print(f"  URL: {COINBASE_WS_URL}")
    print(f"  Products: {TEST_PRODUCTS}")
    print("=" * 70)
    print()

    try:
        async with websockets.connect(COINBASE_WS_URL, ping_interval=30) as ws:
            # Subscribe to ticker channel
            subscribe_msg = {
                "type": "subscribe",
                "product_ids": TEST_PRODUCTS,
                "channels": ["ticker"],
            }
            await ws.send(json.dumps(subscribe_msg))
            print("✓ Sent subscribe message")
            print()
            print("Waiting for ticker updates (Ctrl+C to stop)...")
            print("-" * 70)

            tick_count = 0
            async for message in ws:
                data = json.loads(message)

                msg_type = data.get("type")

                # Handle subscription confirmation
                if msg_type == "subscriptions":
                    channels = data.get("channels", [])
                    for ch in channels:
                        print(f"✓ Subscribed to {ch.get('name', 'ticker')} channel")
                    continue

                # Handle errors
                if msg_type == "error":
                    print(f"✗ Error: {data.get('message')}")
                    continue

                # Handle ticker messages
                if msg_type != "ticker":
                    continue

                tick_count += 1
                product_id = data.get("product_id", "???")
                price = data.get("price", "0")
                best_bid = data.get("best_bid", "0")
                best_ask = data.get("best_ask", "0")
                volume_24h = data.get("volume_24h", "0")
                side = data.get("side", "?")
                
                # Format timestamp
                time_display = datetime.now().strftime("%H:%M:%S")

                # Format price nicely
                try:
                    price_float = float(price)
                    if price_float >= 1000:
                        price_str = f"${price_float:,.2f}"
                    elif price_float >= 1:
                        price_str = f"${price_float:.4f}"
                    else:
                        price_str = f"${price_float:.6f}"
                except:
                    price_str = str(price)

                print(
                    f"  [{time_display}] {product_id:12} │ "
                    f"Last: {price_str:>14} │ "
                    f"Bid: {float(best_bid):>12,.4f} │ "
                    f"Ask: {float(best_ask):>12,.4f} │ "
                    f"Side: {side:>4}"
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
    asyncio.run(test_coinbase_ws())
