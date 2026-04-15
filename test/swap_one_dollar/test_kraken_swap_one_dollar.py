#!/usr/bin/env python3
"""
Kraken $2 swap test.

Reads test/.env and performs a real $2 market swap based on these flags:
  TEST_SWAP_ONE_DOLLAR_CARDANO_TO_USDT=true   → sells $2 worth of ADA for USDT
  TEST_SWAP_ONE_DOLLAR_USDT_TO_CARDANO=true   → buys $2 worth of ADA with USDT

Set the flag you want to true, leave the other false.

Usage:
    python "test_kraken_swap_$1.py"
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "realtime_with_ticker"))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from execution.kraken import KrakenClient
from execution.types import OrderRequest, OrderSide, OrderType


async def main() -> None:
    api_key    = os.getenv("KRAKEN_API_KEY", "")
    api_secret = os.getenv("KRAKEN_API_SECRET", "")
    ada_to_usdt = os.getenv("TEST_SWAP_ONE_DOLLAR_CARDANO_TO_USDT", "false").lower() in ("1", "true", "yes", "on")
    usdt_to_ada = os.getenv("TEST_SWAP_ONE_DOLLAR_USDT_TO_CARDANO", "false").lower() in ("1", "true", "yes", "on")

    print("=" * 60)
    print("  Kraken $1 Swap Test")
    print("=" * 60)
    print(f"  API Key:                    {'***' + api_key[-4:] if len(api_key) > 4 else '(empty)'}")
    print(f"  TEST_SWAP_ONE_DOLLAR_CARDANO_TO_USDT: {ada_to_usdt}")
    print(f"  TEST_SWAP_ONE_DOLLAR_USDT_TO_CARDANO: {usdt_to_ada}")
    print("=" * 60)
    print()

    if not ada_to_usdt and not usdt_to_ada:
        print("Both swap flags are false. Set one to true in test/.env to run a swap.")
        return

    if ada_to_usdt and usdt_to_ada:
        print("Both swap flags are true. Please only enable one at a time.")
        return

    client = KrakenClient(api_key=api_key, api_secret=api_secret, dry_run=False)

    # Fetch current ADA price from Kraken to calculate volume for sell
    ada_price = None
    if ada_to_usdt:
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.kraken.com/0/public/Ticker?pair=ADAUSDT",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()
            pair_data = list(data.get("result", {}).values())
            if pair_data:
                ada_price = float(pair_data[0]["c"][0])
                print(f"  Current ADA price: ${ada_price:.6f}")
        except Exception as e:
            print(f"  ✗ Failed to fetch ADA price: {e}")
            return

    swap_usd = 5.0
    if ada_to_usdt:
        print("Swapping $2 of ADA → USDT (market sell)")
        req = OrderRequest(
            exchange="kraken",
            coin_id="cardano",
            side=OrderSide.SELL,
            quote="usdt",
            size_usd=swap_usd,
            order_type=OrderType.MARKET,
            limit_price=ada_price,
        )
    else:
        print("Swapping $2 of USDT → ADA (market buy)")
        req = OrderRequest(
            exchange="kraken",
            coin_id="cardano",
            side=OrderSide.BUY,
            quote="usdt",
            size_usd=swap_usd,
            order_type=OrderType.MARKET,
        )

    try:
        result = await client.create_order(req)
        print(f"\n  Status:   {result.status.value}")
        print(f"  Order ID: {result.order_id}")
        if result.filled_qty:
            print(f"  Filled:   {result.filled_qty:.8f} @ ${result.filled_price:.6f}")
        if result.error:
            print(f"  Error:    {result.error}")
        print("\n✓ Kraken swap test completed.")
    except Exception as exc:
        print(f"\n✗ Kraken swap test failed: {exc}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n✓ Test stopped by user")
