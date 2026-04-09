#!/usr/bin/env python3
"""
OKX Execution Test.

Tests the OKX v5 REST trading endpoints to verify auth + connectivity.
Runs in DRY-RUN mode by default (no real orders). Set DRY_RUN=false
in .env to test against the live API with real credentials.

Usage:
    python test_okx_execution.py

Press Ctrl+C to stop.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "realtime"))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "realtime", ".env"))

from execution.okx import OKXClient
from execution.types import OrderRequest, OrderSide, OrderType


async def test_okx():
    api_key = os.getenv("OKX_API_KEY", "")
    api_secret = os.getenv("OKX_API_SECRET", "")
    passphrase = os.getenv("OKX_PASSPHRASE", "")
    dry_run = os.getenv("DRY_RUN", "true").lower() in ("1", "true", "yes", "on")

    print("=" * 60)
    print("  OKX Execution Test")
    print("=" * 60)
    print(f"  API Key:     {'***' + api_key[-4:] if len(api_key) > 4 else '(empty)'}")
    print(f"  Passphrase:  {'***' if passphrase else '(empty)'}")
    print(f"  Dry Run:     {dry_run}")
    print("=" * 60)
    print()

    client = OKXClient(
        api_key=api_key, api_secret=api_secret,
        passphrase=passphrase, dry_run=dry_run,
    )

    # --- Test 1: Symbol mapping ---
    print("Test 1: Symbol mapping")
    for coin in ["bitcoin", "ethereum", "solana", "dogecoin"]:
        sym = client.get_exchange_symbol(coin)
        print(f"  {coin:<15} → {sym}")
    print()

    # --- Test 2: Balances ---
    print("Test 2: Fetch balances")
    try:
        balances = await client.get_balances()
        if balances:
            for asset, amount in sorted(balances.items()):
                print(f"  {asset:<6} {amount:>14.6f}")
        else:
            print("  (no balances or dry-run)")
        print("  ✓ Balances OK")
    except Exception as e:
        print(f"  ✗ Balances failed: {e}")
    print()

    # --- Test 3: Dry-run order ---
    print("Test 3: Place test order (market buy $10 of BTC)")
    try:
        req = OrderRequest(
            exchange="okx",
            coin_id="bitcoin",
            side=OrderSide.BUY,
            quote="usd",
            size_usd=10.0,
            order_type=OrderType.MARKET,
            limit_price=65000.0,
        )
        result = await client.create_order(req)
        print(f"  Status: {result.status.value}")
        print(f"  Order ID: {result.order_id}")
        print(f"  Filled: {result.filled_qty:.8f} @ ${result.filled_price:.2f}")
        print(f"  ✓ Order OK")
    except Exception as e:
        print(f"  ✗ Order failed: {e}")
    print()

    print("Done!")


if __name__ == "__main__":
    try:
        asyncio.run(test_okx())
    except KeyboardInterrupt:
        print("\n✓ Test stopped by user")
