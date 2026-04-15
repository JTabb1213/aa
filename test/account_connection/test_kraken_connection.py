#!/usr/bin/env python3
"""
Kraken account connection test.

Loads credentials from test/.env and fetches live balances.

Usage:
    python test_kraken_connection.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "realtime_with_ticker"))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from execution.kraken import KrakenClient


async def main() -> None:
    api_key = os.getenv("KRAKEN_API_KEY", "")
    api_secret = os.getenv("KRAKEN_API_SECRET", "")

    print("=" * 60)
    print("  Kraken Account Connection Test")
    print("=" * 60)
    print(f"  API Key:  {'***' + api_key[-4:] if len(api_key) > 4 else '(empty)'}")
    print("=" * 60)
    print()

    client = KrakenClient(api_key=api_key, api_secret=api_secret, dry_run=False)

    try:
        balances = await client.get_balances()
        if balances:
            print("Balances:")
            for asset, amount in sorted(balances.items()):
                print(f"  {asset:<6} {amount:>14.6f}")
        else:
            print("(no balances returned)")
        print("\n✓ Kraken connection test completed.")
    except Exception as exc:
        print(f"✗ Kraken connection test failed: {exc}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n✓ Test stopped by user")
