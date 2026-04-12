"""
Smoke-test for the GateioClient execution client.

Tests:
  1. Symbol mapping  — correct BTC_USDT / NEAR_USDT / etc. format
  2. Coin map        — JSON loading covers all 13 tracked coins
  3. Dry-run order   — simulates a buy order without hitting the API
  4. Balance mock    — confirms dry-run returns sensible simulated balances

Usage:
    cd /Users/jacktabb/Desktop/aa/realtime
    python ../test/test_gateio_execution.py

No API keys required — all tests run in dry-run mode.
"""

import asyncio
import sys
import os

# Add realtime/ to sys.path so imports work when run from test/ or realtime/
_here = os.path.dirname(os.path.abspath(__file__))
_realtime = os.path.join(_here, "..", "realtime")
if _realtime not in sys.path:
    sys.path.insert(0, _realtime)

from execution.gateio import GateioClient
from execution.types import OrderRequest, OrderSide, OrderType, OrderStatus


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

def ok(label: str) -> None:
    print(f"  ✓  {label}")


def fail(label: str, detail: str = "") -> None:
    print(f"  ✗  {label}" + (f": {detail}" if detail else ""))


# ─────────────────────────────────────────
# Tests
# ─────────────────────────────────────────

def test_symbol_mapping():
    """Verify coin_id → Gate.io symbol conversion."""
    client = GateioClient(dry_run=True)
    cases = [
        ("bitcoin",             "usdt", "BTC_USDT"),
        ("ethereum",            "usdt", "ETH_USDT"),
        ("solana",              "usdt", "SOL_USDT"),
        ("near",                "usdt", "NEAR_USDT"),
        ("filecoin",            "usdt", "FIL_USDT"),
        ("injective-protocol",  "usdt", "INJ_USDT"),
        ("aave",                "usdt", "AAVE_USDT"),
        ("optimism",            "usdt", "OP_USDT"),
        ("nonexistent-coin",    "usdt", None),
    ]
    passed = True
    for coin_id, quote, expected in cases:
        got = client.get_exchange_symbol(coin_id, quote)
        if got == expected:
            ok(f"  {coin_id:25s} → {got}")
        else:
            fail(f"  {coin_id:25s}", f"expected={expected!r}, got={got!r}")
            passed = False
    return passed


def test_coin_map_coverage():
    """All 13 tracked coins should be in the coin_map after JSON load."""
    client = GateioClient(dry_run=True)
    expected_coins = [
        "bitcoin", "ethereum", "solana", "ripple", "cardano",
        "dogecoin", "avalanche-2", "litecoin",
        "near", "filecoin", "injective-protocol", "aave", "optimism",
    ]
    passed = True
    for coin_id in expected_coins:
        if coin_id in client._coin_map:
            ok(f"  {coin_id:25s} → {client._coin_map[coin_id]}")
        else:
            fail(f"  {coin_id:25s} missing from coin_map")
            passed = False
    return passed


async def test_dry_run_order():
    """Dry-run BUY order should return FILLED status immediately."""
    client = GateioClient(dry_run=True)
    request = OrderRequest(
        exchange="gateio",
        coin_id="bitcoin",
        quote="usdt",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        size_usd=500.0,
        limit_price=72000.0,
    )
    result = await client.create_order(request)
    passed = result.status == OrderStatus.FILLED
    if passed:
        ok(f"  BUY bitcoin  status={result.status.value}  order_id={result.order_id!r}")
    else:
        fail("  Dry-run BUY failed", f"status={result.status.value}")
    return passed


async def test_dry_run_balances():
    """Dry-run balances should return a dict with positive USD value."""
    client = GateioClient(dry_run=True)
    balances = await client.get_balances()
    passed = isinstance(balances, dict) and balances.get("usd", 0) > 0
    if passed:
        ok(f"  Dry-run balances: {balances}")
    else:
        fail("  Dry-run balances returned unexpected result", str(balances))
    return passed


def test_signing():
    """Verify that the SIGN header is produced without exceptions."""
    client = GateioClient(api_key="testkey", api_secret="testsecret", dry_run=True)
    try:
        headers = client._sign("POST", "/spot/orders", "", '{"currency_pair":"BTC_USDT"}')
        has_keys = all(k in headers for k in ("KEY", "Timestamp", "SIGN"))
        if has_keys:
            ok(f"  SIGN header generated OK  KEY={headers['KEY']!r}")
        else:
            fail("  Missing expected headers", str(headers.keys()))
        return has_keys
    except Exception as e:
        fail("  Signing raised exception", str(e))
        return False


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────

async def main():
    print(f"\n{'='*60}")
    print("  GateioClient — execution smoke-test")
    print(f"{'='*60}\n")

    results = []

    print("1. Symbol mapping")
    results.append(test_symbol_mapping())

    print("\n2. Coin map coverage (13 coins from JSON)")
    results.append(test_coin_map_coverage())

    print("\n3. Dry-run BUY order")
    results.append(await test_dry_run_order())

    print("\n4. Dry-run balances")
    results.append(await test_dry_run_balances())

    print("\n5. HMAC-SHA512 signing")
    results.append(test_signing())

    passed = sum(results)
    total = len(results)
    print(f"\n{'='*60}")
    print(f"  Results: {passed}/{total} passed")
    print(f"  {'✓ ALL PASS' if passed == total else '✗ SOME FAILURES'}")
    print(f"{'='*60}\n")

    return passed == total


if __name__ == "__main__":
    ok_ = asyncio.run(main())
    sys.exit(0 if ok_ else 1)
