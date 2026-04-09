#!/usr/bin/env python3
"""
Kraken WS v2 subscription acceptance test.

For every coin in coin_aliases.json that has a Kraken exchange symbol, this
script builds the corresponding "/USD" pair, sends a subscribe request to
the Kraken WS v2 ticker channel, checks whether Kraken accepts or rejects
it, then drops the connection immediately.

The goal is to surface any symbol mismatches — e.g. a pair still stored as
"XBT/USD" when the v2 API expects "BTC/USD".

Usage
-----
    # From the project root:
    python tools/test_connections/test_kraken_ws.py

    # Custom path to coin_aliases.json:
    python tools/test_connections/test_kraken_ws.py --json-path /path/to/coin_aliases.json

Output
------
    ════════════════════════════════════════════
    RESULTS: 671 connected  |  7 failed
    ════════════════════════════════════════════

    FAILED PAIRS:
      ✗  FOO/USD   error: Currency pair not supported
      ✗  BAR/USD   timeout (no subscription ack received)
      ...

Dependencies (install if missing):
    pip install websockets
"""

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

try:
    import websockets
except ImportError:
    print("ERROR: 'websockets' package not found.  Run:  pip install websockets")
    sys.exit(1)

# ── Constants ────────────────────────────────────────────────────────────────

WS_URL = "wss://ws.kraken.com/v2"

# Max pairs per WS connection (matches the realtime connector default).
# Smaller chunks give faster per-symbol ack processing.
CHUNK_SIZE = 50

# Seconds to collect subscription confirmations before giving up on a chunk.
# Kraken sends one ack per symbol; with 50 symbols this is plenty.
SUBSCRIBE_TIMEOUT = 15

# Known v1→v2 symbol translations (mirrors realtime/exchanges/kraken.py).
# Pairs stored in coin_aliases.json under exchange_symbols["kraken"] may still
# contain these v1 codes; translate them before testing.
V1_TO_V2 = {
    "XBT": "BTC",
    "XDG": "DOGE",
}

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DEFAULT_JSON_PATH = ROOT_DIR / "data" / "coin_aliases.json"


# ── Pair loading ─────────────────────────────────────────────────────────────

def load_pairs(json_path: Path) -> list[str]:
    """
    Read coin_aliases.json and return all Kraken USD pairs in v2 format.

    e.g. entry with exchange_symbols["kraken"] = "XBT"  → "BTC/USD"
         entry with exchange_symbols["kraken"] = "ETH"  → "ETH/USD"
    """
    with open(json_path) as f:
        data = json.load(f)

    pairs: list[str] = []
    for entry in data.get("assets", {}).values():
        kraken_sym: str | None = entry.get("exchange_symbols", {}).get("kraken")
        if not kraken_sym:
            continue
        base = V1_TO_V2.get(kraken_sym, kraken_sym)
        pairs.append(f"{base}/USD")

    return sorted(set(pairs))


# ── WebSocket test ────────────────────────────────────────────────────────────

async def test_chunk(
    conn_id: int,
    pairs: list[str],
    results: dict[str, str],
) -> None:
    """
    Open one WS connection, subscribe to ticker for all pairs in the chunk,
    collect per-symbol subscription acks, then close.

    Kraken WS v2 sends one response object per symbol:
      success=true  → {"method":"subscribe","result":{"channel":"ticker","symbol":"BTC/USD"},"success":true,...}
      success=false → {"method":"subscribe","error":"Currency pair not supported","success":false,...}

    When success=false there is no result.symbol, so we record it as an
    unattributed error against whichever pairs in the chunk are still pending.
    """
    # Mark all pairs in this chunk as pending
    for p in pairs:
        results[p] = "pending"

    subscribe_msg = json.dumps({
        "method": "subscribe",
        "params": {
            "channel": "ticker",
            "symbol": pairs,
            "snapshot": False,
        },
    })

    try:
        async with websockets.connect(WS_URL, ping_interval=20, open_timeout=10) as ws:
            await ws.send(subscribe_msg)

            deadline = time.monotonic() + SUBSCRIBE_TIMEOUT
            pending = set(pairs)

            while pending and time.monotonic() < deadline:
                timeout_remaining = deadline - time.monotonic()
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=min(2.0, timeout_remaining))
                except asyncio.TimeoutError:
                    break

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                if msg.get("method") != "subscribe":
                    continue  # skip heartbeats, status, ticker data

                success: bool = msg.get("success", False)
                result_obj: dict = msg.get("result", {})
                symbol: str | None = result_obj.get("symbol")
                error_text: str = msg.get("error", "unknown error")

                if success and symbol:
                    results[symbol] = "ok"
                    pending.discard(symbol)
                elif not success:
                    if symbol:
                        # Rare: Kraken included the symbol in the error response
                        results[symbol] = f"error: {error_text}"
                        pending.discard(symbol)
                    else:
                        # Can't attribute to a specific pair — mark first pending
                        # pair as errored so the list shrinks and we don't loop
                        # forever; the remaining pending will timeout naturally.
                        if pending:
                            p = next(iter(pending))
                            results[p] = f"error: {error_text}"
                            pending.discard(p)

    except Exception as e:
        err_msg = f"connection error: {e}"
        for p in pairs:
            if results.get(p) == "pending":
                results[p] = err_msg
        return

    # Anything still pending after the timeout got no ack
    for p in pending:
        if results.get(p) == "pending":
            results[p] = "timeout (no subscription ack received)"


# ── Main ─────────────────────────────────────────────────────────────────────

async def run(json_path: Path) -> None:
    print(f"\nLoading pairs from:  {json_path}")
    pairs = load_pairs(json_path)

    if not pairs:
        print("No Kraken pairs found in the alias file — nothing to test.")
        return

    print(f"Pairs to test:       {len(pairs)}")
    chunks = [pairs[i : i + CHUNK_SIZE] for i in range(0, len(pairs), CHUNK_SIZE)]
    print(f"WS connections:      {len(chunks)}  ({CHUNK_SIZE} pairs/chunk)\n")

    results: dict[str, str] = {}

    for i, chunk in enumerate(chunks):
        print(f"  [{i + 1:>3}/{len(chunks)}] {len(chunk)} pairs ... ", end="", flush=True)
        await test_chunk(i, chunk, results)
        ok_so_far = sum(1 for v in results.values() if v == "ok")
        print(f"ok so far: {ok_so_far}")
        # Small pause between connections — avoids hammering the endpoint
        await asyncio.sleep(0.3)

    # ── Final report ─────────────────────────────────────────────────────────
    ok_pairs     = sorted(p for p, v in results.items() if v == "ok")
    failed_pairs = sorted(p for p, v in results.items() if v != "ok")

    width = 60
    print("\n" + "═" * width)
    print(f"  RESULTS:  {len(ok_pairs)} connected   |   {len(failed_pairs)} failed")
    print("═" * width)

    if failed_pairs:
        print(f"\n{'FAILED PAIRS':}")
        col = max(len(p) for p in failed_pairs) + 2
        for p in failed_pairs:
            print(f"  ✗  {p:<{col}}  {results[p]}")
    else:
        print("\n  ✓  All pairs accepted by Kraken WS v2!\n")

    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Test Kraken WS v2 subscription acceptance for all alias pairs."
    )
    parser.add_argument(
        "--json-path",
        type=Path,
        default=DEFAULT_JSON_PATH,
        metavar="PATH",
        help=f"Path to coin_aliases.json  (default: {DEFAULT_JSON_PATH})",
    )
    args = parser.parse_args()

    if not args.json_path.exists():
        print(f"ERROR: File not found: {args.json_path}")
        sys.exit(1)

    asyncio.run(run(args.json_path))


if __name__ == "__main__":
    main()
