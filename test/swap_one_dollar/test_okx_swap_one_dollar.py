#!/usr/bin/env python3
"""
OKX $2 swap test.

Reads test/.env and performs a real $2 market swap based on these flags:
  TEST_SWAP_ONE_DOLLAR_CARDANO_TO_USDT=true   → sells $2 worth of ADA for USDT
  TEST_SWAP_ONE_DOLLAR_USDT_TO_CARDANO=true   → buys $2 worth of ADA with USDT

Set the flag you want to true, leave the other false.

Usage:
    python test_okx_swap_one_dollar.py
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))


def sign_okx_request(timestamp: str, method: str, path: str, body: str, api_secret: str) -> str:
    pre_sign = f"{timestamp}{method.upper()}{path}{body}"
    return base64.b64encode(
        hmac.new(api_secret.encode(), pre_sign.encode(), hashlib.sha256).digest()
    ).decode()


async def place_okx_order(api_key: str, api_secret: str, passphrase: str, body: dict) -> dict:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    path = "/api/v5/trade/order"
    body_str = json.dumps(body)
    signature = sign_okx_request(timestamp, "POST", path, body_str, api_secret)
    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
    }
    url = f"https://us.okx.com{path}"

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=body_str, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            text = await resp.text()
            try:
                return {"status": resp.status, "body": await resp.json(), "text": text}
            except Exception:
                return {"status": resp.status, "body": None, "text": text}


async def main() -> None:
    api_key = os.getenv("OKX_API_KEY", "")
    api_secret = os.getenv("OKX_API_SECRET", "")
    passphrase = os.getenv("OKX_PASSPHRASE", "")
    ada_to_usdt = os.getenv("TEST_SWAP_ONE_DOLLAR_CARDANO_TO_USDT", "false").lower() in ("1", "true", "yes", "on")
    usdt_to_ada = os.getenv("TEST_SWAP_ONE_DOLLAR_USDT_TO_CARDANO", "false").lower() in ("1", "true", "yes", "on")

    print("=" * 60)
    print("  OKX $1 Swap Test")
    print("=" * 60)
    print(f"  API Key:                    {'***' + api_key[-4:] if len(api_key) > 4 else '(empty)'}")
    print(f"  Passphrase:                 {'***' if passphrase else '(empty)'}")
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

    swap_usd = 5.0
    ada_price = None
    if ada_to_usdt:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://us.okx.com/api/v5/market/ticker?instId=ADA-USDT",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    ticker = await resp.json()
            ada_price = float(ticker.get("data", [{}])[0].get("last", 0))
            print(f"  Current ADA price: ${ada_price:.6f}")
        except Exception as e:
            print(f"  ✗ Failed to fetch ADA price: {e}")
            return

    if ada_to_usdt:
        print("Swapping $2 of ADA → USDT (market sell)")
        body = {
            "instId": "ADA-USDT",
            "tdMode": "cash",
            "side": "sell",
            "ordType": "market",
            "tgtCcy": "base_ccy",
            "sz": str(round(swap_usd / ada_price, 8)),
        }
    else:
        print("Swapping $2 of USDT → ADA (market buy)")
        body = {
            "instId": "ADA-USDT",
            "tdMode": "cash",
            "side": "buy",
            "ordType": "market",
            "tgtCcy": "quote_ccy",
            "sz": str(swap_usd),
        }

    result = await place_okx_order(api_key, api_secret, passphrase, body)
    print(f"\n  URL: https://us.okx.com/api/v5/trade/order")
    print(f"  HTTP status: {result['status']}")
    if result["body"] is not None:
        print(f"  Response: {json.dumps(result['body'], indent=2)}")
    else:
        print(f"  Response text: {result['text']}")

    if result["status"] != 200 or (result["body"] and result["body"].get("code", "0") != "0"):
        print("\n✗ OKX swap test failed.")
        return

    data = result["body"].get("data", [{}])[0]
    print(f"  Order ID: {data.get('ordId')}")
    print("\n✓ OKX swap test completed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n✓ Test stopped by user")
