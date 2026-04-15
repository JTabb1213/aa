#!/usr/bin/env python3
"""
OKX account connection test.

Loads credentials from test/.env and fetches live balances.

Usage:
    python test_okx_connection.py
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


async def test_okx() -> None:
    api_key = os.getenv("OKX_API_KEY", "")
    api_secret = os.getenv("OKX_API_SECRET", "")
    passphrase = os.getenv("OKX_PASSPHRASE", "")

    print("=" * 60)
    print("  OKX Account Connection Test")
    print("=" * 60)
    print(f"  API Key:     {'***' + api_key[-4:] if len(api_key) > 4 else '(empty)'}")
    print(f"  Passphrase:  {'***' if passphrase else '(empty)'}")
    print("=" * 60)
    print()

    def sign_request(timestamp: str, method: str, path: str, body: str = "") -> dict:
        pre_sign = f"{timestamp}{method.upper()}{path}{body}"
        sig = base64.b64encode(
            hmac.new(
                api_secret.encode(),
                pre_sign.encode(),
                hashlib.sha256,
            ).digest()
        ).decode()
        return {
            "OK-ACCESS-KEY": api_key,
            "OK-ACCESS-SIGN": sig,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": passphrase,
            "Content-Type": "application/json",
        }

    try:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        path = "/api/v5/account/balance"
        headers = sign_request(timestamp, "GET", path)
        url = f"https://us.okx.com{path}"

        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                result = await resp.json()

        if result.get("code", "0") != "0":
            print(f"✗ OKX connection test failed: OKX API error {result.get('code')}: {result.get('msg')}")
            return

        balances_data = result.get("data", [{}])[0].get("details", [])
        if balances_data:
            print("Balances:")
            for item in balances_data:
                asset = item.get("ccy", "").lower()
                available = float(item.get("availBal", 0))
                if available > 0:
                    print(f"  {asset:<6} {available:>14.6f}")
        else:
            print("(no balances returned)")

        print("\n✓ OKX connection test completed.")
    except Exception as exc:
        print(f"✗ OKX connection test failed: {exc}")


if __name__ == "__main__":
    try:
        asyncio.run(test_okx())
    except KeyboardInterrupt:
        print("\n✓ Test stopped by user")
