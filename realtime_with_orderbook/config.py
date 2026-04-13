"""
Configuration for the orderbook-based arbitrage engine.

All settings are loaded from environment variables with sensible defaults.
See .env for documentation on each setting.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Alias file
# ---------------------------------------------------------------------------
ALIAS_JSON_PATH = os.getenv(
    "ALIAS_JSON_PATH",
    os.path.join(os.path.dirname(__file__), "..", "data", "coin_aliases.json"),
)

# ---------------------------------------------------------------------------
# Mode: TEST (dry run) vs LIVE (real trades)
# ---------------------------------------------------------------------------
# TEST_MODE=true  → log signals + simulated fills, no real orders
# TEST_MODE=false → execute real trades via exchange APIs
TEST_MODE = os.getenv("TEST_MODE", "true").strip().lower() in (
    "1", "true", "yes", "on",
)

# Alias for code clarity
DRY_RUN = TEST_MODE

# Auto-execute detected arbitrage signals.  When false the engine still
# prints/logs signals, but no orders (real or simulated) are placed.
AUTO_EXECUTE = os.getenv("AUTO_EXECUTE", "false").strip().lower() in (
    "1", "true", "yes", "on",
)

# ---------------------------------------------------------------------------
# Orderbook depth settings
# ---------------------------------------------------------------------------
# Number of price levels to request from each exchange orderbook.
# 5 is enough for small trades ($100).  Increase for bigger size.
ORDERBOOK_DEPTH = int(os.getenv("ORDERBOOK_DEPTH", "5"))

# ---------------------------------------------------------------------------
# Trade sizing
# ---------------------------------------------------------------------------
# Maximum USD per trade leg.
TRADE_SIZE_USD = float(os.getenv("TRADE_SIZE_USD", "100"))

# Hard cap — overrides TRADE_SIZE_USD if set > 0
MAX_TRADE_SIZE_USD = float(os.getenv("MAX_TRADE_SIZE_USD", "100"))

# ---------------------------------------------------------------------------
# Arbitrage strategy settings
# ---------------------------------------------------------------------------
# How often the scanner checks for opportunities (milliseconds)
SCAN_INTERVAL_MS = int(os.getenv("SCAN_INTERVAL_MS", "500"))

# Minimum NET spread (after fees) to consider actionable.  Percentage.
MIN_NET_SPREAD_PCT = float(os.getenv("MIN_NET_SPREAD_PCT", "0.05"))

# Print each signal to terminal
SIGNAL_PRINT_ENABLED = os.getenv("SIGNAL_PRINT_ENABLED", "true").strip().lower() in (
    "1", "true", "yes", "on",
)

# Live terminal table
LIVE_TABLE = os.getenv("LIVE_TABLE", "true").strip().lower() in (
    "1", "true", "yes", "on",
)

# ---------------------------------------------------------------------------
# Quote currencies
# ---------------------------------------------------------------------------
QUOTE_CURRENCIES = [
    q.strip().upper()
    for q in os.getenv("QUOTE_CURRENCIES", "USDT").split(",")
]

# ---------------------------------------------------------------------------
# Exchange websocket URLs
# ---------------------------------------------------------------------------
KRAKEN_WS_URL = os.getenv("KRAKEN_WS_URL", "wss://ws.kraken.com/v2")
COINBASE_WS_URL = os.getenv("COINBASE_WS_URL", "wss://advanced-trade-ws.coinbase.com")
BINANCE_WS_URL = os.getenv("BINANCE_WS_URL", "wss://stream.binance.us:9443")
BYBIT_WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/spot")
OKX_WS_URL = os.getenv("OKX_WS_URL", "wss://ws.okx.com:8443/ws/v5/public")
PIONEX_WS_URL = os.getenv("PIONEX_WS_URL", "wss://ws.pionex.us/wsPub")
MEXC_WS_URL = os.getenv("MEXC_WS_URL", "wss://wbs.mexc.com/ws")
GATEIO_WS_URL = os.getenv("GATEIO_WS_URL", "wss://api.gateio.ws/ws/v4/")

# ---------------------------------------------------------------------------
# Coinbase Advanced Trade JWT auth (required for level2 orderbook)
# ---------------------------------------------------------------------------
COINBASE_ADT_KEY_ID = os.getenv("COINBASE_ADT_KEY_ID", "")
COINBASE_ADT_PRIVATE_KEY = os.getenv("COINBASE_ADT_PRIVATE_KEY", "")
COINBASE_ADT_PRIVATE_KEY_PATH = os.getenv("COINBASE_ADT_PRIVATE_KEY_PATH", "")

# ---------------------------------------------------------------------------
# Exchange API credentials (for execution — not needed in test mode)
# ---------------------------------------------------------------------------
KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY", "")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET", "")

COINBASE_API_KEY = os.getenv("COINBASE_API_KEY", "")
COINBASE_API_SECRET = os.getenv("COINBASE_API_SECRET", "")

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")

OKX_API_KEY = os.getenv("OKX_API_KEY", "")
OKX_API_SECRET = os.getenv("OKX_API_SECRET", "")
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE", "")

PIONEX_API_KEY = os.getenv("PIONEX_API_KEY", "")
PIONEX_API_SECRET = os.getenv("PIONEX_API_SECRET", "")

MEXC_API_KEY = os.getenv("MEXC_API_KEY", "")
MEXC_API_SECRET = os.getenv("MEXC_API_SECRET", "")

GATEIO_API_KEY = os.getenv("GATEIO_API_KEY", "")
GATEIO_API_SECRET = os.getenv("GATEIO_API_SECRET", "")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ---------------------------------------------------------------------------
# Health check server
# ---------------------------------------------------------------------------
HEALTH_PORT = int(os.getenv("PORT", "8082"))
