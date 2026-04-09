"""
Configuration for the realtime market data ingestion server.

All settings are loaded from environment variables with sensible defaults.
See .env.example for documentation on each setting.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Alias file
# ---------------------------------------------------------------------------
# Path to the shared coin_aliases.json used across the whole project.
# Default assumes this server lives at <project-root>/realtime/
ALIAS_JSON_PATH = os.getenv(
    "ALIAS_JSON_PATH",
    os.path.join(os.path.dirname(__file__), "..", "data", "coin_aliases.json"),
)

# ---------------------------------------------------------------------------
# Kraken exchange settings
# ---------------------------------------------------------------------------
KRAKEN_WS_URL = os.getenv("KRAKEN_WS_URL", "wss://ws.kraken.com/v2")
KRAKEN_REST_URL = os.getenv(
    "KRAKEN_REST_URL", "https://api.kraken.com/0/public/AssetPairs",
)
# Max pairs per websocket connection (Kraken limit is ~250, stay under)
KRAKEN_CHUNK_SIZE = int(os.getenv("KRAKEN_CHUNK_SIZE", "200"))

# ---------------------------------------------------------------------------
# Coinbase exchange settings
# ---------------------------------------------------------------------------
COINBASE_WS_URL = os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
COINBASE_REST_URL = os.getenv(
    "COINBASE_REST_URL", "https://api.exchange.coinbase.com/products",
)

# ---------------------------------------------------------------------------
# Binance.US exchange settings (US-compliant endpoint)
# ---------------------------------------------------------------------------
# Note: Regular Binance (binance.com) is not available in the US.
# Binance.US has a different product set and endpoints.
BINANCE_WS_URL = os.getenv("BINANCE_WS_URL", "wss://stream.binance.us:9443")
BINANCE_REST_URL = os.getenv(
    "BINANCE_REST_URL", "https://api.binance.us/api/v3/exchangeInfo",
)

# ---------------------------------------------------------------------------
# Bybit exchange settings
# ---------------------------------------------------------------------------
BYBIT_WS_URL = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/spot")
BYBIT_REST_URL = os.getenv(
    "BYBIT_REST_URL", "https://api.bybit.com/v5/market/instruments-info",
)

# ---------------------------------------------------------------------------
# OKX exchange settings
# ---------------------------------------------------------------------------
OKX_WS_URL = os.getenv("OKX_WS_URL", "wss://ws.okx.com:8443/ws/v5/public")
OKX_REST_URL = os.getenv(
    "OKX_REST_URL", "https://www.okx.com/api/v5/public/instruments",
)

# ---------------------------------------------------------------------------
# Pionex exchange settings
# ---------------------------------------------------------------------------
PIONEX_WS_URL = os.getenv("PIONEX_WS_URL", "wss://ws.pionex.com/wsPub")
PIONEX_REST_URL = os.getenv(
    "PIONEX_REST_URL", "https://api.pionex.com/api/v1/common/symbols",
)

# ---------------------------------------------------------------------------
# Quote currencies to track
# ---------------------------------------------------------------------------
# Only subscribe to pairs quoted in these currencies.
# Comma-separated in env, e.g. "USD,EUR"
QUOTE_CURRENCIES = [
    q.strip().upper()
    for q in os.getenv("QUOTE_CURRENCIES", "USD").split(",")
]

# ---------------------------------------------------------------------------
# Cache debug / diagnostics
# ---------------------------------------------------------------------------
# Set CACHE_LIVE_PRINT=true to print each coin's updated aggregate to the
# terminal in real time as prices arrive from the exchanges.
CACHE_LIVE_PRINT = os.getenv("CACHE_LIVE_PRINT", "false").strip().lower() in (
    "1", "true", "yes", "on"
)

# ---------------------------------------------------------------------------
# Arbitrage strategy settings
# ---------------------------------------------------------------------------
# How often the arbitrage scanner checks for opportunities (milliseconds).
SCAN_INTERVAL_MS = int(os.getenv("SCAN_INTERVAL_MS", "500"))

# Minimum NET spread (after both taker fees) required to emit a signal.
# Expressed as a percentage — e.g. 0.05 means 0.05%.
# Set higher to reduce noise; set lower to catch more (but weaker) signals.
MIN_NET_SPREAD_PCT = float(os.getenv("MIN_NET_SPREAD_PCT", "0.05"))

# Print each detected signal to the terminal (in addition to logging).
SIGNAL_PRINT_ENABLED = os.getenv("SIGNAL_PRINT_ENABLED", "true").strip().lower() in (
    "1", "true", "yes", "on"
)

# Reference trade size in USD used to estimate profit per signal.
# Does not affect signal detection — only the est_profit_usd field.
TRADE_SIZE_USD = float(os.getenv("TRADE_SIZE_USD", "1000"))

# ---------------------------------------------------------------------------
# Execution layer settings
# ---------------------------------------------------------------------------
# When DRY_RUN=true (default), the coordinator simulates fills instead of
# sending real orders.  Flip to false only when you're ready to trade real $.
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() in (
    "1", "true", "yes", "on"
)

# Maximum single-trade size (caps TRADE_SIZE_USD for safety).
# Set to 0 to disable the cap.
MAX_TRADE_SIZE_USD = float(os.getenv("MAX_TRADE_SIZE_USD", "0"))

# Enable the coordinator to automatically execute detected signals.
# When false the scanner still prints signals, but no orders are placed.
AUTO_EXECUTE = os.getenv("AUTO_EXECUTE", "false").strip().lower() in (
    "1", "true", "yes", "on"
)

# --- API credentials (kept empty by default — set in .env) ----------------
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

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ---------------------------------------------------------------------------
# Health check server (Cloud Run injects PORT automatically)
# ---------------------------------------------------------------------------
HEALTH_PORT = int(os.getenv("PORT", "8081"))
