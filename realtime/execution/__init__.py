"""
Execution layer — handles order placement and management across exchanges.

Structure:
    base.py         Abstract ExchangeClient interface
    types.py        Shared order types (Order, OrderResult, etc.)
    manager.py      Routes orders to the correct exchange client
    coordinator.py  Two-sided arbitrage execution flow
    kraken.py       Kraken trading client
    coinbase.py     Coinbase Advanced Trade client
    binance.py      Binance.US trading client
    bybit.py        Bybit trading client
    okx.py          OKX trading client
    pionex.py       Pionex trading client

To add a new exchange:
    1. Create execution/your_exchange.py
    2. Subclass ExchangeClient
    3. Implement create_order, get_order, cancel_order, get_balances
    4. Register it in main.py when building the ExecutionManager
"""

from execution.types import (               # noqa: F401
    OrderSide,
    OrderType,
    OrderStatus,
    OrderRequest,
    OrderResult,
    ArbitrageTradeResult,
)
from execution.base import ExchangeClient    # noqa: F401
from execution.manager import ExecutionManager  # noqa: F401
from execution.coordinator import ArbitrageCoordinator  # noqa: F401
from execution.kraken import KrakenClient    # noqa: F401
from execution.coinbase import CoinbaseClient  # noqa: F401
from execution.binance import BinanceClient  # noqa: F401
from execution.bybit import BybitClient      # noqa: F401
from execution.okx import OKXClient          # noqa: F401
from execution.pionex import PionexClient    # noqa: F401
