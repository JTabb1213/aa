"""
Arbitrage coordinator — executes two-leg trades.

Takes ArbitrageSignal from the scanner, validates pre-conditions,
places buy + sell simultaneously, and reports results.

Safety features:
  • TEST_MODE — simulates fills, no real orders
  • Per-coin cooldown to prevent re-entry
  • Max concurrent trades limit
  • Trade size cap
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import config
from execution.manager import ExecutionManager
from execution.types import (
    ArbitrageTradeResult,
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderType,
)
from strategy.arbitrage import ArbitrageSignal

logger = logging.getLogger(__name__)

COOLDOWN_SECONDS = 30
MAX_CONCURRENT_TRADES = 3
MIN_TRADE_USD = 10.0


@dataclass
class TradeRecord:
    signal: ArbitrageSignal
    result: ArbitrageTradeResult
    timestamp: float = field(default_factory=time.time)


class ArbitrageCoordinator:
    """Orchestrates two-leg arbitrage execution."""

    def __init__(self, manager: ExecutionManager):
        self._manager = manager
        self._cooldowns: Dict[str, float] = {}
        self._active_trades: int = 0
        self._history: List[TradeRecord] = []
        self._total_profit: float = 0.0

    async def execute_signal(self, signal: ArbitrageSignal) -> Optional[ArbitrageTradeResult]:
        """Attempt to execute an arbitrage signal."""
        if not self._pre_checks(signal):
            return None

        trade_usd = min(config.TRADE_SIZE_USD, config.MAX_TRADE_SIZE_USD)
        if trade_usd < MIN_TRADE_USD:
            logger.warning(f"[coordinator] Trade size ${trade_usd} below min ${MIN_TRADE_USD}")
            return None

        buy_request = OrderRequest(
            exchange=signal.buy_exchange,
            coin_id=signal.coin_id,
            side=OrderSide.BUY,
            quote="usdt",
            size_usd=trade_usd,
            order_type=OrderType.MARKET,
            limit_price=signal.buy_price,
        )
        sell_request = OrderRequest(
            exchange=signal.sell_exchange,
            coin_id=signal.coin_id,
            side=OrderSide.SELL,
            quote="usdt",
            size_usd=trade_usd,
            order_type=OrderType.MARKET,
            limit_price=signal.sell_price,
        )

        self._active_trades += 1
        mode = "TEST" if config.TEST_MODE else "LIVE"
        logger.info(
            f"[coordinator] [{mode}] EXECUTING {signal.coin_id}: "
            f"BUY {signal.buy_exchange} @ ${signal.buy_price:.4f} | "
            f"SELL {signal.sell_exchange} @ ${signal.sell_price:.4f} | "
            f"net_spread={signal.net_spread_pct:.4f}%"
        )

        try:
            buy_result, sell_result = await asyncio.gather(
                self._manager.place_order(buy_request),
                self._manager.place_order(sell_request),
            )
        except Exception as e:
            self._active_trades -= 1
            logger.error(f"[coordinator] Execution error: {e}")
            return None

        self._active_trades -= 1

        trade_result = ArbitrageTradeResult(
            coin_id=signal.coin_id,
            buy_result=buy_result,
            sell_result=sell_result,
        )
        trade_result.compute_profit()

        self._cooldowns[signal.coin_id] = time.time()
        self._history.append(TradeRecord(signal=signal, result=trade_result))
        if trade_result.status == "success":
            self._total_profit += trade_result.net_profit_usd

        self._report(trade_result)
        return trade_result

    async def run(self, scanner) -> None:
        """Continuously read signals from scanner and execute."""
        interval = config.SCAN_INTERVAL_MS / 1000
        logger.info("[coordinator] Arbitrage coordinator started")

        while True:
            try:
                signals = scanner.scan()
                for signal in signals:
                    await self.execute_signal(signal)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"[coordinator] Loop error: {e}")
            await asyncio.sleep(interval)

    def _pre_checks(self, signal: ArbitrageSignal) -> bool:
        if self._active_trades >= MAX_CONCURRENT_TRADES:
            return False

        last = self._cooldowns.get(signal.coin_id, 0)
        if time.time() - last < COOLDOWN_SECONDS:
            return False

        return True

    def _report(self, result: ArbitrageTradeResult) -> None:
        mode = "TEST" if config.TEST_MODE else "LIVE"
        status = result.status.upper()

        msg = (
            f"\n  ┌─ [{mode}] TRADE RESULT ───────────────────────────────────┐\n"
            f"  │  Coin     : {result.coin_id}\n"
            f"  │  Status   : {status}\n"
            f"  │  Buy      : {result.buy_result}\n"
            f"  │  Sell     : {result.sell_result}\n"
            f"  │  Net P&L  : ${result.net_profit_usd:,.4f}\n"
            f"  │  Total P&L: ${self._total_profit:,.4f} ({len(self._history)} trades)\n"
            f"  └────────────────────────────────────────────────────────────┘"
        )
        logger.info(msg)
        if config.SIGNAL_PRINT_ENABLED:
            print(msg, flush=True)
