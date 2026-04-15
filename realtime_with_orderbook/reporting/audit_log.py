"""
Audit log for live trade runs.

Creates a timestamped log file in realtime_with_orderbook/logs/ and
records balances, trade signals, execution results, and P&L.

Log files are named:  logs/live_run_YYYY-MM-DD_HHMMSS.log

Toggle with TRADE_LOG_ENABLED in .env.

Usage:
    audit = AuditLog(enabled=True)
    audit.log_balances(balances, "INITIAL")
    audit.log_signal(signal)
    audit.log_trade(signal, trade_result)
    audit.log_balances(balances, "FINAL")
    audit.log_summary(initial_balances, final_balances)
    audit.close()
"""

import logging
import os
from datetime import datetime
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")


class AuditLog:
    """Manages a timestamped audit log file for a single trade run."""

    def __init__(self, enabled: bool = True):
        self._enabled = enabled
        self._filepath: Optional[str] = None
        self._fh = None

        if self._enabled:
            self._open()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _open(self) -> None:
        os.makedirs(_LOGS_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        self._filepath = os.path.join(_LOGS_DIR, f"live_run_{ts}.log")
        self._fh = open(self._filepath, "w")
        self._write_line(f"{'=' * 70}")
        self._write_line(
            f"  LIVE TRADE RUN — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        self._write_line(f"{'=' * 70}")
        self._write_line("")

    @property
    def filepath(self) -> Optional[str]:
        return self._filepath

    def close(self) -> None:
        if self._fh:
            self._write_line("")
            self._write_line(f"{'=' * 70}")
            self._write_line(
                f"  RUN COMPLETE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            self._write_line(f"{'=' * 70}")
            self._fh.close()
            self._fh = None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _write_line(self, line: str) -> None:
        if self._fh:
            self._fh.write(line + "\n")
            self._fh.flush()

    # ------------------------------------------------------------------
    # Public logging methods
    # ------------------------------------------------------------------

    def log_message(self, msg: str) -> None:
        """Write a generic timestamped message."""
        if not self._enabled:
            return
        ts = datetime.now().strftime("%H:%M:%S")
        self._write_line(f"[{ts}] {msg}")

    def log_balances(
        self,
        balances: Dict[str, Dict[str, float]],
        label: str = "",
    ) -> None:
        """Write a balance snapshot from all exchanges."""
        if not self._enabled:
            return
        self._write_line("")
        header = f"  ── {label} BALANCES "
        self._write_line(header + "─" * max(0, 70 - len(header)))
        for exchange, assets in sorted(balances.items()):
            self._write_line(f"  {exchange}:")
            for asset, amount in sorted(assets.items()):
                self._write_line(f"    {asset:<8} {amount:>18.8f}")
        self._write_line("")

    def log_signal(self, signal) -> None:
        """Write the details of a detected arbitrage signal."""
        if not self._enabled:
            return
        self._write_line("")
        self._write_line("  ── ARBITRAGE SIGNAL DETECTED " + "─" * 39)
        self._write_line(f"  Coin:           {signal.coin_id}")
        self._write_line(f"  Trade size:     ${signal.trade_size_usd:,.2f}")
        self._write_line(
            f"  BUY on:         {signal.buy_exchange}  "
            f"eff.price = ${signal.buy_price:,.6f}"
        )
        self._write_line(
            f"    best ask:     ${signal.buy_best_ask:,.6f}  "
            f"depth = ${signal.buy_depth_usd:,.2f}"
        )
        self._write_line(f"    slippage:     {signal.slippage_buy_pct:+.4f}%")
        self._write_line(f"    book age:     {signal.buy_book_age_s:.1f}s")
        self._write_line(
            f"  SELL on:        {signal.sell_exchange}  "
            f"eff.price = ${signal.sell_price:,.6f}"
        )
        self._write_line(
            f"    best bid:     ${signal.sell_best_bid:,.6f}  "
            f"depth = ${signal.sell_depth_usd:,.2f}"
        )
        self._write_line(f"    slippage:     {signal.slippage_sell_pct:+.4f}%")
        self._write_line(f"    book age:     {signal.sell_book_age_s:.1f}s")
        self._write_line(f"  Gross spread:   {signal.gross_spread_pct:+.4f}%")
        self._write_line(
            f"  Net spread:     {signal.net_spread_pct:+.4f}%  (after fees)"
        )
        self._write_line(f"  Est. profit:    ${signal.est_profit_usd:,.6f}")
        self._write_line("")

    def log_trade(self, signal, trade_result) -> None:
        """Write the details of an executed arbitrage trade."""
        if not self._enabled:
            return
        self._write_line("")
        self._write_line("  ── TRADE EXECUTION RESULT " + "─" * 43)
        self._write_line(f"  Status:         {trade_result.status}")
        self._write_line(f"  Coin:           {trade_result.coin_id}")
        self._write_line("")

        buy = trade_result.buy_result
        self._write_line(f"  BUY leg ({buy.exchange}):")
        self._write_line(f"    Status:       {buy.status.value}")
        self._write_line(f"    Order ID:     {buy.order_id}")
        if buy.is_success:
            self._write_line(f"    Filled qty:   {buy.filled_qty:.8f}")
            self._write_line(f"    Avg price:    ${buy.filled_price:.6f}")
            self._write_line(f"    Cost (USDT):  ${buy.filled_usd:.6f}")
            self._write_line(f"    Fee (USDT≈):  ${buy.fee:.6f}")
        if buy.error:
            self._write_line(f"    Error:        {buy.error}")

        self._write_line("")
        sell = trade_result.sell_result
        self._write_line(f"  SELL leg ({sell.exchange}):")
        self._write_line(f"    Status:       {sell.status.value}")
        self._write_line(f"    Order ID:     {sell.order_id}")
        if sell.is_success:
            self._write_line(f"    Filled qty:   {sell.filled_qty:.8f}")
            self._write_line(f"    Avg price:    ${sell.filled_price:.6f}")
            self._write_line(f"    Revenue:      ${sell.filled_usd:.6f}")
            self._write_line(f"    Fee (USDT):   ${sell.fee:.6f}")
        if sell.error:
            self._write_line(f"    Error:        {sell.error}")

        self._write_line("")
        self._write_line(
            f"  Net P&L (order-based): ${trade_result.net_profit_usd:+.6f}"
        )
        self._write_line("")

    def log_summary(
        self,
        initial_balances: Dict[str, Dict[str, float]],
        final_balances: Dict[str, Dict[str, float]],
    ) -> None:
        """Compute and log actual P&L from balance changes (most reliable)."""
        if not self._enabled:
            return
        self._write_line("")
        self._write_line("  ── P&L SUMMARY (from actual balance changes) " + "─" * 23)

        total_usdt_before = 0.0
        total_usdt_after = 0.0
        total_ada_before = 0.0
        total_ada_after = 0.0

        all_exchanges = sorted(
            set(list(initial_balances.keys()) + list(final_balances.keys()))
        )
        for ex in all_exchanges:
            init = initial_balances.get(ex, {})
            final = final_balances.get(ex, {})

            usdt_before = init.get("usdt", 0.0)
            usdt_after = final.get("usdt", 0.0)
            ada_before = init.get("ada", 0.0)
            ada_after = final.get("ada", 0.0)

            total_usdt_before += usdt_before
            total_usdt_after += usdt_after
            total_ada_before += ada_before
            total_ada_after += ada_after

            self._write_line(f"  {ex}:")
            self._write_line(
                f"    USDT: {usdt_before:>14.6f} → {usdt_after:>14.6f}  "
                f"(Δ {usdt_after - usdt_before:+.6f})"
            )
            self._write_line(
                f"    ADA:  {ada_before:>14.6f} → {ada_after:>14.6f}  "
                f"(Δ {ada_after - ada_before:+.6f})"
            )

        self._write_line("")
        self._write_line("  TOTALS (across all exchanges):")
        self._write_line(
            f"    USDT: {total_usdt_before:>14.6f} → {total_usdt_after:>14.6f}  "
            f"(Δ {total_usdt_after - total_usdt_before:+.6f})"
        )
        self._write_line(
            f"    ADA:  {total_ada_before:>14.6f} → {total_ada_after:>14.6f}  "
            f"(Δ {total_ada_after - total_ada_before:+.6f})"
        )
        self._write_line("")
