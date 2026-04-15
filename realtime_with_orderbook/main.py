"""
Realtime Orderbook Arbitrage Engine
=====================================

Connects to 8 cryptocurrency exchange orderbook websockets, normalizes
incoming data into a unified format, and scans for cross-exchange
arbitrage opportunities using depth-aware pricing.

Architecture:
    Exchange Connectors → Queue → Normalizer → OrderBookCache
                                                     ↓
                                              Live Terminal Table
                                                     ↓
                                        OrderBookArbitrageScanner
                                                     ↓
                                          ArbitrageCoordinator → ExecutionManager
                                                                      ↓
                                                          Exchange Execution Clients

Usage:
    python main.py

    Toggle TEST_MODE in .env to switch between dry-run and live trading.

Environment:
    See .env for required configuration.
"""

import asyncio
import logging
import sys

from aiohttp import web

import config
from core.pipeline import Pipeline
from exchanges.kraken import KrakenOrderBookConnector
from exchanges.coinbase import CoinbaseOrderBookConnector
from exchanges.binance import BinanceOrderBookConnector
from exchanges.bybit import BybitOrderBookConnector
from exchanges.okx import OKXOrderBookConnector
from exchanges.pionex import PionexOrderBookConnector
from exchanges.mexc import MexcOrderBookConnector
from exchanges.gateio import GateioOrderBookConnector
from normalizer.normalizer import Normalizer
from normalizer.aliases import AliasResolver
from storage.orderbook_cache import OrderBookCache
from strategy.arbitrage import OrderBookArbitrageScanner
from execution.manager import ExecutionManager
from execution.coordinator import ArbitrageCoordinator
from execution.kraken import KrakenClient
from execution.coinbase import CoinbaseClient
from execution.binance import BinanceClient
from execution.bybit import BybitClient
from execution.okx import OKXClient
from execution.pionex import PionexClient
from execution.mexc import MexcClient
from execution.gateio import GateioClient


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s │ %(levelname)-7s │ %(name)-20s │ %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("orderbook")

logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Health check server
# ---------------------------------------------------------------------------
_pipeline: Pipeline = None


async def _health_handler(request):
    if _pipeline:
        return web.json_response({
            "status": "healthy",
            "mode": "TEST" if config.TEST_MODE else "LIVE",
            "stats": _pipeline.stats,
        })
    return web.json_response({"status": "starting"}, status=503)


async def _start_health_server():
    app = web.Application()
    app.router.add_get("/health", _health_handler)
    app.router.add_get("/", _health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", config.HEALTH_PORT)
    await site.start()
    logger.info(f"Health check server listening on :{config.HEALTH_PORT}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    global _pipeline

    mode_str = "\033[33m TEST MODE (dry-run) \033[0m" if config.TEST_MODE else "\033[31m LIVE MODE \033[0m"

    logger.info("=" * 60)
    logger.info("  Orderbook Arbitrage Engine")
    logger.info(f"  Mode: {'TEST (dry-run)' if config.TEST_MODE else 'LIVE'}")
    logger.info("=" * 60)
    print(f"\n  ▶ Running in {mode_str}\n", flush=True)

    # 1. Alias resolver
    alias_resolver = AliasResolver()
    logger.info(
        f"Alias resolver: {alias_resolver.total_assets} assets, "
        f"{alias_resolver.total_aliases} aliases"
    )

    # 2. Normalizer
    normalizer = Normalizer(alias_resolver)

    # 3. Orderbook cache
    cache = OrderBookCache()

    # 4. Ingestion queue
    ingestion_queue = asyncio.Queue(maxsize=10_000)

    # 5. Exchange orderbook connectors
    kraken = KrakenOrderBookConnector(ingestion_queue)
    coinbase = CoinbaseOrderBookConnector(ingestion_queue)
    binance = BinanceOrderBookConnector(ingestion_queue)
    bybit = BybitOrderBookConnector(ingestion_queue)
    okx = OKXOrderBookConnector(ingestion_queue)
    pionex = PionexOrderBookConnector(ingestion_queue)
    mexc = MexcOrderBookConnector(ingestion_queue)
    gateio = GateioOrderBookConnector(ingestion_queue)

    connectors = [kraken, coinbase, binance, bybit, okx, pionex, gateio]
    # connectors = [kraken, binance]   # ← uncomment to test with fewer exchanges

    # 6. Pipeline
    _pipeline = Pipeline(
        connectors=connectors,
        normalizer=normalizer,
        cache=cache,
        queue=ingestion_queue,
    )

    # 7. Arbitrage scanner
    scanner = OrderBookArbitrageScanner(cache)

    # 8. Execution layer
    exec_manager = ExecutionManager()

    kraken_client = KrakenClient(
        api_key=config.KRAKEN_API_KEY,
        api_secret=config.KRAKEN_API_SECRET,
        dry_run=config.DRY_RUN,
    )
    coinbase_client = CoinbaseClient(
        api_key=config.COINBASE_API_KEY,
        api_secret=config.COINBASE_API_SECRET,
        dry_run=config.DRY_RUN,
    )
    binance_client = BinanceClient(
        api_key=config.BINANCE_API_KEY,
        api_secret=config.BINANCE_API_SECRET,
        dry_run=config.DRY_RUN,
    )
    bybit_client = BybitClient(
        api_key=config.BYBIT_API_KEY,
        api_secret=config.BYBIT_API_SECRET,
        dry_run=config.DRY_RUN,
    )
    okx_client = OKXClient(
        api_key=config.OKX_API_KEY,
        api_secret=config.OKX_API_SECRET,
        passphrase=config.OKX_PASSPHRASE,
        dry_run=config.DRY_RUN,
    )
    pionex_client = PionexClient(
        api_key=config.PIONEX_API_KEY,
        api_secret=config.PIONEX_API_SECRET,
        dry_run=config.DRY_RUN,
    )
    mexc_client = MexcClient(
        api_key=config.MEXC_API_KEY,
        api_secret=config.MEXC_API_SECRET,
        dry_run=config.DRY_RUN,
    )
    gateio_client = GateioClient(
        api_key=config.GATEIO_API_KEY,
        api_secret=config.GATEIO_API_SECRET,
        dry_run=config.DRY_RUN,
    )

    exec_manager.register(kraken_client)
    exec_manager.register(coinbase_client)
    exec_manager.register(binance_client)
    exec_manager.register(bybit_client)
    exec_manager.register(okx_client)
    exec_manager.register(pionex_client)
    exec_manager.register(mexc_client)
    exec_manager.register(gateio_client)

    coordinator = ArbitrageCoordinator(exec_manager)

    logger.info(
        f"Execution: TEST_MODE={config.TEST_MODE}  "
        f"AUTO_EXECUTE={config.AUTO_EXECUTE}  "
        f"TRADE_SIZE=${config.TRADE_SIZE_USD}"
    )

    # 9. Build task list
    tasks = [
        _pipeline.run(),
        scanner.scan_loop(),
    ]
    if config.AUTO_EXECUTE:
        tasks.append(coordinator.run(scanner))
        logger.info("Auto-execution ENABLED — coordinator will trade signals")
    else:
        logger.info("Auto-execution DISABLED — signals are logged/printed only")

    # 10. Start health check and all tasks
    await _start_health_server()
    await asyncio.gather(*tasks, return_exceptions=True)


# ---------------------------------------------------------------------------
# One-shot live test mode
# ---------------------------------------------------------------------------

async def main_one_shot():
    """
    One-shot live test mode.

    Executes exactly ONE arbitrage trade, logs everything to a timestamped
    file, then exits.  Designed for live testing with real money.

    Flow:
        1. Connect to enabled exchanges (websocket for orderbook data)
        2. Fetch + log initial balances from exchange REST APIs
        3. Start orderbook pipeline (no terminal table)
        4. Wait for first arbitrage signal that meets threshold
        5. Execute both legs simultaneously (buy on one, sell on other)
        6. Wait for fills → log trade results + final balances
        7. Exit

    Safety:
        - Only ONE trade is ever executed per run
        - Program exits immediately after the trade settles
        - All details are written to logs/live_run_<timestamp>.log
        - The scanner loop is NOT started — we scan manually
    """
    from reporting.audit_log import AuditLog
    from reporting.balance import fetch_all_balances

    # Suppress terminal clutter from pipeline/scanner
    config.LIVE_TABLE = False
    config.SIGNAL_PRINT_ENABLED = False

    # Determine which exchanges to use
    enabled = config.ENABLED_EXCHANGES or [
        "kraken", "coinbase", "binance", "bybit",
        "okx", "pionex", "mexc", "gateio",
    ]

    # ── startup banner ──────────────────────────────────────────────
    print(flush=True)
    print(f"  {'=' * 58}", flush=True)
    print(f"  ▶ ONE-SHOT LIVE TEST MODE", flush=True)
    print(f"  ▶ Trade size: ${config.TRADE_SIZE_USD:.0f} USD", flush=True)
    print(f"  ▶ Exchanges:  {', '.join(enabled)}", flush=True)
    print(f"  ▶ Min net spread: {config.MIN_NET_SPREAD_PCT}%", flush=True)
    print(f"  {'=' * 58}", flush=True)
    print(flush=True)

    # ── 1. audit log ────────────────────────────────────────────────
    audit = AuditLog(enabled=config.TRADE_LOG_ENABLED)
    if audit.filepath:
        audit.log_message("ONE-SHOT LIVE TEST MODE")
        audit.log_message(f"Trade size: ${config.TRADE_SIZE_USD:.0f} USD")
        audit.log_message(f"Exchanges: {', '.join(enabled)}")
        audit.log_message(f"Min net spread: {config.MIN_NET_SPREAD_PCT}%")

    # ── 2. alias resolver / normalizer / cache ──────────────────────
    alias_resolver = AliasResolver()
    normalizer_inst = Normalizer(alias_resolver)
    cache = OrderBookCache()
    ingestion_queue = asyncio.Queue(maxsize=10_000)

    # ── 3. build connector list (only enabled exchanges) ────────────
    connector_map = {
        "kraken":   lambda: KrakenOrderBookConnector(ingestion_queue),
        "coinbase": lambda: CoinbaseOrderBookConnector(ingestion_queue),
        "binance":  lambda: BinanceOrderBookConnector(ingestion_queue),
        "bybit":    lambda: BybitOrderBookConnector(ingestion_queue),
        "okx":      lambda: OKXOrderBookConnector(ingestion_queue),
        "pionex":   lambda: PionexOrderBookConnector(ingestion_queue),
        "mexc":     lambda: MexcOrderBookConnector(ingestion_queue),
        "gateio":   lambda: GateioOrderBookConnector(ingestion_queue),
    }
    connectors = [
        connector_map[name]()
        for name in enabled
        if name in connector_map
    ]
    if not connectors:
        print("  ✗ No valid exchanges enabled. Check ENABLED_EXCHANGES.", flush=True)
        audit.log_message("ERROR: No valid exchanges enabled.")
        audit.close()
        return

    # ── 4. pipeline (no live table, no signal printing) ─────────────
    pipeline = Pipeline(
        connectors=connectors,
        normalizer=normalizer_inst,
        cache=cache,
        queue=ingestion_queue,
    )

    # ── 5. execution clients (LIVE — dry_run=False) ─────────────────
    exec_manager = ExecutionManager()

    if "kraken" in enabled:
        kraken_client = KrakenClient(
            api_key=config.KRAKEN_API_KEY,
            api_secret=config.KRAKEN_API_SECRET,
            dry_run=False,
        )
        exec_manager.register(kraken_client)

    if "okx" in enabled:
        okx_client = OKXClient(
            api_key=config.OKX_API_KEY,
            api_secret=config.OKX_API_SECRET,
            passphrase=config.OKX_PASSPHRASE,
            dry_run=False,
        )
        exec_manager.register(okx_client)

    # ── 6. fetch + log initial balances ─────────────────────────────
    print("  ▶ Fetching initial balances...", flush=True)
    try:
        initial_balances = await fetch_all_balances(exec_manager)
        for ex, assets in sorted(initial_balances.items()):
            for asset, amount in sorted(assets.items()):
                print(f"    {ex}: {asset:<8} {amount:>14.8f}", flush=True)
        audit.log_balances(initial_balances, "INITIAL")
    except Exception as e:
        print(f"  ✗ Failed to fetch initial balances: {e}", flush=True)
        audit.log_message(f"ERROR: Failed to fetch initial balances: {e}")
        audit.close()
        return

    # ── 7. start pipeline in background ─────────────────────────────
    print("  ▶ Connecting to exchange websockets...", flush=True)
    pipeline_task = asyncio.create_task(pipeline.run())

    # Give websockets time to connect and receive initial orderbooks
    await asyncio.sleep(6)

    # Verify we have data from the expected exchanges
    all_coins = cache.get_all_coins()
    connected_exchanges: set = set()
    for coin in all_coins:
        books = cache.get_books(coin)
        if books:
            connected_exchanges.update(books.keys())

    print(
        f"  ▶ Connected: {', '.join(sorted(connected_exchanges)) or '(none)'}",
        flush=True,
    )
    print(
        f"  ▶ Coins with data: "
        f"{', '.join(sorted(all_coins)) if all_coins else '(none)'}",
        flush=True,
    )

    # Extra wait if not all exchanges are connected yet
    if len(connected_exchanges) < len(enabled):
        print("  ▶ Waiting for remaining exchanges...", flush=True)
        await asyncio.sleep(6)
        for coin in cache.get_all_coins():
            books = cache.get_books(coin)
            if books:
                connected_exchanges.update(books.keys())
        print(
            f"  ▶ Connected: "
            f"{', '.join(sorted(connected_exchanges)) or '(none)'}",
            flush=True,
        )

    if len(connected_exchanges) < 2:
        print(
            "  ✗ Need at least 2 exchanges connected. Aborting.", flush=True
        )
        audit.log_message("ERROR: Fewer than 2 exchanges connected.")
        pipeline_task.cancel()
        try:
            await pipeline_task
        except asyncio.CancelledError:
            pass
        audit.close()
        return

    # ── 8. scan for the first arbitrage signal ──────────────────────
    print("  ▶ Scanning for arbitrage opportunities...", flush=True)
    audit.log_message("Scanning for arbitrage opportunities...")

    scanner = OrderBookArbitrageScanner(cache)
    scan_interval = config.SCAN_INTERVAL_MS / 1000
    signal = None
    MAX_WAIT_SECONDS = 3600  # safety timeout: 1 hour max

    import time as _time
    scan_start = _time.time()

    try:
        while signal is None:
            signals = scanner.scan()
            if signals:
                signal = signals[0]  # take the best
                break
            if _time.time() - scan_start > MAX_WAIT_SECONDS:
                print(
                    "  ✗ No signal found within timeout. Exiting.",
                    flush=True,
                )
                audit.log_message(
                    f"No signal found within {MAX_WAIT_SECONDS}s timeout."
                )
                pipeline_task.cancel()
                try:
                    await pipeline_task
                except asyncio.CancelledError:
                    pass
                audit.close()
                return
            await asyncio.sleep(scan_interval)
    except asyncio.CancelledError:
        print("\n  ▶ Cancelled while waiting for signal.", flush=True)
        audit.log_message("Cancelled while waiting for signal.")
        pipeline_task.cancel()
        try:
            await pipeline_task
        except asyncio.CancelledError:
            pass
        audit.close()
        return

    # ── 9. signal found — log it ────────────────────────────────────
    print(flush=True)
    print(
        "  ┌─ SIGNAL FOUND ─────────────────────────────────────────┐",
        flush=True,
    )
    print(f"  │  Coin:          {signal.coin_id}", flush=True)
    print(
        f"  │  BUY on:        {signal.buy_exchange} "
        f"@ ${signal.buy_price:.6f}",
        flush=True,
    )
    print(
        f"  │  SELL on:       {signal.sell_exchange} "
        f"@ ${signal.sell_price:.6f}",
        flush=True,
    )
    print(
        f"  │  Gross spread:  {signal.gross_spread_pct:+.4f}%", flush=True
    )
    print(
        f"  │  Net spread:    {signal.net_spread_pct:+.4f}%", flush=True
    )
    print(
        f"  │  Est. profit:   ${signal.est_profit_usd:+.4f}", flush=True
    )
    print(
        "  └─────────────────────────────────────────────────────────┘",
        flush=True,
    )
    print(flush=True)

    audit.log_signal(signal)

    # ── 10. execute the trade ───────────────────────────────────────
    print("  ▶ Executing trade (BUY + SELL simultaneously)...", flush=True)
    audit.log_message("Executing trade...")

    coordinator = ArbitrageCoordinator(exec_manager)
    trade_result = await coordinator.execute_signal(signal)

    if not trade_result:
        print("  ✗ Trade was not executed (pre-check failed).", flush=True)
        audit.log_message("Trade was not executed (coordinator pre-check failed).")
        pipeline_task.cancel()
        try:
            await pipeline_task
        except asyncio.CancelledError:
            pass
        audit.close()
        return

    # ── 11. log trade results ───────────────────────────────────────
    audit.log_trade(signal, trade_result)

    buy = trade_result.buy_result
    sell = trade_result.sell_result

    print(
        "  ┌─ TRADE RESULT ──────────────────────────────────────────┐",
        flush=True,
    )
    print(f"  │  Status:        {trade_result.status}", flush=True)
    print(
        f"  │  BUY  ({buy.exchange}):  {buy.status.value}", flush=True
    )
    if buy.is_success:
        print(f"  │    qty:         {buy.filled_qty:.8f}", flush=True)
        print(f"  │    avg price:   ${buy.filled_price:.6f}", flush=True)
        print(f"  │    cost:        ${buy.filled_usd:.6f}", flush=True)
        print(f"  │    fee:         ${buy.fee:.6f}", flush=True)
    if buy.error:
        print(f"  │    error:       {buy.error}", flush=True)
    print(
        f"  │  SELL ({sell.exchange}):  {sell.status.value}", flush=True
    )
    if sell.is_success:
        print(f"  │    qty:         {sell.filled_qty:.8f}", flush=True)
        print(f"  │    avg price:   ${sell.filled_price:.6f}", flush=True)
        print(f"  │    revenue:     ${sell.filled_usd:.6f}", flush=True)
        print(f"  │    fee:         ${sell.fee:.6f}", flush=True)
    if sell.error:
        print(f"  │    error:       {sell.error}", flush=True)
    print(
        f"  │  Net P&L:       ${trade_result.net_profit_usd:+.6f}",
        flush=True,
    )
    print(
        "  └──────────────────────────────────────────────────────────┘",
        flush=True,
    )
    print(flush=True)

    # ── 12. wait for settlement, then fetch final balances ──────────
    print("  ▶ Waiting for settlement...", flush=True)
    await asyncio.sleep(3)

    print("  ▶ Fetching final balances...", flush=True)
    try:
        final_balances = await fetch_all_balances(exec_manager)
        for ex, assets in sorted(final_balances.items()):
            for asset, amount in sorted(assets.items()):
                print(f"    {ex}: {asset:<8} {amount:>14.8f}", flush=True)
        audit.log_balances(final_balances, "FINAL")
        audit.log_summary(initial_balances, final_balances)
    except Exception as e:
        print(f"  ✗ Failed to fetch final balances: {e}", flush=True)
        audit.log_message(f"ERROR: Failed to fetch final balances: {e}")

    # ── 13. shut down ───────────────────────────────────────────────
    print("\n  ▶ Shutting down pipeline...", flush=True)
    pipeline_task.cancel()
    try:
        await pipeline_task
    except asyncio.CancelledError:
        pass

    audit.close()
    if audit.filepath:
        print(f"  ▶ Log file: {audit.filepath}", flush=True)

    print("  ▶ One-shot test complete. Exiting.\n", flush=True)


if __name__ == "__main__":
    try:
        if config.ONE_SHOT_MODE:
            asyncio.run(main_one_shot())
        else:
            asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down...", flush=True)
