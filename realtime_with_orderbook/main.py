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


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down...", flush=True)
