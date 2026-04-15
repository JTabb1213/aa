"""
Realtime Market Data Ingestion Server
======================================

Connects to cryptocurrency exchange websockets, normalizes incoming
market data into a unified format, and caches it in-memory.  An
arbitrage scanner detects cross-exchange opportunities and, when
AUTO_EXECUTE is enabled, the coordinator places both legs automatically.

Architecture:
    Exchange Connectors → Queue → Normalizer → PriceCache
                                                   ↓
                                          ArbitrageScanner
                                                   ↓
                                       ArbitrageCoordinator → ExecutionManager
                                                                  ↓
                                                       KrakenClient / CoinbaseClient / BinanceClient

Usage:
    python main.py

Environment:
    See .env for required configuration.
"""

import asyncio
import logging
import sys

from aiohttp import web

import config
from core.pipeline import Pipeline
from exchanges.kraken import KrakenConnector
from exchanges.coinbase import CoinbaseConnector
from exchanges.binance import BinanceConnector
from exchanges.bybit import BybitConnector
from exchanges.okx import OKXConnector
from exchanges.pionex import PionexConnector
from exchanges.mexc import MexcConnector
from exchanges.gateio import GateioConnector
from normalizer.normalizer import Normalizer
from normalizer.aliases import AliasResolver
from storage.price_cache import PriceCache
from strategy.arbitrage import ArbitrageScanner
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
logger = logging.getLogger("realtime")

# Silence noisy libraries
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Health check server
# ---------------------------------------------------------------------------
# Cloud Run requires an HTTP endpoint to verify the container is alive.
# This tiny aiohttp server exposes /health with pipeline stats.
# ---------------------------------------------------------------------------
_pipeline: Pipeline = None   # set in main(), read by health handler


async def _health_handler(request):
    """Health check endpoint for Cloud Run / load balancers."""
    if _pipeline:
        return web.json_response({
            "status": "healthy",
            "stats": _pipeline.stats,
        })
    return web.json_response({"status": "starting"}, status=503)


async def _start_health_server():
    """Start a minimal HTTP server for health/readiness probes."""
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

    logger.info("=" * 60)
    logger.info("  Realtime Market Data Ingestion Server")
    logger.info("=" * 60)

    # 1. Alias resolver — loads data/coin_aliases.json into memory
    alias_resolver = AliasResolver()
    logger.info(
        f"Alias resolver: {alias_resolver.total_assets} assets, "
        f"{alias_resolver.total_aliases} aliases"
    )

    # 2. Normalizer — converts exchange-specific data to canonical format
    normalizer = Normalizer(alias_resolver)

    # 3. In-memory price cache (replaces Redis for arbitrage use)
    writer = PriceCache()

    # 4. Ingestion queue — decouples websocket receiving from processing
    #    maxsize prevents unbounded memory growth if processing falls behind
    ingestion_queue = asyncio.Queue(maxsize=10_000)

    # 5. Exchange connectors
    #    Comment out or delete any exchange you don't want to use.
    kraken = KrakenConnector(ingestion_queue)
    coinbase = CoinbaseConnector(ingestion_queue)
    binance = BinanceConnector(ingestion_queue)
    bybit = BybitConnector(ingestion_queue)
    okx = OKXConnector(ingestion_queue)
    pionex = PionexConnector(ingestion_queue)
    mexc = MexcConnector(ingestion_queue)
    gateio = GateioConnector(ingestion_queue)
    connectors = [kraken, coinbase, binance, bybit, okx, pionex, mexc, gateio]
   #connectors = [binance]

    # 6. Wire up the pipeline
    _pipeline = Pipeline(
        connectors=connectors,
        normalizer=normalizer,
        writer=writer,
        queue=ingestion_queue,
    )

    # 7. Arbitrage scanner — reads from the price cache
    scanner = ArbitrageScanner(writer)

    # 8. Execution layer — trading clients, manager, coordinator
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
        f"Execution: DRY_RUN={config.DRY_RUN}  "
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

    # 10. Start health check server and all tasks concurrently
    await _start_health_server()

    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down...", flush=True)
