# Realtime Market Data Ingestion Server

Connects to cryptocurrency exchange websockets, normalizes incoming market data, and writes it to Redis for the API layer to serve.

## Architecture

```
┌───────────────┐     ┌────────────────┐     ┌────────────┐     ┌──────────────┐
│   Kraken WS    │────▶│ Ingestion Queue │────▶│ Normalizer │────▶│ Redis Writer  │
│  (connector)   │     │  (asyncio.Queue)│     │  (aliases)  │    │  (batched)   │
└───────────────┘     └────────────────┘     └────────────┘     └──────────────┘
  exchanges/               core/               normalizer/          storage/
```

**Layer 1 — Exchange Connectors** (`exchanges/`)
- Connect to exchange websockets
- Parse exchange-specific message formats
- Emit standardized `RawTick` events onto the ingestion queue
- Handle reconnection with exponential backoff

**Layer 2 — Normalizer** (`normalizer/`)
- Reads from the ingestion queue
- Resolves exchange symbols (e.g. Kraken's `XBT`) to canonical coin IDs (e.g. `bitcoin`) using `data/coin_aliases.json`
- Computes derived fields (mid price, spread %)
- Outputs `NormalizedTick` events

**Layer 3 — Storage** (`storage/`)
- Accumulates normalized ticks in a batch buffer
- Flushes to Redis using pipelining (100× fewer network round-trips)
- Writes two keys per tick:
  - `rt:price:<coin_id>` — latest price from any exchange
  - `rt:ticker:<exchange>:<coin_id>` — per-exchange data

## Project Structure

```
realtime/
├── main.py              # Entry point — wires everything together
├── config.py            # Configuration from env vars
├── .env.example         # Template for environment variables
├── requirements.txt     # Python dependencies
├── Dockerfile           # Container for deployment
├── exchanges/
│   ├── base.py          # Abstract base class for connectors
│   └── kraken.py        # Kraken websocket connector
├── normalizer/
│   ├── aliases.py       # Alias resolver (reads coin_aliases.json)
│   └── normalizer.py    # RawTick → NormalizedTick conversion
├── storage/
│   └── redis_writer.py  # Batched Redis pipeline writer
└── core/
    ├── models.py        # RawTick & NormalizedTick dataclasses
    └── pipeline.py      # Pipeline orchestrator
```

## Quick Start

```bash
# 1. Install dependencies
cd realtime
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env and add your REDIS_URL

# 3. Run
python main.py
```

## Docker

```bash
# Build from project root (not realtime/ directory)
docker build -f realtime/Dockerfile -t realtime-ingest .

# Run
docker run --env-file realtime/.env realtime-ingest
```

## Redis Key Schema

| Key | Description | TTL |
|-----|-------------|-----|
| `rt:price:bitcoin` | Latest price data for bitcoin (from any exchange) | 30s |
| `rt:ticker:kraken:bitcoin` | Kraken-specific ticker data for bitcoin | 30s |

The `rt:` prefix separates realtime data from the existing `crypto:tokenomics:` cache.

## Adding a New Exchange

1. Create `exchanges/your_exchange.py`
2. Subclass `BaseExchange`
3. Implement `_connect_and_stream()` — parse WS messages and call `self._emit(RawTick(...))`
4. Add the connector to `main.py`:
   ```python
   your_exchange = YourExchangeConnector(ingestion_queue)
   connectors = [kraken, your_exchange]
   ```

The normalizer, batching, and Redis writing are all handled automatically.

## Configuration

All settings are in `.env` (see `.env.example` for documentation):

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | (required) | Redis connection URL |
| `RT_PRICE_TTL` | `30` | TTL for realtime keys in Redis (seconds) |
| `BATCH_MAX_SIZE` | `100` | Flush after this many ticks |
| `BATCH_INTERVAL_MS` | `500` | Flush after this many ms |
| `QUOTE_CURRENCIES` | `USD` | Which quote currencies to track |
| `LOG_LEVEL` | `INFO` | Logging verbosity |
| `PORT` | `8080` | Health check HTTP port |
