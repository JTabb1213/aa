# Redis Caching Implementation

## Overview

The backend now uses Redis caching to reduce API calls and avoid rate limits. The architecture is designed to support eventual separation of the cache updater into a standalone service.

## Architecture

### Components

1. **cache_service.py** - Redis client wrapper
   - Handles get/set operations for tokenomics and coin data
   - Manages TTL (time-to-live) for cached entries
   - Provides cache statistics

2. **cache_updater_service.py** - Cache population service
   - Fetches fresh data from providers (CoinGecko, etc.)
   - Writes to Redis cache
   - Designed to run independently from main API

3. **data_service.py** - Modified to be cache-aware
   - Checks Redis cache first
   - Falls back to providers on cache miss
   - Auto-caches provider responses

## Configuration

### Redis URL

Set in `config.py` or via environment variable:

```python
REDIS_URL = "rediss://default:password@host:6379"
```

**Note:** Upstash Redis requires TLS, so use `rediss://` (not `redis://`).

### Cache TTL

Default TTL values in `config.py`:

```python
CACHE_TTL_TOKENOMICS = 600  # 10 minutes
CACHE_TTL_COIN_DATA = 300   # 5 minutes
```

## API Endpoints

### Update Cache

**POST /api/update-cache**

Update cache for specific coins or batch updates.

#### Single Coin Update

```bash
curl -X POST http://localhost:8000/api/update-cache \
  -H "Content-Type: application/json" \
  -d '{"coin_id": "bitcoin"}'
```

#### Multiple Coins Update

```bash
curl -X POST http://localhost:8000/api/update-cache \
  -H "Content-Type: application/json" \
  -d '{"coin_ids": ["bitcoin", "ethereum", "solana"]}'
```

#### Popular Coins Update

```bash
curl -X POST http://localhost:8000/api/update-cache \
  -H "Content-Type: application/json" \
  -d '{"popular": true, "limit": 20}'
```

### Cache Statistics

**GET /api/cache-stats**

Get current cache statistics:

```bash
curl http://localhost:8000/api/cache-stats
```

Response:
```json
{
  "connected": true,
  "total_keys": 42,
  "used_memory_human": "2.5M",
  "connected_clients": 3
}
```

## Usage Flow

### Current (Monolithic)

```
User Request → Flask API → data_service
                              ↓
                          Check Cache?
                          ↙         ↘
                    Cache Hit    Cache Miss
                        ↓             ↓
                   Return Data   Fetch from Provider
                                      ↓
                                 Cache Result
                                      ↓
                                 Return Data
```

### Future (Separated)

**Read API (User-facing):**
```
User Request → Flask API → data_service → Redis Cache
```

**Write Service (Background):**
```
Scheduler → cache_updater_service → Provider API → Redis Cache
```

## Separating the Cache Updater

When ready to separate the cache updater into its own service:

### Option 1: Scheduled Background Job

Create a separate Python script that runs on a schedule (cron, systemd timer, etc.):

```python
# cache_updater_job.py
from services.cache_updater_service import cache_updater

# Update popular coins every 5 minutes
cache_updater.update_popular_coins(limit=50)
```

Run via cron:
```bash
*/5 * * * * cd /path/to/backend && venv/bin/python cache_updater_job.py
```

### Option 2: Separate Microservice

Create a new Flask app that only handles cache updates:

```python
# cache_updater_app.py
from flask import Flask, request, jsonify
from services.cache_updater_service import cache_updater

app = Flask(__name__)

@app.route('/update', methods=['POST'])
def update():
    data = request.json
    result = cache_updater.update_coin(data['coin_id'])
    return jsonify(result)

if __name__ == '__main__':
    app.run(port=8001)  # Different port from main API
```

### Option 3: Message Queue (Advanced)

Use Celery or similar for asynchronous updates:

```python
# tasks.py
from celery import Celery
from services.cache_updater_service import cache_updater

app = Celery('tasks', broker='redis://localhost:6379')

@app.task
def update_coin_cache(coin_id):
    return cache_updater.update_coin(coin_id)
```

## Testing

Run the test suite:

```bash
cd backend
source venv/bin/activate
python test_cache.py
```

Expected output:
```
Testing Redis cache implementation...

1. Testing cache service connection...
   ✓ Redis connected: True
   ✓ Total keys: 0

2. Testing cache updater...
   ✓ Bitcoin tokenomics cached
   ✓ Bitcoin coin_data cached

3. Testing data_service cache reads...
   ✓ Tokenomics retrieved: Bitcoin
   ✓ Cache now has 2 keys

✅ All cache tests passed!
```

## Monitoring

### Check Cache Health

```python
from services.cache_service import cache_service

stats = cache_service.get_stats()
print(stats)
```

### View Cached Keys (via Redis CLI)

```bash
redis-cli -u rediss://default:password@host:6379
> KEYS crypto:*
> TTL crypto:tokenomics:bitcoin
```

## Best Practices

1. **Pre-warm cache** for popular coins on startup or via scheduled job
2. **Monitor cache hit rate** to optimize TTL values
3. **Set appropriate TTL** based on data volatility:
   - Price data: 1-2 minutes
   - Tokenomics: 5-10 minutes
   - Static info: 1+ hours
4. **Handle cache failures gracefully** - always fall back to provider
5. **Use separate Redis instances** for production vs development

## Environment Variables

For production, use environment variables instead of hardcoded values:

```bash
export REDIS_URL="rediss://default:password@host:6379"
export CACHE_TTL_TOKENOMICS=600
export CACHE_TTL_COIN_DATA=300
```

## Troubleshooting

### Connection Errors

- Verify Redis URL uses `rediss://` for TLS
- Check firewall rules allow outbound connections to Redis port
- Verify authentication credentials

### Cache Misses

- Check TTL settings aren't too short
- Verify coin_id format matches what's stored in cache
- Use `/api/cache-stats` to monitor key count

### Rate Limits Still Occurring

- Increase cache TTL
- Pre-warm cache with popular coins
- Add retry logic with exponential backoff in provider code
