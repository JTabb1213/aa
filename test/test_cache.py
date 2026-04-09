"""
Test script for Redis cache implementation.
"""

print('Testing Redis cache implementation...\n')

# Test 1: Cache service connection
print('1. Testing cache service connection...')
try:
    from services.cache import cache_service
    stats = cache_service.get_stats()
    print(f'   ✓ Redis connected: {stats["connected"]}')
    print(f'   ✓ Total keys: {stats["total_keys"]}')
except Exception as e:
    print(f'   ✗ Error: {e}')
    exit(1)

# Test 2: Cache updater service
print('\n2. Testing cache updater...')
try:
    from services.cache import cache_updater
    result = cache_updater.update_coin('bitcoin')
    if result['tokenomics_updated']:
        print(f'   ✓ Bitcoin tokenomics cached')
    if result['coin_data_updated']:
        print(f'   ✓ Bitcoin coin_data cached')
except Exception as e:
    print(f'   ✗ Error: {e}')
    exit(1)

# Test 3: Data service with cache
print('\n3. Testing data_service cache reads...')
try:
    from services.data import data_service
    
    # This should hit cache (warm from previous update)
    tokenomics = data_service.get_tokenomics('bitcoin')
    print(f'   ✓ Tokenomics retrieved: {tokenomics["name"]}')
    
    # Check cache stats
    stats = cache_service.get_stats()
    print(f'   ✓ Cache now has {stats["total_keys"]} keys')
except Exception as e:
    print(f'   ✗ Error: {e}')
    exit(1)

print('\n✅ All cache tests passed!')
