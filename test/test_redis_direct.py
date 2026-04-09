"""
Simple Redis connection test.
"""
import redis

REDIS_URL = "redis://default:AbM1AAIncDJmMDg0NGJiZTJhNTg0MmMzYjAyZmJjNjBlZDk1NWIwMXAyNDU4Nzc@natural-bluebird-45877.upstash.io:6379"

print("Testing Redis connection...")
print(f"URL: {REDIS_URL[:50]}...\n")

try:
    # Try with rediss:// (TLS) instead of redis://
    tls_url = REDIS_URL.replace("redis://", "rediss://")
    print(f"Trying TLS URL: {tls_url[:50]}...")
    
    client = redis.from_url(tls_url, decode_responses=True)
    print("Client created, attempting ping...")
    
    result = client.ping()
    print(f"✓ Ping successful: {result}")
    
    # Try to set and get a test value
    client.set("test_key", "test_value", ex=10)
    value = client.get("test_key")
    print(f"✓ Set/Get test: {value}")
    
    print("\n✅ Redis connection works!")
    
except Exception as e:
    print(f"✗ Error: {e}")
    print(f"Error type: {type(e).__name__}")
