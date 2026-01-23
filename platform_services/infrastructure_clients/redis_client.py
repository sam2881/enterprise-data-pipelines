"""Redis client utility for short-term memory and caching"""
import json
import os
import base64
from typing import Any, List, Optional
import redis
import structlog

logger = structlog.get_logger()


class RedisClient:
    """Redis client for caching and short-term memory"""

    def __init__(self):
        self.host = os.getenv("REDIS_HOST", "redis")
        self.port = int(os.getenv("REDIS_PORT", "6379"))
        self.password = os.getenv("REDIS_PASSWORD", None)
        self._client: Optional[redis.Redis] = None
        self._binary_client: Optional[redis.Redis] = None

    @property
    def client(self) -> redis.Redis:
        """Get or create Redis client (string responses)"""
        if not self._client:
            self._client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                decode_responses=True
            )
        return self._client

    @property
    def binary_client(self) -> redis.Redis:
        """Get or create Redis client for binary data (embeddings)"""
        if not self._binary_client:
            self._binary_client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                decode_responses=False  # Keep binary data
            )
        return self._binary_client

    def set(self, key: str, value: Any, ex: Optional[int] = None):
        """Set key-value pair with optional expiry"""
        try:
            serialized = json.dumps(value) if not isinstance(value, str) else value
            self.client.set(key, serialized, ex=ex)
            logger.debug("redis_set", key=key, ex=ex)
        except Exception as e:
            logger.error("redis_set_error", error=str(e), key=key)
            raise

    def get(self, key: str) -> Optional[Any]:
        """Get value by key"""
        try:
            value = self.client.get(key)
            if value is None:
                return None
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        except Exception as e:
            logger.error("redis_get_error", error=str(e), key=key)
            return None

    def set_embedding(self, key: str, embedding: "np.ndarray", ex: Optional[int] = 86400):
        """
        Store numpy embedding in Redis with base64 encoding.

        Args:
            key: Cache key
            embedding: Numpy array (1D or 2D)
            ex: Expiry in seconds (default 24 hours)
        """
        try:
            import numpy as np
            # Convert to bytes
            embedding_bytes = embedding.tobytes()
            shape = embedding.shape
            dtype = str(embedding.dtype)

            # Store metadata and data
            metadata = json.dumps({"shape": shape, "dtype": dtype})
            data = base64.b64encode(embedding_bytes).decode('ascii')

            # Store as hash
            self.client.hset(key, mapping={
                "metadata": metadata,
                "data": data
            })
            if ex:
                self.client.expire(key, ex)

            logger.debug("redis_embedding_set", key=key, shape=shape)
        except Exception as e:
            logger.warning("redis_embedding_set_error", error=str(e), key=key)

    def get_embedding(self, key: str) -> Optional["np.ndarray"]:
        """
        Retrieve numpy embedding from Redis.

        Args:
            key: Cache key

        Returns:
            Numpy array or None if not found
        """
        try:
            import numpy as np

            # Get hash data
            result = self.client.hgetall(key)
            if not result:
                return None

            metadata = json.loads(result.get("metadata", "{}"))
            data = result.get("data")

            if not data or not metadata:
                return None

            # Decode and reconstruct
            embedding_bytes = base64.b64decode(data)
            shape = tuple(metadata.get("shape", []))
            dtype = metadata.get("dtype", "float32")

            embedding = np.frombuffer(embedding_bytes, dtype=dtype).reshape(shape)
            logger.debug("redis_embedding_get", key=key, shape=shape)
            return embedding

        except Exception as e:
            logger.warning("redis_embedding_get_error", error=str(e), key=key)
            return None

    def get_embeddings_batch(self, keys: List[str]) -> dict:
        """
        Retrieve multiple embeddings from Redis.

        Args:
            keys: List of cache keys

        Returns:
            Dict mapping keys to embeddings (missing keys not included)
        """
        results = {}
        for key in keys:
            embedding = self.get_embedding(key)
            if embedding is not None:
                results[key] = embedding
        return results

    def delete(self, key: str):
        """Delete key"""
        try:
            self.client.delete(key)
            logger.debug("redis_delete", key=key)
        except Exception as e:
            logger.error("redis_delete_error", error=str(e), key=key)

    def delete_pattern(self, pattern: str):
        """Delete all keys matching pattern"""
        try:
            keys = self.client.keys(pattern)
            if keys:
                self.client.delete(*keys)
                logger.info("redis_delete_pattern", pattern=pattern, count=len(keys))
        except Exception as e:
            logger.error("redis_delete_pattern_error", error=str(e), pattern=pattern)

    def exists(self, key: str) -> bool:
        """Check if key exists"""
        try:
            return self.client.exists(key) > 0
        except Exception as e:
            logger.warning("redis_exists_error", error=str(e))
            return False

    def expire(self, key: str, seconds: int):
        """Set expiry on key"""
        self.client.expire(key, seconds)

    def ping(self) -> bool:
        """Check Redis connection"""
        try:
            return self.client.ping()
        except Exception as e:
            logger.warning("redis_ping_failed", error=str(e))
            return False

    def close(self):
        """Close Redis connection"""
        if self._client:
            self._client.close()
        if self._binary_client:
            self._binary_client.close()


# Global Redis client instance
redis_client = RedisClient()


def get_redis_client() -> RedisClient:
    """Get the global Redis client instance."""
    return redis_client


def redis_cache(key: str, ttl: int = 300):
    """
    Decorator for caching function results in Redis.

    Args:
        key: Cache key prefix
        ttl: Time-to-live in seconds (default 5 minutes)

    Usage:
        @redis_cache("my_function", ttl=600)
        def my_function(arg1, arg2):
            return expensive_computation(arg1, arg2)
    """
    import functools
    import hashlib

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Build cache key from function args
            cache_key = f"{key}:{hashlib.md5(str((args, kwargs)).encode()).hexdigest()}"

            # Check cache
            cached = redis_client.get(cache_key)
            if cached is not None:
                logger.debug("redis_cache_hit", key=cache_key)
                return cached

            # Execute function
            result = func(*args, **kwargs)

            # Store in cache
            redis_client.set(cache_key, result, ex=ttl)
            logger.debug("redis_cache_set", key=cache_key, ttl=ttl)

            return result

        return wrapper
    return decorator
