
import os
import redis
import logging

logger = logging.getLogger(__name__)

class RedisClient:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            host = os.environ.get("REDIS_HOST", "redis")
            port = int(os.environ.get("REDIS_PORT", "6379"))
            # Support both docker-network host ("redis") and local dev ("localhost").
            host_candidates = [host]
            for fallback in ("localhost", "127.0.0.1", "redis"):
                if fallback not in host_candidates:
                    host_candidates.append(fallback)

            last_error = None
            for candidate in host_candidates:
                try:
                    logger.info("Connecting to Redis at %s:%s", candidate, port)
                    client = redis.Redis(
                        host=candidate,
                        port=port,
                        decode_responses=True,
                        socket_connect_timeout=2,
                    )
                    client.ping()
                    cls._instance = client
                    break
                except Exception as exc:  # pragma: no cover - environment dependent
                    last_error = exc
                    logger.warning("Redis connection failed at %s:%s (%s)", candidate, port, exc)

            if cls._instance is None and last_error is not None:
                raise last_error
        return cls._instance

def get_redis():
    return RedisClient.get_instance()
