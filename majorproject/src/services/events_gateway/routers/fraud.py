"""Fraud alerts read API (Redis-backed, populated by realtime fraud job)."""

import json
import logging
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Query

from src.services.events_gateway.metrics.redis_client import get_redis
from src.services.events_gateway.metrics.runtime_metrics import inc_counter
from src.services.events_gateway.security import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/fraud", tags=["fraud"])


@router.get("/alerts/recent")
async def recent_alerts(
    limit: int = Query(default=20, ge=1, le=100),
    _user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    """Return recent fraud alerts (JSON blobs) from Redis."""
    inc_counter("fraud.api.recent")
    try:
        r = get_redis()
        if not r.ping():
            raise HTTPException(status_code=503, detail="Redis unavailable")
    except Exception as e:
        logger.error("Redis error: %s", e)
        raise HTTPException(status_code=503, detail="Redis unavailable") from e

    raw = r.lrange("fraud:alerts:recent", 0, limit - 1)
    alerts: List[Dict[str, Any]] = []
    for item in raw:
        try:
            alerts.append(json.loads(item))
        except Exception:
            continue
    total = int(r.get("metrics:fraud_alerts_total") or 0)
    return {"alerts": alerts, "total_emitted": total, "limit": limit}


@router.get("/metrics")
async def fraud_metrics(_user: dict = Depends(get_current_user)) -> Dict[str, Any]:
    inc_counter("fraud.api.metrics")
    try:
        r = get_redis()
        if not r.ping():
            raise HTTPException(status_code=503, detail="Redis unavailable")
    except Exception as e:
        raise HTTPException(status_code=503, detail="Redis unavailable") from e

    return {
        "fraud_alerts_total": int(r.get("metrics:fraud_alerts_total") or 0),
        "recent_list_len": r.llen("fraud:alerts:recent"),
    }
