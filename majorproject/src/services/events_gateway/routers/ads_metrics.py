
from fastapi import APIRouter, Depends, HTTPException, Query
from src.services.events_gateway.metrics.redis_client import get_redis
from datetime import datetime
import logging
from typing import List, Dict, Any

from src.services.events_gateway.db import get_db_cursor
from src.services.events_gateway.security import get_current_user
from src.services.events_gateway.metrics.runtime_metrics import inc_counter

router = APIRouter(
    prefix="/api/v1/ads",
    tags=["ads"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)

@router.get("/metrics/{item_id}")
async def get_ad_metrics(item_id: str):
    """
    Get real-time ad performance metrics for a specific item.
    Returns daily stats (impressions, clicks, orders, revenue, etc.).
    """
    redis_client = get_redis()
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Check if redis is available
    try:
        if not redis_client.ping():
             raise HTTPException(status_code=503, detail="Redis unavailable")
    except Exception as e:
        logger.error(f"Redis connection error: {e}")
        # Return zeros gracefully if Redis is down
        return {
            "item_id": item_id,
            "date": today,
            "impressions": 0,
            "clicks": 0,
            "orders": 0,
            "attributed_orders": 0,
            "revenue": 0.0,
            "ctr": 0.0,
            "cvr": 0.0,
            "error": "Redis unavailable"
        }

    keys = {
        "impressions": f"ads:impressions:{item_id}:{today}",
        "clicks": f"ads:clicks:{item_id}:{today}",
        "orders": f"ads:orders:{item_id}:{today}",
        "attributed_orders": f"ads:attributed_orders:{item_id}:{today}",
        "revenue": f"ads:revenue:{item_id}:{today}"
    }

    # Pipeline for efficient fetching
    pipe = redis_client.pipeline()
    for k in keys.values():
        pipe.get(k)
    results = pipe.execute()

    # Parse results (Redis returns strings or None)
    metrics = {
        "impressions": int(float(results[0] or 0)),
        "clicks": int(float(results[1] or 0)),
        "orders": int(float(results[2] or 0)),
        "attributed_orders": int(float(results[3] or 0)),
        "revenue": float(results[4] or 0.0)
    }

    # Calculate derived metrics
    ctr = (metrics["clicks"] / metrics["impressions"]) if metrics["impressions"] > 0 else 0.0
    cvr = (metrics["attributed_orders"] / metrics["clicks"]) if metrics["clicks"] > 0 else 0.0

    return {
        "item_id": item_id,
        "date": today,
        **metrics,
        "ctr": round(ctr, 4),
        "cvr": round(cvr, 4)
    }


@router.get("/personalized")
async def get_personalized_ads(
    limit: int = Query(default=10, ge=1, le=25),
    current_user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    user_id = int(current_user["id"])
    redis_client = get_redis()
    category_key = f"ads:user:{user_id}:top_categories"
    item_key = f"ads:user:{user_id}:top_items"

    category_scores = redis_client.zrevrange(category_key, 0, 9, withscores=True)
    item_scores = redis_client.zrevrange(item_key, 0, 49, withscores=True)

    ranked_items: List[Dict[str, Any]] = []

    with get_db_cursor() as cursor:
        # 1) Prefer explicit item preferences.
        for item_id, score in item_scores:
            cursor.execute(
                """
                SELECT food_id, name, category, price, image_url, is_available
                FROM foods
                WHERE food_id = %s AND is_available = true
                """,
                (int(item_id),),
            )
            row = cursor.fetchone()
            if row:
                ranked_items.append(
                    {
                        "food_id": row["food_id"],
                        "name": row["name"],
                        "category": row["category"],
                        "price": float(row["price"]),
                        "image_url": row["image_url"],
                        "score": round(float(score), 4),
                        "reason": "user_item_preference",
                    }
                )
            if len(ranked_items) >= limit:
                break

        # 2) Fill with top categories.
        if len(ranked_items) < limit:
            seen = {int(i["food_id"]) for i in ranked_items}
            for category, score in category_scores:
                cursor.execute(
                    """
                    SELECT food_id, name, category, price, image_url
                    FROM foods
                    WHERE is_available = true
                      AND category = %s
                      AND food_id <> ALL(%s)
                    ORDER BY food_id DESC
                    LIMIT %s
                    """,
                    (str(category), list(seen) if seen else [0], max(1, limit - len(ranked_items))),
                )
                rows = cursor.fetchall()
                for row in rows:
                    if int(row["food_id"]) in seen:
                        continue
                    seen.add(int(row["food_id"]))
                    ranked_items.append(
                        {
                            "food_id": row["food_id"],
                            "name": row["name"],
                            "category": row["category"],
                            "price": float(row["price"]),
                            "image_url": row["image_url"],
                            "score": round(float(score), 4),
                            "reason": "user_category_preference",
                        }
                    )
                    if len(ranked_items) >= limit:
                        break
                if len(ranked_items) >= limit:
                    break

        # 3) Cold start fallback.
        if len(ranked_items) < limit:
            cursor.execute(
                """
                SELECT food_id, name, category, price, image_url
                FROM foods
                WHERE is_available = true
                ORDER BY food_id DESC
                LIMIT %s
                """,
                (limit,),
            )
            fallback_rows = cursor.fetchall()
            seen = {int(i["food_id"]) for i in ranked_items}
            for row in fallback_rows:
                if int(row["food_id"]) in seen:
                    continue
                ranked_items.append(
                    {
                        "food_id": row["food_id"],
                        "name": row["name"],
                        "category": row["category"],
                        "price": float(row["price"]),
                        "image_url": row["image_url"],
                        "score": 0.0,
                        "reason": "cold_start_popular",
                    }
                )
                if len(ranked_items) >= limit:
                    break

    if category_scores or item_scores:
        inc_counter("ads.personalized.hit")
    else:
        inc_counter("ads.personalized.empty_profile")
    inc_counter("ads.personalized.requests")

    return {
        "user_id": user_id,
        "items": ranked_items[:limit],
        "debug": {
            "category_signals": len(category_scores),
            "item_signals": len(item_scores),
        },
    }
