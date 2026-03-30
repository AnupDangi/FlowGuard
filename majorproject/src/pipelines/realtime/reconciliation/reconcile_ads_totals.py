#!/usr/bin/env python3
"""Periodic reconciliation for ads counters.

Reads authoritative totals from Bronze tables in analytics Postgres and applies
overwrite-if-greater updates to Redis counters to correct drift.
"""

import argparse
import os
from datetime import datetime, timedelta, timezone

import psycopg2
import redis
from dotenv import load_dotenv

load_dotenv()


def pg_conn():
    return psycopg2.connect(
        host=os.getenv("ANALYTICS_POSTGRES_HOST", "localhost"),
        port=int(os.getenv("ANALYTICS_POSTGRES_PORT", "5434")),
        user=os.getenv("ANALYTICS_POSTGRES_USER", "flowguard"),
        password=os.getenv("ANALYTICS_POSTGRES_PASSWORD", ""),
        dbname=os.getenv("ANALYTICS_POSTGRES_DB", "flowguard_analytics"),
    )


def redis_client():
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
    )


def apply_overwrite_if_greater(r, key: str, value: float):
    current = r.get(key)
    current_v = float(current) if current is not None else 0.0
    if value > current_v:
        pipe = r.pipeline()
        pipe.set(key, value)
        pipe.expire(key, 25 * 3600)
        pipe.execute()
        return True
    return False


def run(lookback_hours: int = 24):
    now = datetime.now(timezone.utc)
    window_start = (now - timedelta(hours=lookback_hours)).replace(minute=0, second=0, microsecond=0)
    # Exclude current incomplete hour to mimic blog strategy.
    window_end = now.replace(minute=0, second=0, microsecond=0)

    pconn = pg_conn()
    r = redis_client()
    updated_keys = 0
    with pconn:
        with pconn.cursor() as cur:
            cur.execute(
                """
                WITH scoped_clicks AS (
                  SELECT *
                  FROM analytics.bronze_clicks_raw
                  WHERE event_timestamp >= %s
                    AND event_timestamp < %s
                ),
                scoped_orders AS (
                  SELECT *
                  FROM analytics.bronze_orders_raw
                  WHERE event_timestamp >= %s
                    AND event_timestamp < %s
                ),
                agg AS (
                  SELECT
                    date_trunc('hour', coalesce(c.event_timestamp, o.event_timestamp)) AS date_hour,
                    COALESCE(c.item_id::text, o.item_id::text) AS item_id,
                    COUNT(*) FILTER (WHERE c.event_type = 'impression') AS impressions,
                    COUNT(*) FILTER (WHERE c.event_type = 'click') AS clicks,
                    COUNT(DISTINCT o.order_id) AS orders,
                    COALESCE(SUM(o.price), 0) AS revenue
                  FROM scoped_clicks c
                  FULL OUTER JOIN scoped_orders o
                    ON c.item_id = o.item_id
                   AND c.user_id = o.user_id
                   AND o.event_timestamp >= c.event_timestamp
                   AND o.event_timestamp <= c.event_timestamp + INTERVAL '30 minute'
                  GROUP BY 1, 2
                ),
                attributed AS (
                  SELECT
                    date_trunc('hour', o.event_timestamp) AS date_hour,
                    o.item_id::text AS item_id,
                    COUNT(DISTINCT o.order_id) AS attributed_orders
                  FROM scoped_orders o
                  JOIN scoped_clicks c
                    ON c.user_id = o.user_id
                   AND c.item_id = o.item_id
                   AND c.event_type = 'click'
                   AND o.event_timestamp >= c.event_timestamp
                   AND o.event_timestamp <= c.event_timestamp + INTERVAL '30 minute'
                  GROUP BY 1, 2
                )
                SELECT
                  a.date_hour,
                  a.item_id,
                  COALESCE(a.impressions, 0),
                  COALESCE(a.clicks, 0),
                  COALESCE(a.orders, 0),
                  COALESCE(at.attributed_orders, 0),
                  COALESCE(a.revenue, 0)
                FROM agg a
                LEFT JOIN attributed at ON a.date_hour = at.date_hour AND a.item_id = at.item_id
                WHERE a.item_id IS NOT NULL
                ORDER BY a.date_hour, a.item_id
                """,
                (window_start, window_end, window_start, window_end),
            )
            rows = cur.fetchall()

            for date_hour, item_id, impr, clk, ords, attr, rev in rows:
                # Persist recon facts
                cur.execute(
                    """
                    INSERT INTO analytics.recon_hourly_totals (
                      date_hour, item_id, impressions, clicks, orders, attributed_orders, revenue, computed_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (date_hour, item_id)
                    DO UPDATE SET
                      impressions = EXCLUDED.impressions,
                      clicks = EXCLUDED.clicks,
                      orders = EXCLUDED.orders,
                      attributed_orders = EXCLUDED.attributed_orders,
                      revenue = EXCLUDED.revenue,
                      computed_at = NOW()
                    """,
                    (date_hour, item_id, impr, clk, ords, attr, rev),
                )

                date_str = date_hour.date().isoformat()
                updated_keys += int(
                    apply_overwrite_if_greater(r, f"ads:impressions:{item_id}:{date_str}", float(impr))
                )
                updated_keys += int(
                    apply_overwrite_if_greater(r, f"ads:clicks:{item_id}:{date_str}", float(clk))
                )
                updated_keys += int(
                    apply_overwrite_if_greater(r, f"ads:orders:{item_id}:{date_str}", float(ords))
                )
                updated_keys += int(
                    apply_overwrite_if_greater(r, f"ads:attributed_orders:{item_id}:{date_str}", float(attr))
                )
                updated_keys += int(
                    apply_overwrite_if_greater(r, f"ads:revenue:{item_id}:{date_str}", float(rev))
                )

    pconn.close()
    print(f"Reconciliation completed. Rows processed={len(rows)} keys_updated={updated_keys}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reconcile ads totals into Redis")
    parser.add_argument("--lookback-hours", type=int, default=24)
    args = parser.parse_args()
    run(lookback_hours=args.lookback_hours)
