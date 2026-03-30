#!/usr/bin/env python3
"""Smoke-test Phase 1 + Phase 2 HTTP contracts against a running Events Gateway.

Requires: Postgres (catalog), Events Gateway on GATEWAY_URL (default http://localhost:8000).

Does not start Kafka/Redis workers; optional checks print SKIP when dependencies are absent.

Usage:
  cd majorproject && python3 scripts/verify_phases.py
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from uuid import uuid4

GATEWAY = os.getenv("GATEWAY_URL", "http://localhost:8000").rstrip("/")


def _req(method: str, path: str, body: dict | None = None, token: str | None = None):
    url = f"{GATEWAY}{path}"
    data = None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    r = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(r, timeout=15) as resp:
        raw = resp.read().decode("utf-8")
        return resp.status, json.loads(raw) if raw else {}


def main() -> int:
    print(f"Gateway: {GATEWAY}")
    try:
        status, _ = _req("GET", "/health")
        assert status == 200
        print("OK  health")
    except Exception as e:
        print(f"FAIL  gateway unreachable: {e}")
        print("      Start Events Gateway: python src/services/events_gateway/main.py")
        return 1

    email = f"verify_{uuid4().hex[:8]}@example.com"
    password = "TestPass123!"
    name = "Verify User"

    try:
        status, signup = _req(
            "POST",
            "/api/v1/auth/signup",
            {"email": email, "password": password, "name": name},
        )
        assert status == 200
        token = signup["access_token"]
        print("OK  auth signup")
    except Exception as e:
        print(f"FAIL  signup: {e}")
        return 1

    try:
        status, me = _req("GET", "/api/v1/auth/me", token=token)
        assert status == 200
        assert me["user"]["email"] == email
        print("OK  auth me")
    except Exception as e:
        print(f"FAIL  me: {e}")
        return 1

    try:
        status, ads = _req("GET", "/api/v1/ads/personalized?limit=2", token=token)
        assert status == 200
        assert "items" in ads
        print(f"OK  ads personalized ({len(ads['items'])} items)")
    except Exception as e:
        print(f"FAIL  personalized ads: {e}")
        return 1

    try:
        status, fraud_m = _req("GET", "/api/v1/fraud/metrics", token=token)
        assert status == 200
        print(f"OK  fraud metrics {fraud_m}")
    except Exception as e:
        print(f"FAIL  fraud metrics: {e}")
        return 1

    try:
        status, _ = _req(
            "POST",
            "/api/v1/behavior/",
            {
                "event_type": "view",
                "item_id": 1,
                "category": "Test",
                "session_id": "verify_sess",
                "source_page": "verify",
                "metadata": {},
            },
            token=token,
        )
        assert status == 202
        print("OK  behavior view")
    except Exception as e:
        print(f"FAIL  behavior: {e}")
        return 1

    print("\nAll automated HTTP checks passed.")
    print("Manual: run ./scripts/start_flink.sh personalize-python and fraud-python with Kafka+Redis for streaming.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
