"""Behavior Router - canonical behavior events for personalization."""

from datetime import datetime
from typing import Dict, Any
from uuid6 import uuid7

from fastapi import APIRouter, Depends, HTTPException, status

from src.services.events_gateway.producers.kafka_producer import get_producer, KafkaProducerError
from src.services.events_gateway.security import get_current_user
from src.shared.kafka.topics import TopicName
from src.shared.schemas.events import BehaviorEvent

router = APIRouter(prefix="/api/v1/behavior", tags=["behavior"])


@router.post("/", status_code=status.HTTP_202_ACCEPTED)
async def emit_behavior_event(
    event: BehaviorEvent,
    current_user: dict = Depends(get_current_user),
) -> Dict[str, Any]:
    try:
        event.user_id = int(current_user["id"])
        if not event.event_id:
            event.event_id = f"beh_{uuid7().hex[:12]}"
        if not event.event_time:
            event.event_time = datetime.utcnow()

        payload = event.model_dump(mode="json")
        get_producer().send_event(
            topic=TopicName.BEHAVIOR_EVENTS,
            event=payload,
            key=str(event.user_id),
            headers={"event_type": str(event.event_type), "source": "events_gateway"},
        )
        return {"success": True, "event_id": event.event_id}
    except KafkaProducerError as e:
        raise HTTPException(status_code=503, detail=f"Kafka publish failed: {e}") from e
