"""Orders Router - Handle order event submissions"""

import logging
from uuid6 import uuid7
from datetime import datetime
from fastapi import APIRouter, HTTPException, status
from typing import Dict, Any

from src.shared.schemas.events import OrderEvent
from src.shared.kafka.topics import TopicName
from src.services.events_gateway.producers.kafka_producer import (
    get_producer,
    KafkaProducerError
)
from src.services.events_gateway.db import get_db_cursor

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/orders",
    tags=["orders"],
    responses={
        500: {"description": "Internal server error"},
        503: {"description": "Service unavailable - Kafka connection issue"}
    }
)


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    summary="Submit Order Event",
    description="Submit a user order event - stored in DB then emitted to Kafka"
)
async def create_order_event(order: OrderEvent) -> Dict[str, Any]:
    """
    Submit an order event to the events gateway.
    
    **Flow:**
    1. Generate UUID for order_id (server-side)
    2. Insert into PostgreSQL orders table
    3. If DB insert succeeds, emit to Kafka
    4. Return order_id to client
    """
    try:
        # Generate server-side IDs (UUID v7 for time-ordering)
        order_id = str(uuid7())
        event_id = f"evt_{uuid7().hex[:12]}"
        
        # Ensure timestamp is set
        if not order.timestamp:
            order.timestamp = datetime.utcnow()
        
        # Step 1: Insert into database (source of truth)
        with get_db_cursor() as cursor:
            insert_query = """
                INSERT INTO orders (order_id, user_id, item_id, item_name, price, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                order_id,
                order.user_id,
                order.item_id,
                order.item_name or f"Item {order.item_id}",
                order.price or 0.0,
                'confirmed',
                order.timestamp
            ))
        
        logger.info(f"✅ Order {order_id} inserted to database")
        
        # Step 2: Emit to Kafka (only after DB success)
        order.order_id = order_id
        order.event_id = event_id
        
        event_dict = order.model_dump(mode='json')
        producer = get_producer()
        
        headers = {
            "event_type": "order",
            "source": "events_gateway",
            "user_id": str(order.user_id)
        }
        
        producer.send_event(
            topic=TopicName.RAW_ORDERS,
            event=event_dict,
            key=str(order.user_id),
            headers=headers
        )
        
        logger.info(f"✅ Order {order_id} event emitted to Kafka")
        
        # Return order details to client
        return {
            "success": True,
            "order_id": order_id,
            "event_id": event_id,
            "status": "confirmed",
            "message": "Order placed successfully"
        }
        
    except KafkaProducerError as e:
        logger.error(f"Kafka producer error: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Unable to publish event to Kafka: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error processing order: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal error processing order event: {str(e)}"
        )


@router.get(
    "/health",
    summary="Orders Router Health Check",
    description="Check if orders router is operational"
)
async def health_check() -> Dict[str, Any]:
    """Health check endpoint for orders router"""
    try:
        producer = get_producer()
        stats = producer.get_stats()
        
        return {
            "status": "healthy",
            "service": "orders_router",
            "kafka_connected": True,
            "statistics": stats
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "service": "orders_router",
            "kafka_connected": False,
            "error": str(e)
        }
