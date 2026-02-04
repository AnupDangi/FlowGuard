"""Events Gateway Service - FastAPI Main Application

This is the main FastAPI application that serves as the entry point for
user events into the FlowGuard real-time processing pipeline.
"""

import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from src.services.events_gateway.config import get_settings
from src.services.events_gateway.routers import orders, clicks
from src.services.events_gateway.producers.kafka_producer import (
    get_producer,
    close_producer,
    check_kafka_connection
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management
    
    Handles startup and shutdown events for proper resource management.
    """
    # Startup
    logger.info("="*50)
    logger.info("FlowGuard Events Gateway Starting...")
    logger.info("="*50)
    
    # Check Kafka connection
    logger.info("Checking Kafka connection...")
    if check_kafka_connection():
        logger.info("✓ Kafka connection successful")
    else:
        logger.warning("⚠ Kafka connection failed - service may not function properly")
    
    # Initialize producer
    try:
        producer = get_producer()
        logger.info(f"✓ Kafka producer initialized: {producer.get_stats()}")
    except Exception as e:
        logger.error(f"✗ Failed to initialize Kafka producer: {e}")
    
    logger.info("✓ Events Gateway is ready to accept events")
    logger.info("="*50)
    
    yield
    
    # Shutdown
    logger.info("="*50)
    logger.info("FlowGuard Events Gateway Shutting Down...")
    logger.info("="*50)
    
    try:
        close_producer()
        logger.info("✓ Kafka producer closed successfully")
    except Exception as e:
        logger.error(f"✗ Error closing producer: {e}")
    
    logger.info("✓ Events Gateway shutdown complete")
    logger.info("="*50)


# Initialize FastAPI app
settings = get_settings()

app = FastAPI(
    title=settings.api_title,
    description=settings.api_description,
    version=settings.api_version,
    docs_url=settings.docs_url,
    redoc_url=settings.redoc_url,
    lifespan=lifespan
)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Exception Handlers
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler for unhandled errors"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Internal server error occurred",
            "error": str(exc),
            "path": str(request.url)
        }
    )


# Register Routers
app.include_router(orders.router)
app.include_router(clicks.router)


# Root Endpoints
@app.get(
    "/",
    summary="Service Info",
    description="Get basic service information"
)
async def root():
    """Root endpoint with service information"""
    return {
        "service": "FlowGuard Events Gateway",
        "version": settings.api_version,
        "environment": settings.environment,
        "status": "operational",
        "endpoints": {
            "docs": settings.docs_url,
            "health": "/health",
            "orders": "/api/v1/orders",
            "clicks": "/api/v1/clicks"
        }
    }


@app.get(
    "/health",
    summary="Service Health Check",
    description="Check if the service and its dependencies are healthy"
)
async def health_check():
    """Comprehensive health check endpoint"""
    health_status = {
        "service": "events_gateway",
        "status": "healthy",
        "environment": settings.environment,
        "checks": {}
    }
    
    # Check Kafka connection
    try:
        kafka_connected = check_kafka_connection()
        producer = get_producer()
        stats = producer.get_stats()
        
        health_status["checks"]["kafka"] = {
            "status": "healthy" if kafka_connected else "unhealthy",
            "connected": kafka_connected,
            "producer_stats": stats
        }
    except Exception as e:
        logger.error(f"Health check - Kafka error: {e}")
        health_status["checks"]["kafka"] = {
            "status": "unhealthy",
            "connected": False,
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    return health_status


@app.get(
    "/metrics",
    summary="Service Metrics",
    description="Get service metrics and statistics"
)
async def metrics():
    """Get service metrics"""
    try:
        producer = get_producer()
        stats = producer.get_stats()
        
        return {
            "service": "events_gateway",
            "kafka_producer": stats,
            "environment": settings.environment
        }
    except Exception as e:
        logger.error(f"Error retrieving metrics: {e}")
        return {
            "service": "events_gateway",
            "error": str(e)
        }


if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"Starting Events Gateway on {settings.host}:{settings.port}")
    uvicorn.run(
        "src.services.events_gateway.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.reload,
        log_level=settings.log_level.lower()
    )
