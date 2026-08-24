#!/usr/bin/env python3
"""
Log Ingestion Service
HTTP API endpoint for receiving logs from external applications
"""

import json
import os
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
from kafka import KafkaProducer
from kafka.errors import KafkaError
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global Kafka producer
kafka_producer = None

class LogEntry(BaseModel):
    """Pydantic model for log entry validation"""
    timestamp: Optional[str] = None
    service: str = Field(..., min_length=1, max_length=100)
    level: str = Field(..., regex=r'^(DEBUG|INFO|WARN|ERROR|FATAL)$')
    message: str = Field(..., min_length=1, max_length=1000)
    user_id: Optional[str] = Field(None, max_length=100)
    request_id: Optional[str] = Field(None, max_length=100)
    response_time: Optional[int] = Field(None, ge=0)
    status_code: Optional[int] = Field(None, ge=100, le=599)
    endpoint: Optional[str] = Field(None, max_length=255)
    ip_address: Optional[str] = Field(None, max_length=45)
    metadata: Optional[Dict] = None

    @validator('timestamp', pre=True, always=True)
    def set_timestamp(cls, v):
        if v is None:
            return datetime.now(timezone.utc).isoformat()
        return v

    @validator('metadata', pre=True, always=True)
    def validate_metadata(cls, v):
        if v is None:
            return {}
        if isinstance(v, dict):
            return v
        raise ValueError('metadata must be a dictionary')

class LogBatch(BaseModel):
    """Pydantic model for batch log entries"""
    logs: List[LogEntry] = Field(..., min_items=1, max_items=100)

class IngestionStats:
    """Simple statistics tracking"""
    def __init__(self):
        self.total_received = 0
        self.total_sent = 0
        self.total_errors = 0
        self.start_time = datetime.now()

    def to_dict(self):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        return {
            'total_received': self.total_received,
            'total_sent': self.total_sent,
            'total_errors': self.total_errors,
            'success_rate': (self.total_sent / self.total_received * 100) if self.total_received > 0 else 0,
            'uptime_seconds': elapsed,
            'events_per_second': self.total_received / elapsed if elapsed > 0 else 0
        }

# Global statistics
stats = IngestionStats()

def get_kafka_producer():
    """Initialize Kafka producer with error handling"""
    global kafka_producer
    if kafka_producer is None:
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        kafka_topic = os.getenv('KAFKA_TOPIC', 'raw-logs')
        
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda v: v.encode('utf-8') if v else None,
                retry_backoff_ms=100,
                retries=3,
                acks='all',  # Wait for all replicas to acknowledge
                max_in_flight_requests_per_connection=5,
                batch_size=16384,
                linger_ms=10  # Small delay to batch messages
            )
            logger.info(f"Kafka producer initialized - servers: {kafka_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    return kafka_producer

async def send_to_kafka(log_entry: dict):
    """Send log entry to Kafka asynchronously"""
    try:
        producer = get_kafka_producer()
        
        # Use service name as partition key for better distribution
        future = producer.send(
            os.getenv('KAFKA_TOPIC', 'raw-logs'),
            value=log_entry,
            key=log_entry.get('service', 'unknown')
        )
        
        # Don't wait for acknowledgment to maintain high throughput
        stats.total_sent += 1
        logger.debug(f"Sent log to Kafka: {log_entry['service']} - {log_entry['level']}")
        
    except KafkaError as e:
        stats.total_errors += 1
        logger.error(f"Kafka error: {e}")
        raise HTTPException(status_code=503, detail="Message queue unavailable")
    except Exception as e:
        stats.total_errors += 1
        logger.error(f"Unexpected error sending to Kafka: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    # Startup
    logger.info("Starting Log Ingestion Service")
    try:
        get_kafka_producer()  # Initialize Kafka connection
        logger.info("Log Ingestion Service started successfully")
    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down Log Ingestion Service")
    global kafka_producer
    if kafka_producer:
        kafka_producer.close()
    logger.info("Log Ingestion Service stopped")

# Create FastAPI app
app = FastAPI(
    title="Log Ingestion Service",
    description="HTTP API for receiving and forwarding log entries to Kafka",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test Kafka connection
        producer = get_kafka_producer()
        # Simple test - get cluster metadata
        producer.bootstrap_connected()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "kafka_connected": True,
            "service": "log-ingestion"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
                "service": "log-ingestion"
            }
        )

@app.get("/metrics")
async def get_metrics():
    """Get service metrics"""
    return {
        "service": "log-ingestion",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "statistics": stats.to_dict()
    }

@app.post("/api/logs/ingest")
async def ingest_log(log_entry: LogEntry, background_tasks: BackgroundTasks, request: Request):
    """Ingest a single log entry"""
    try:
        stats.total_received += 1
        
        # Convert to dict and add ingestion metadata
        log_dict = log_entry.dict()
        log_dict['ingestion_timestamp'] = datetime.now(timezone.utc).isoformat()
        log_dict['source_ip'] = request.client.host
        
        # Send to Kafka in background
        background_tasks.add_task(send_to_kafka, log_dict)
        
        return {
            "status": "accepted",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "log_id": f"{log_entry.service}_{log_entry.timestamp}"
        }
        
    except Exception as e:
        stats.total_errors += 1
        logger.error(f"Error ingesting log: {e}")
        raise HTTPException(status_code=500, detail="Failed to ingest log")

@app.post("/api/logs/batch")
async def ingest_batch(batch: LogBatch, background_tasks: BackgroundTasks, request: Request):
    """Ingest multiple log entries in a batch"""
    try:
        stats.total_received += len(batch.logs)
        
        # Process each log in the batch
        for log_entry in batch.logs:
            log_dict = log_entry.dict()
            log_dict['ingestion_timestamp'] = datetime.now(timezone.utc).isoformat()
            log_dict['source_ip'] = request.client.host
            
            # Send to Kafka in background
            background_tasks.add_task(send_to_kafka, log_dict)
        
        return {
            "status": "accepted",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "batch_size": len(batch.logs),
            "message": f"Batch of {len(batch.logs)} logs accepted for processing"
        }
        
    except Exception as e:
        stats.total_errors += len(batch.logs)
        logger.error(f"Error ingesting batch: {e}")
        raise HTTPException(status_code=500, detail="Failed to ingest batch")

@app.get("/api/logs/test")
async def test_endpoint():
    """Test endpoint to generate a sample log"""
    test_log = LogEntry(
        service="test-service",
        level="INFO",
        message="Test log entry from ingestion service",
        user_id="test_user",
        request_id="test_request",
        response_time=100,
        status_code=200,
        endpoint="/api/test",
        metadata={"test": True}
    )
    
    # Process the test log
    background_tasks = BackgroundTasks()
    return await ingest_log(test_log, background_tasks, Request({"type": "http", "client": ("127.0.0.1", 0)}))

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": "Internal server error",
            "path": str(request.url)
        }
    )

if __name__ == "__main__":
    port = int(os.getenv('PORT', '8081'))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")