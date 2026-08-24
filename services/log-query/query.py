import json
import logging
import os
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum

import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Pydantic models
class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class LogQueryRequest(BaseModel):
    start_time: Optional[str] = Field(None, description="Start time in ISO format")
    end_time: Optional[str] = Field(None, description="End time in ISO format")
    service: Optional[str] = Field(None, description="Service name filter")
    level: Optional[LogLevel] = Field(None, description="Log level filter")
    user_id: Optional[str] = Field(None, description="User ID filter")
    message_contains: Optional[str] = Field(None, description="Message text search")
    limit: int = Field(100, ge=1, le=1000, description="Maximum number of results")
    offset: int = Field(0, ge=0, description="Offset for pagination")
    order_by: str = Field("timestamp", description="Order by field")
    order_desc: bool = Field(True, description="Descending order")

class LogResponse(BaseModel):
    id: str
    timestamp: str
    service: str
    level: str
    message: str
    user_id: Optional[str]
    metadata: Dict[str, Any]
    created_at: str

class LogQueryResponse(BaseModel):
    logs: List[LogResponse]
    total_count: int
    has_more: bool
    query_time_ms: int
    data_source: str  # "redis" or "postgres" or "mixed"

class WebSocketMessage(BaseModel):
    type: str  # "log", "error", "info"
    data: Optional[Dict[str, Any]] = None
    message: Optional[str] = None
    timestamp: str

@dataclass
class QueryConfig:
    """Configuration for query service"""
    redis_host: str
    redis_port: int
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    port: int

class WebSocketManager:
    """Manages WebSocket connections for real-time log streaming"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.connection_filters: Dict[WebSocket, Dict[str, Any]] = {}
        
    async def connect(self, websocket: WebSocket, filters: Dict[str, Any] = None):
        """Accept WebSocket connection and store filters"""
        await websocket.accept()
        self.active_connections.append(websocket)
        self.connection_filters[websocket] = filters or {}
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")
        
    def disconnect(self, websocket: WebSocket):
        """Remove WebSocket connection"""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if websocket in self.connection_filters:
            del self.connection_filters[websocket]
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
        
    async def broadcast_log(self, log_data: Dict[str, Any]):
        """Broadcast log to all connected clients with matching filters"""
        if not self.active_connections:
            return
            
        message = WebSocketMessage(
            type="log",
            data=log_data,
            timestamp=datetime.utcnow().isoformat() + 'Z'
        )
        
        disconnected = []
        for websocket in self.active_connections:
            try:
                # Check if log matches connection filters
                if self._matches_filters(log_data, self.connection_filters[websocket]):
                    await websocket.send_text(message.json())
            except Exception as e:
                logger.error(f"Error broadcasting to WebSocket: {e}")
                disconnected.append(websocket)
                
        # Clean up disconnected websockets
        for websocket in disconnected:
            self.disconnect(websocket)
            
    def _matches_filters(self, log_data: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        """Check if log matches WebSocket filters"""
        if not filters:
            return True
            
        # Check service filter
        if 'service' in filters and filters['service'] != log_data.get('service'):
            return False
            
        # Check level filter
        if 'level' in filters and filters['level'] != log_data.get('level'):
            return False
            
        # Check user_id filter
        if 'user_id' in filters and filters['user_id'] != log_data.get('user_id'):
            return False
            
        return True

class LogQueryService:
    def __init__(self):
        self.config = self._load_config()
        self.redis_client = None
        self.postgres_pool = None
        self.websocket_manager = WebSocketManager()
        
    def _load_config(self) -> QueryConfig:
        """Load configuration from environment variables"""
        return QueryConfig(
            redis_host=os.getenv('REDIS_HOST', 'localhost'),
            redis_port=int(os.getenv('REDIS_PORT', 6379)),
            postgres_host=os.getenv('POSTGRES_HOST', 'localhost'),
            postgres_port=int(os.getenv('POSTGRES_PORT', 5432)),
            postgres_db=os.getenv('POSTGRES_DB', 'logs_db'),
            postgres_user=os.getenv('POSTGRES_USER', 'postgres'),
            postgres_password=os.getenv('POSTGRES_PASSWORD', 'password'),
            port=int(os.getenv('PORT', 8080))
        )
        
    async def initialize(self):
        """Initialize connections"""
        await self._setup_redis()
        await self._setup_postgres()
        
    async def _setup_redis(self):
        """Initialize Redis connection"""
        try:
            self.redis_client = redis.Redis(
                host=self.config.redis_host,
                port=self.config.redis_port,
                decode_responses=True,
                socket_keepalive=True,
                socket_keepalive_options={}
            )
            
            # Test connection
            self.redis_client.ping()
            logger.info(f"Redis connected: {self.config.redis_host}:{self.config.redis_port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
            
    async def _setup_postgres(self):
        """Initialize PostgreSQL connection pool"""
        try:
            self.postgres_pool = ThreadedConnectionPool(
                minconn=5,
                maxconn=20,
                host=self.config.postgres_host,
                port=self.config.postgres_port,
                database=self.config.postgres_db,
                user=self.config.postgres_user,
                password=self.config.postgres_password
            )
            
            logger.info(f"PostgreSQL connected: {self.config.postgres_host}:{self.config.postgres_port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
            
    async def query_logs(self, query_request: LogQueryRequest) -> LogQueryResponse:
        """Query logs from Redis (hot) and PostgreSQL (cold)"""
        start_time = datetime.utcnow()
        
        # Try Redis first for recent data
        redis_logs = await self._query_redis(query_request)
        
        # If we need more data or specific time range, query PostgreSQL
        postgres_logs = []
        if len(redis_logs) < query_request.limit or self._needs_postgres_query(query_request):
            postgres_logs = await self._query_postgres(query_request, len(redis_logs))
        
        # Combine and deduplicate results
        all_logs = self._combine_and_deduplicate(redis_logs, postgres_logs)
        
        # Apply final sorting and pagination
        sorted_logs = self._sort_and_paginate(all_logs, query_request)
        
        # Get total count for pagination
        total_count = await self._get_total_count(query_request)
        
        query_time_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        
        # Determine data source
        data_source = "mixed"
        if redis_logs and not postgres_logs:
            data_source = "redis"
        elif postgres_logs and not redis_logs:
            data_source = "postgres"
        
        return LogQueryResponse(
            logs=[LogResponse(**log) for log in sorted_logs],
            total_count=total_count,
            has_more=len(sorted_logs) >= query_request.limit,
            query_time_ms=query_time_ms,
            data_source=data_source
        )
        
    async def _query_redis(self, query_request: LogQueryRequest) -> List[Dict[str, Any]]:
        """Query logs from Redis using sorted sets"""
        try:
            # Build time range
            start_score = 0
            end_score = datetime.utcnow().timestamp()
            
            if query_request.start_time:
                start_dt = datetime.fromisoformat(query_request.start_time.replace('Z', '+00:00'))
                start_score = start_dt.timestamp()
            if query_request.end_time:
                end_dt = datetime.fromisoformat(query_request.end_time.replace('Z', '+00:00'))
                end_score = end_dt.timestamp()
            
            # Choose the right sorted set based on filters
            if query_request.service:
                key = f"logs:by_service:{query_request.service}"
            elif query_request.level:
                key = f"logs:by_level:{query_request.level.value}"
            else:
                key = "logs:by_time"
            
            # Get log IDs from sorted set
            log_ids = self.redis_client.zrevrangebyscore(
                key, end_score, start_score,
                start=query_request.offset,
                num=query_request.limit
            )
            
            # Fetch full log data
            logs = []
            for log_id in log_ids:
                log_key = f"log:{log_id}"
                log_data = self.redis_client.get(log_key)
                if log_data:
                    log_dict = json.loads(log_data)
                    if self._matches_filters(log_dict, query_request):
                        logs.append(log_dict)
            
            return logs
            
        except Exception as e:
            logger.error(f"Error querying Redis: {e}")
            return []
            
    async def _query_postgres(self, query_request: LogQueryRequest, skip_count: int = 0) -> List[Dict[str, Any]]:
        """Query logs from PostgreSQL"""
        conn = self.postgres_pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Build query conditions
                conditions = []
                params = []
                
                if query_request.start_time:
                    conditions.append("timestamp >= %s")
                    params.append(query_request.start_time)
                if query_request.end_time:
                    conditions.append("timestamp <= %s")
                    params.append(query_request.end_time)
                if query_request.service:
                    conditions.append("service = %s")
                    params.append(query_request.service)
                if query_request.level:
                    conditions.append("level = %s")
                    params.append(query_request.level.value)
                if query_request.user_id:
                    conditions.append("user_id = %s")
                    params.append(query_request.user_id)
                if query_request.message_contains:
                    conditions.append("message ILIKE %s")
                    params.append(f"%{query_request.message_contains}%")
                
                # Build WHERE clause
                where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
                
                # Build ORDER BY clause
                order_direction = "DESC" if query_request.order_desc else "ASC"
                order_clause = f"ORDER BY {query_request.order_by} {order_direction}"
                
                # Build LIMIT and OFFSET
                limit = query_request.limit - skip_count if skip_count > 0 else query_request.limit
                offset = query_request.offset + skip_count if skip_count > 0 else query_request.offset
                
                query = f"""
                    SELECT id, timestamp, service, level, message, user_id, metadata, created_at
                    FROM logs
                    {where_clause}
                    {order_clause}
                    LIMIT %s OFFSET %s
                """
                
                params.extend([limit, offset])
                cursor.execute(query, params)
                
                results = cursor.fetchall()
                
                # Convert to list of dicts
                logs = []
                for row in results:
                    log_dict = dict(row)
                    # Convert datetime objects to ISO strings
                    log_dict['timestamp'] = log_dict['timestamp'].isoformat() + 'Z'
                    log_dict['created_at'] = log_dict['created_at'].isoformat() + 'Z'
                    logs.append(log_dict)
                
                return logs
                
        except Exception as e:
            logger.error(f"Error querying PostgreSQL: {e}")
            return []
        finally:
            self.postgres_pool.putconn(conn)
            
    def _needs_postgres_query(self, query_request: LogQueryRequest) -> bool:
        """Determine if PostgreSQL query is needed"""
        # If querying older data (beyond Redis TTL), we need PostgreSQL
        if query_request.start_time:
            start_dt = datetime.fromisoformat(query_request.start_time.replace('Z', '+00:00'))
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            if start_dt < cutoff_time:
                return True
        
        # If doing text search, PostgreSQL is better
        if query_request.message_contains:
            return True
            
        return False
        
    def _combine_and_deduplicate(self, redis_logs: List[Dict], postgres_logs: List[Dict]) -> List[Dict]:
        """Combine logs from both sources and remove duplicates"""
        seen_ids = set()
        combined_logs = []
        
        # Add Redis logs first (they're more recent)
        for log in redis_logs:
            if log['id'] not in seen_ids:
                combined_logs.append(log)
                seen_ids.add(log['id'])
        
        # Add PostgreSQL logs that aren't already included
        for log in postgres_logs:
            if log['id'] not in seen_ids:
                combined_logs.append(log)
                seen_ids.add(log['id'])
                
        return combined_logs
        
    def _sort_and_paginate(self, logs: List[Dict], query_request: LogQueryRequest) -> List[Dict]:
        """Sort and paginate combined results"""
        # Sort by timestamp (default) or other fields
        if query_request.order_by == "timestamp":
            logs.sort(
                key=lambda x: x['timestamp'],
                reverse=query_request.order_desc
            )
        
        # Apply pagination
        start_idx = query_request.offset
        end_idx = start_idx + query_request.limit
        
        return logs[start_idx:end_idx]
        
    def _matches_filters(self, log_dict: Dict[str, Any], query_request: LogQueryRequest) -> bool:
        """Check if log matches query filters"""
        if query_request.service and log_dict.get('service') != query_request.service:
            return False
        if query_request.level and log_dict.get('level') != query_request.level.value:
            return False
        if query_request.user_id and log_dict.get('user_id') != query_request.user_id:
            return False
        if query_request.message_contains and query_request.message_contains.lower() not in log_dict.get('message', '').lower():
            return False
        
        return True
        
    async def _get_total_count(self, query_request: LogQueryRequest) -> int:
        """Get total count for pagination"""
        # This is a simplified version - in production, you'd want to optimize this
        try:
            conn = self.postgres_pool.getconn()
            with conn.cursor() as cursor:
                conditions = []
                params = []
                
                if query_request.start_time:
                    conditions.append("timestamp >= %s")
                    params.append(query_request.start_time)
                if query_request.end_time:
                    conditions.append("timestamp <= %s")
                    params.append(query_request.end_time)
                if query_request.service:
                    conditions.append("service = %s")
                    params.append(query_request.service)
                if query_request.level:
                    conditions.append("level = %s")
                    params.append(query_request.level.value)
                if query_request.user_id:
                    conditions.append("user_id = %s")
                    params.append(query_request.user_id)
                if query_request.message_contains:
                    conditions.append("message ILIKE %s")
                    params.append(f"%{query_request.message_contains}%")
                
                where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
                
                query = f"SELECT COUNT(*) FROM logs {where_clause}"
                cursor.execute(query, params)
                
                return cursor.fetchone()[0]
                
        except Exception as e:
            logger.error(f"Error getting total count: {e}")
            return 0
        finally:
            self.postgres_pool.putconn(conn)

# Initialize service
query_service = LogQueryService()

# FastAPI app
app = FastAPI(
    title="Distributed Log Processing Engine - Query API",
    description="REST API and WebSocket service for querying logs",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    """Initialize connections on startup"""
    await query_service.initialize()

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"status": "healthy", "service": "log-query", "timestamp": datetime.utcnow().isoformat()}

@app.get("/health")
async def health_check():
    """Detailed health check"""
    try:
        # Test Redis connection
        query_service.redis_client.ping()
        redis_status = "healthy"
    except Exception as e:
        redis_status = f"unhealthy: {e}"
    
    try:
        # Test PostgreSQL connection
        conn = query_service.postgres_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        query_service.postgres_pool.putconn(conn)
        postgres_status = "healthy"
    except Exception as e:
        postgres_status = f"unhealthy: {e}"
    
    return {
        "status": "healthy" if redis_status == "healthy" and postgres_status == "healthy" else "degraded",
        "redis": redis_status,
        "postgres": postgres_status,
        "websocket_connections": len(query_service.websocket_manager.active_connections),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/logs/query", response_model=LogQueryResponse)
async def query_logs(query_request: LogQueryRequest):
    """Query logs with filters and pagination"""
    try:
        result = await query_service.query_logs(query_request)
        return result
    except Exception as e:
        logger.error(f"Error querying logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/logs/search", response_model=LogQueryResponse)
async def search_logs(
    start_time: Optional[str] = Query(None, description="Start time in ISO format"),
    end_time: Optional[str] = Query(None, description="End time in ISO format"),
    service: Optional[str] = Query(None, description="Service name filter"),
    level: Optional[LogLevel] = Query(None, description="Log level filter"),
    user_id: Optional[str] = Query(None, description="User ID filter"),
    message_contains: Optional[str] = Query(None, description="Message text search"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    order_by: str = Query("timestamp", description="Order by field"),
    order_desc: bool = Query(True, description="Descending order")
):
    """Search logs using GET parameters"""
    query_request = LogQueryRequest(
        start_time=start_time,
        end_time=end_time,
        service=service,
        level=level,
        user_id=user_id,
        message_contains=message_contains,
        limit=limit,
        offset=offset,
        order_by=order_by,
        order_desc=order_desc
    )
    
    try:
        result = await query_service.query_logs(query_request)
        return result
    except Exception as e:
        logger.error(f"Error searching logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.websocket("/logs/stream")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time log streaming"""
    await query_service.websocket_manager.connect(websocket)
    
    try:
        while True:
            # Wait for client messages (filters, ping, etc.)
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                if message.get('type') == 'set_filters':
                    # Update filters for this connection
                    filters = message.get('filters', {})
                    query_service.websocket_manager.connection_filters[websocket] = filters
                    await websocket.send_text(json.dumps({
                        "type": "info",
                        "message": "Filters updated",
                        "timestamp": datetime.utcnow().isoformat()
                    }))
                elif message.get('type') == 'ping':
                    await websocket.send_text(json.dumps({
                        "type": "pong",
                        "timestamp": datetime.utcnow().isoformat()
                    }))
            except json.JSONDecodeError:
                pass  # Ignore invalid JSON
                
    except WebSocketDisconnect:
        query_service.websocket_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        query_service.websocket_manager.disconnect(websocket)

@app.get("/stats")
async def get_stats():
    """Get service statistics"""
    try:
        # Get Redis stats
        redis_info = query_service.redis_client.info()
        redis_keys = len(query_service.redis_client.keys("log:*"))
        
        # Get PostgreSQL stats
        conn = query_service.postgres_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM logs")
            total_logs = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM logs WHERE created_at >= NOW() - INTERVAL '24 hours'")
            recent_logs = cursor.fetchone()[0]
            
            cursor.execute("SELECT service, COUNT(*) FROM logs GROUP BY service ORDER BY COUNT(*) DESC LIMIT 10")
            top_services = cursor.fetchall()
            
        query_service.postgres_pool.putconn(conn)
        
        return {
            "redis": {
                "connected_clients": redis_info.get("connected_clients", 0),
                "used_memory_human": redis_info.get("used_memory_human", "0B"),
                "hot_logs_count": redis_keys
            },
            "postgres": {
                "total_logs": total_logs,
                "recent_logs_24h": recent_logs,
                "top_services": [{"service": s[0], "count": s[1]} for s in top_services]
            },
            "websocket": {
                "active_connections": len(query_service.websocket_manager.active_connections)
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    config = query_service.config
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=config.port,
        reload=False,
        log_level="info"
    )