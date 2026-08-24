import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class StorageConfig:
    """Configuration for storage service"""
    kafka_servers: str
    kafka_topic: str
    consumer_group: str
    redis_host: str
    redis_port: int
    redis_ttl_hours: int
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

class LogStorageService:
    def __init__(self):
        self.config = self._load_config()
        self.running = True
        
        # Storage connections
        self.redis_client = None
        self.postgres_pool = None
        self.kafka_consumer = None
        
        # Background thread for data lifecycle management
        self.lifecycle_thread = None
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'redis_writes': 0,
            'postgres_writes': 0,
            'failed_writes': 0,
            'archived_count': 0
        }
        
    def _load_config(self) -> StorageConfig:
        """Load configuration from environment variables"""
        return StorageConfig(
            kafka_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            kafka_topic=os.getenv('KAFKA_TOPIC', 'parsed-logs'),
            consumer_group=os.getenv('KAFKA_GROUP_ID', 'storage-group'),
            redis_host=os.getenv('REDIS_HOST', 'localhost'),
            redis_port=int(os.getenv('REDIS_PORT', 6379)),
            redis_ttl_hours=int(os.getenv('REDIS_TTL_HOURS', 24)),
            postgres_host=os.getenv('POSTGRES_HOST', 'localhost'),
            postgres_port=int(os.getenv('POSTGRES_PORT', 5432)),
            postgres_db=os.getenv('POSTGRES_DB', 'logs_db'),
            postgres_user=os.getenv('POSTGRES_USER', 'postgres'),
            postgres_password=os.getenv('POSTGRES_PASSWORD', 'password')
        )
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
        
    def _setup_redis(self) -> bool:
        """Initialize Redis connection"""
        try:
            self.redis_client = redis.Redis(
                host=self.config.redis_host,
                port=self.config.redis_port,
                decode_responses=True,
                health_check_interval=30,
                socket_keepalive=True,
                socket_keepalive_options={}
            )
            
            # Test connection
            self.redis_client.ping()
            logger.info(f"Redis connected: {self.config.redis_host}:{self.config.redis_port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False
            
    def _setup_postgres(self) -> bool:
        """Initialize PostgreSQL connection pool"""
        try:
            # Create connection pool
            self.postgres_pool = ThreadedConnectionPool(
                minconn=2,
                maxconn=20,
                host=self.config.postgres_host,
                port=self.config.postgres_port,
                database=self.config.postgres_db,
                user=self.config.postgres_user,
                password=self.config.postgres_password
            )
            
            # Test connection and create tables
            self._initialize_database()
            logger.info(f"PostgreSQL connected: {self.config.postgres_host}:{self.config.postgres_port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
            
    def _initialize_database(self):
        """Create database tables if they don't exist"""
        conn = self.postgres_pool.getconn()
        try:
            with conn.cursor() as cursor:
                # Create logs table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS logs (
                        id VARCHAR(36) PRIMARY KEY,
                        timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                        service VARCHAR(100) NOT NULL,
                        level VARCHAR(20) NOT NULL,
                        message TEXT NOT NULL,
                        user_id VARCHAR(100),
                        metadata JSONB,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        archived_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                # Create indexes for common queries
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_logs_timestamp 
                    ON logs(timestamp);
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_logs_service 
                    ON logs(service);
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_logs_level 
                    ON logs(level);
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_logs_user_id 
                    ON logs(user_id) WHERE user_id IS NOT NULL;
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_logs_created_at 
                    ON logs(created_at);
                """)
                
                conn.commit()
                logger.info("Database tables initialized successfully")
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            conn.rollback()
            raise
        finally:
            self.postgres_pool.putconn(conn)
            
    def _setup_kafka_consumer(self) -> bool:
        """Initialize Kafka consumer"""
        try:
            self.kafka_consumer = KafkaConsumer(
                self.config.kafka_topic,
                bootstrap_servers=self.config.kafka_servers,
                group_id=self.config.consumer_group,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                max_poll_records=50,
                consumer_timeout_ms=1000
            )
            
            logger.info(f"Kafka consumer connected: {self.config.kafka_servers}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup Kafka consumer: {e}")
            return False
            
    def _store_to_redis(self, log_data: Dict[str, Any]) -> bool:
        """Store log data to Redis with TTL"""
        try:
            log_id = log_data['id']
            key = f"log:{log_id}"
            
            # Store as JSON with TTL
            ttl_seconds = self.config.redis_ttl_hours * 3600
            
            self.redis_client.setex(
                key,
                ttl_seconds,
                json.dumps(log_data)
            )
            
            # Also add to time-based sorted set for efficient querying
            timestamp = datetime.fromisoformat(log_data['timestamp'].replace('Z', '+00:00'))
            score = timestamp.timestamp()
            
            # Store in sorted sets by service and level for efficient filtering
            self.redis_client.zadd(f"logs:by_time", {log_id: score})
            self.redis_client.zadd(f"logs:by_service:{log_data['service']}", {log_id: score})
            self.redis_client.zadd(f"logs:by_level:{log_data['level']}", {log_id: score})
            
            # Set TTL on sorted sets too
            self.redis_client.expire(f"logs:by_time", ttl_seconds)
            self.redis_client.expire(f"logs:by_service:{log_data['service']}", ttl_seconds)
            self.redis_client.expire(f"logs:by_level:{log_data['level']}", ttl_seconds)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to store log to Redis: {e}")
            return False
            
    def _store_to_postgres(self, log_data: Dict[str, Any]) -> bool:
        """Store log data to PostgreSQL"""
        conn = self.postgres_pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO logs (id, timestamp, service, level, message, user_id, metadata, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    log_data['id'],
                    log_data['timestamp'],
                    log_data['service'],
                    log_data['level'],
                    log_data['message'],
                    log_data.get('user_id'),
                    json.dumps(log_data.get('metadata', {})),
                    log_data['created_at']
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Failed to store log to PostgreSQL: {e}")
            conn.rollback()
            return False
        finally:
            self.postgres_pool.putconn(conn)
            
    def _process_log_batch(self, logs: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process a batch of logs"""
        results = {
            'processed': 0,
            'redis_success': 0,
            'postgres_success': 0,
            'failed': 0
        }
        
        for log_data in logs:
            try:
                # Store in both Redis (hot) and PostgreSQL (cold)
                redis_success = self._store_to_redis(log_data)
                postgres_success = self._store_to_postgres(log_data)
                
                if redis_success:
                    results['redis_success'] += 1
                    self.stats['redis_writes'] += 1
                    
                if postgres_success:
                    results['postgres_success'] += 1
                    self.stats['postgres_writes'] += 1
                    
                if redis_success or postgres_success:
                    results['processed'] += 1
                    self.stats['total_processed'] += 1
                else:
                    results['failed'] += 1
                    self.stats['failed_writes'] += 1
                    
            except Exception as e:
                logger.error(f"Error processing log {log_data.get('id', 'unknown')}: {e}")
                results['failed'] += 1
                self.stats['failed_writes'] += 1
                
        return results
        
    def _cleanup_expired_data(self):
        """Background task to clean up expired data"""
        logger.info("Starting data lifecycle management thread")
        
        while self.running:
            try:
                # Clean up old entries from Redis sorted sets
                cutoff_time = datetime.utcnow() - timedelta(hours=self.config.redis_ttl_hours)
                cutoff_score = cutoff_time.timestamp()
                
                # Remove expired entries from sorted sets
                for key in self.redis_client.scan_iter(match="logs:by_*"):
                    removed = self.redis_client.zremrangebyscore(key, 0, cutoff_score)
                    if removed > 0:
                        logger.debug(f"Removed {removed} expired entries from {key}")
                
                # Sleep for 1 hour before next cleanup
                time.sleep(3600)
                
            except Exception as e:
                logger.error(f"Error in data lifecycle management: {e}")
                time.sleep(60)  # Wait 1 minute before retry
                
    def start(self):
        """Start the log storage service"""
        logger.info("Starting Log Storage Service...")
        
        # Setup connections
        if not self._setup_redis():
            logger.error("Failed to setup Redis, exiting")
            return
            
        if not self._setup_postgres():
            logger.error("Failed to setup PostgreSQL, exiting")
            return
            
        if not self._setup_kafka_consumer():
            logger.error("Failed to setup Kafka consumer, exiting")
            return
            
        # Start background data lifecycle management
        self.lifecycle_thread = threading.Thread(
            target=self._cleanup_expired_data,
            daemon=True
        )
        self.lifecycle_thread.start()
        
        logger.info("Log Storage Service started successfully")
        
        try:
            while self.running:
                try:
                    # Poll for messages
                    message_batch = self.kafka_consumer.poll(timeout_ms=1000)
                    
                    if message_batch:
                        # Collect all messages from all partitions
                        all_logs = []
                        for topic_partition, messages in message_batch.items():
                            for message in messages:
                                all_logs.append(message.value)
                        
                        if all_logs:
                            # Process batch
                            results = self._process_log_batch(all_logs)
                            
                            if results['processed'] > 0:
                                logger.info(
                                    f"Processed {results['processed']} logs: "
                                    f"Redis({results['redis_success']}), "
                                    f"PostgreSQL({results['postgres_success']}), "
                                    f"Failed({results['failed']})"
                                )
                            
                            # Log overall stats periodically
                            if self.stats['total_processed'] % 1000 == 0 and self.stats['total_processed'] > 0:
                                logger.info(
                                    f"Total stats - Processed: {self.stats['total_processed']}, "
                                    f"Redis: {self.stats['redis_writes']}, "
                                    f"PostgreSQL: {self.stats['postgres_writes']}, "
                                    f"Failed: {self.stats['failed_writes']}"
                                )
                                
                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            self._cleanup()
            
        logger.info("Log Storage Service stopped")
        
    def _cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up resources...")
        
        self.running = False
        
        if self.kafka_consumer:
            try:
                self.kafka_consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
                
        if self.postgres_pool:
            try:
                self.postgres_pool.closeall()
                logger.info("PostgreSQL pool closed")
            except Exception as e:
                logger.error(f"Error closing PostgreSQL pool: {e}")
                
        if self.redis_client:
            try:
                self.redis_client.close()
                logger.info("Redis client closed")
            except Exception as e:
                logger.error(f"Error closing Redis client: {e}")

def main():
    """Main entry point"""
    service = LogStorageService()
    
    try:
        service.start()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()