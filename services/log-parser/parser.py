import json
import logging
import os
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
import signal
import sys

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LogParser:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.raw_topic = os.getenv('KAFKA_TOPIC_RAW', 'raw-logs')
        self.parsed_topic = os.getenv('KAFKA_TOPIC_PARSED', 'parsed-logs')
        self.consumer_group = os.getenv('KAFKA_GROUP_ID', 'parser-group')
        
        self.consumer = None
        self.producer = None
        self.running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
        
    def _setup_kafka_consumer(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                self.raw_topic,
                bootstrap_servers=self.kafka_servers,
                group_id=self.consumer_group,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',  # Start from latest messages
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                max_poll_records=100,  # Process up to 100 messages at once
                consumer_timeout_ms=1000  # Timeout for polling
            )
            logger.info(f"Consumer connected to {self.kafka_servers}, subscribed to {self.raw_topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to setup Kafka consumer: {e}")
            return False
            
    def _setup_kafka_producer(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                batch_size=16384,
                linger_ms=10,  # Wait 10ms to batch messages
                compression_type='gzip'
            )
            logger.info(f"Producer connected to {self.kafka_servers}")
            return True
        except Exception as e:
            logger.error(f"Failed to setup Kafka producer: {e}")
            return False
            
    def _parse_log_event(self, raw_log: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse raw log event into structured format
        
        Args:
            raw_log: Raw log event from Kafka
            
        Returns:
            Parsed log event or None if parsing fails
        """
        try:
            # Extract required fields
            timestamp = raw_log.get('timestamp')
            if not timestamp:
                # If no timestamp, use current time
                timestamp = datetime.utcnow().isoformat() + 'Z'
            
            # Validate timestamp format
            if not timestamp.endswith('Z'):
                # Try to parse and reformat timestamp
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    timestamp = dt.isoformat() + 'Z'
                except:
                    timestamp = datetime.utcnow().isoformat() + 'Z'
            
            # Extract core fields
            service = raw_log.get('service', 'unknown-service')
            level = raw_log.get('level', 'INFO').upper()
            message = raw_log.get('message', '')
            user_id = raw_log.get('user_id')
            
            # Extract metadata fields
            metadata = {}
            metadata_fields = [
                'request_id', 'response_time', 'status_code', 
                'endpoint', 'ip_address', 'user_agent', 'method',
                'error_code', 'stack_trace', 'session_id'
            ]
            
            for field in metadata_fields:
                if field in raw_log and raw_log[field] is not None:
                    metadata[field] = raw_log[field]
            
            # Create structured log event
            parsed_log = {
                'id': str(uuid.uuid4()),
                'timestamp': timestamp,
                'service': service,
                'level': level,
                'message': message,
                'user_id': user_id,
                'metadata': metadata if metadata else {},
                'created_at': datetime.utcnow().isoformat() + 'Z'
            }
            
            return parsed_log
            
        except Exception as e:
            logger.error(f"Failed to parse log event: {e}, raw_log: {raw_log}")
            return None
            
    def _validate_log_level(self, level: str) -> str:
        """Validate and normalize log level"""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        level_upper = level.upper()
        
        if level_upper in valid_levels:
            return level_upper
        
        # Try to map common variations
        level_mapping = {
            'WARN': 'WARNING',
            'ERR': 'ERROR',
            'CRIT': 'CRITICAL',
            'FATAL': 'CRITICAL'
        }
        
        return level_mapping.get(level_upper, 'INFO')
        
    def _send_parsed_log(self, parsed_log: Dict[str, Any]) -> bool:
        """
        Send parsed log to Kafka topic
        
        Args:
            parsed_log: Parsed log event
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use log_id as key for partitioning
            key = parsed_log['id']
            
            future = self.producer.send(
                self.parsed_topic,
                key=key.encode('utf-8'),
                value=parsed_log
            )
            
            # Don't wait for each message - let batching handle it
            return True
            
        except Exception as e:
            logger.error(f"Failed to send parsed log: {e}")
            return False
            
    def _process_batch(self, messages):
        """Process a batch of messages"""
        parsed_count = 0
        failed_count = 0
        
        for message in messages:
            try:
                raw_log = message.value
                parsed_log = self._parse_log_event(raw_log)
                
                if parsed_log:
                    if self._send_parsed_log(parsed_log):
                        parsed_count += 1
                    else:
                        failed_count += 1
                else:
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                failed_count += 1
                
        # Flush producer to ensure messages are sent
        if parsed_count > 0:
            try:
                self.producer.flush(timeout=5)
                logger.info(f"Processed batch: {parsed_count} parsed, {failed_count} failed")
            except Exception as e:
                logger.error(f"Failed to flush producer: {e}")
                
        return parsed_count, failed_count
        
    def start(self):
        """Start the log parser service"""
        logger.info("Starting Log Parser Service...")
        
        # Setup Kafka connections
        if not self._setup_kafka_consumer():
            logger.error("Failed to setup Kafka consumer, exiting")
            return
            
        if not self._setup_kafka_producer():
            logger.error("Failed to setup Kafka producer, exiting")
            return
            
        logger.info("Log Parser Service started successfully")
        
        total_processed = 0
        total_failed = 0
        
        try:
            while self.running:
                try:
                    # Poll for messages
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    
                    if message_batch:
                        # Process all messages in the batch
                        for topic_partition, messages in message_batch.items():
                            processed, failed = self._process_batch(messages)
                            total_processed += processed
                            total_failed += failed
                            
                        # Log progress periodically
                        if total_processed % 1000 == 0 and total_processed > 0:
                            logger.info(f"Total processed: {total_processed}, failed: {total_failed}")
                            
                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            self._cleanup()
            
        logger.info(f"Log Parser Service stopped. Final stats - Processed: {total_processed}, Failed: {total_failed}")
        
    def _cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up resources...")
        
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
                logger.info("Producer closed")
            except Exception as e:
                logger.error(f"Error closing producer: {e}")
                
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Consumer closed")
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")

def main():
    """Main entry point"""
    parser = LogParser()
    
    try:
        parser.start()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()