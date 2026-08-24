#!/usr/bin/env python3
"""
Log Generator Service
Simulates multiple web applications generating realistic log events
"""

import json
import random
import time
import threading
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LogGenerator:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'raw-logs')
        self.log_rate = int(os.getenv('LOG_RATE', '30'))  # events per second
        self.fake = Faker()
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8') if v else None,
            retry_backoff_ms=100,
            retries=3
        )
        
        # Service configurations
        self.services = {
            'auth-service': {
                'endpoints': ['/api/auth/login', '/api/auth/logout', '/api/auth/refresh'],
                'log_levels': ['INFO', 'WARN', 'ERROR'],
                'messages': [
                    'User login successful',
                    'User logout successful', 
                    'Invalid credentials provided',
                    'Token refresh successful',
                    'Authentication failed'
                ]
            },
            'payment-service': {
                'endpoints': ['/api/payments/process', '/api/payments/refund', '/api/payments/status'],
                'log_levels': ['INFO', 'WARN', 'ERROR', 'FATAL'],
                'messages': [
                    'Payment processing successful',
                    'Payment declined by bank',
                    'Refund processed successfully',
                    'Payment gateway timeout',
                    'Critical payment system error'
                ]
            },
            'user-service': {
                'endpoints': ['/api/users/profile', '/api/users/update', '/api/users/delete'],
                'log_levels': ['INFO', 'WARN', 'ERROR', 'DEBUG'],
                'messages': [
                    'User profile retrieved',
                    'User profile updated',
                    'User deletion requested',
                    'Rate limit exceeded',
                    'Database connection slow'
                ]
            },
            'notification-service': {
                'endpoints': ['/api/notifications/send', '/api/notifications/status'],
                'log_levels': ['INFO', 'WARN', 'ERROR'],
                'messages': [
                    'Email notification sent',
                    'Push notification delivered',
                    'SMS notification failed',
                    'Notification queue full',
                    'Invalid notification template'
                ]
            }
        }
        
        self.status_codes = {
            'INFO': [200, 201, 204],
            'WARN': [400, 401, 403, 429],
            'ERROR': [500, 502, 503],
            'FATAL': [500, 503],
            'DEBUG': [200, 201]
        }
        
        self.running = False
        self.stats = {
            'total_sent': 0,
            'last_sent_time': None,
            'start_time': None
        }

    def generate_log_event(self):
        """Generate a realistic log event"""
        service = random.choice(list(self.services.keys()))
        service_config = self.services[service]
        
        level = random.choice(service_config['log_levels'])
        endpoint = random.choice(service_config['endpoints'])
        message = random.choice(service_config['messages'])
        
        # Generate realistic response times based on log level
        if level == 'ERROR' or level == 'FATAL':
            response_time = random.randint(1000, 5000)  # Slow for errors
        elif level == 'WARN':
            response_time = random.randint(500, 2000)   # Medium for warnings
        else:
            response_time = random.randint(50, 800)     # Fast for normal ops
        
        log_event = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'service': service,
            'level': level,
            'message': message,
            'user_id': f'user_{random.randint(10000, 99999)}',
            'request_id': f'req_{self.fake.uuid4()[:8]}',
            'response_time': response_time,
            'status_code': random.choice(self.status_codes[level]),
            'endpoint': endpoint,
            'ip_address': self.fake.ipv4(),
            'metadata': {
                'user_agent': self.fake.user_agent(),
                'session_id': self.fake.uuid4(),
                'correlation_id': f'corr_{self.fake.uuid4()[:12]}'
            }
        }
        
        return log_event

    def send_log_event(self, event):
        """Send log event to Kafka"""
        try:
            # Use service name as partition key for better distribution
            future = self.producer.send(
                self.kafka_topic,
                value=event,
                key=event['service']
            )
            
            # Don't wait for acknowledgment to maintain high throughput
            self.stats['total_sent'] += 1
            self.stats['last_sent_time'] = datetime.now()
            
        except Exception as e:
            logger.error(f"Failed to send log event: {e}")

    def log_worker(self):
        """Worker thread that generates logs at specified rate"""
        interval = 1.0 / self.log_rate  # seconds between events
        
        while self.running:
            try:
                event = self.generate_log_event()
                self.send_log_event(event)
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in log worker: {e}")
                time.sleep(1)  # Brief pause on error

    def stats_worker(self):
        """Worker thread that prints statistics"""
        while self.running:
            time.sleep(30)  # Print stats every 30 seconds
            if self.stats['start_time']:
                elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
                rate = self.stats['total_sent'] / elapsed if elapsed > 0 else 0
                logger.info(f"Stats: {self.stats['total_sent']} events sent, "
                           f"Rate: {rate:.2f} events/sec, "
                           f"Runtime: {elapsed:.0f}s")

    def start(self):
        """Start the log generation process"""
        logger.info(f"Starting log generator - Target rate: {self.log_rate} events/sec")
        logger.info(f"Kafka servers: {self.kafka_servers}")
        logger.info(f"Kafka topic: {self.kafka_topic}")
        
        self.running = True
        self.stats['start_time'] = datetime.now()
        
        # Start worker threads
        log_thread = threading.Thread(target=self.log_worker)
        stats_thread = threading.Thread(target=self.stats_worker)
        
        log_thread.daemon = True
        stats_thread.daemon = True
        
        log_thread.start()
        stats_thread.start()
        
        try:
            # Keep main thread alive
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down log generator...")
            self.stop()

    def stop(self):
        """Stop the log generation process"""
        self.running = False
        if self.producer:
            self.producer.close()
        logger.info("Log generator stopped")

def main():
    generator = LogGenerator()
    generator.start()

if __name__ == "__main__":
    main()