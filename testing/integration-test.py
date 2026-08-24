#!/usr/bin/env python3
"""
Distributed Log Processing Engine - Integration Test Suite
Tests the complete system end-to-end and measures performance metrics
"""

import asyncio
import json
import time
import requests
import websockets
import psycopg2
import redis
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import threading
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SystemIntegrationTester:
    def __init__(self):
        self.config = {
            'kafka_bootstrap_servers': 'localhost:9092',
            'redis_host': 'localhost',
            'redis_port': 6379,
            'postgres_host': 'localhost',
            'postgres_port': 5432,
            'postgres_db': 'logs_db',
            'postgres_user': 'postgres',
            'postgres_password': 'password',
            'log_ingestion_url': 'http://localhost:8081',
            'log_query_url': 'http://localhost:8080',
            'log_query_ws': 'ws://localhost:8080'
        }
        self.test_results = {}
        
    def setup_connections(self):
        """Initialize connections to all services"""
        try:
            # Redis connection
            self.redis_client = redis.Redis(
                host=self.config['redis_host'],
                port=self.config['redis_port'],
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info("✅ Redis connection established")
            
            # PostgreSQL connection
            self.postgres_conn = psycopg2.connect(
                host=self.config['postgres_host'],
                port=self.config['postgres_port'],
                database=self.config['postgres_db'],
                user=self.config['postgres_user'],
                password=self.config['postgres_password']
            )
            logger.info("✅ PostgreSQL connection established")
            
            # Kafka producer for testing
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.config['kafka_bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("✅ Kafka producer initialized")
            
        except Exception as e:
            logger.error(f"❌ Connection setup failed: {e}")
            raise

    def test_service_health(self):
        """Test that all services are running and healthy"""
        logger.info("🔍 Testing service health...")
        
        services = {
            'Log Ingestion API': f"{self.config['log_ingestion_url']}/health",
            'Log Query API': f"{self.config['log_query_url']}/health"
        }
        
        for service_name, url in services.items():
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    logger.info(f"✅ {service_name} is healthy")
                else:
                    logger.error(f"❌ {service_name} returned {response.status_code}")
            except requests.RequestException as e:
                logger.error(f"❌ {service_name} is unreachable: {e}")

    def generate_test_log(self, service: str = "test-service", level: str = "INFO") -> Dict:
        """Generate a test log event"""
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": service,
            "level": level,
            "message": f"Test log message from {service}",
            "user_id": f"user_{int(time.time() * 1000) % 10000}",
            "request_id": f"req_{int(time.time() * 1000)}",
            "response_time": 150,
            "status_code": 200,
            "endpoint": "/api/test",
            "ip_address": "192.168.1.100"
        }

    def test_log_ingestion_api(self):
        """Test log ingestion through HTTP API"""
        logger.info("🔍 Testing log ingestion API...")
        
        test_log = self.generate_test_log("ingestion-test")
        
        try:
            response = requests.post(
                f"{self.config['log_ingestion_url']}/logs",
                json=test_log,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info("✅ Log ingestion API working")
                return True
            else:
                logger.error(f"❌ Log ingestion failed: {response.status_code}")
                return False
                
        except requests.RequestException as e:
            logger.error(f"❌ Log ingestion API error: {e}")
            return False

    def test_kafka_pipeline(self):
        """Test direct Kafka message flow"""
        logger.info("🔍 Testing Kafka pipeline...")
        
        test_log = self.generate_test_log("kafka-test")
        
        try:
            # Send test message to raw-logs topic
            future = self.kafka_producer.send('raw-logs', test_log)
            future.get(timeout=10)
            
            logger.info("✅ Message sent to Kafka")
            
            # Give processing time
            time.sleep(2)
            
            return True
            
        except KafkaError as e:
            logger.error(f"❌ Kafka pipeline error: {e}")
            return False

    def test_data_storage(self):
        """Test that data is stored in Redis and PostgreSQL"""
        logger.info("🔍 Testing data storage...")
        
        # Wait for processing
        time.sleep(3)
        
        # Check Redis for recent logs
        try:
            redis_keys = self.redis_client.keys("log:*")
            redis_count = len(redis_keys)
            logger.info(f"✅ Found {redis_count} logs in Redis")
            
            if redis_count > 0:
                # Check a sample log
                sample_log = self.redis_client.get(redis_keys[0])
                if sample_log:
                    log_data = json.loads(sample_log)
                    logger.info(f"✅ Sample Redis log: {log_data.get('service', 'unknown')}")
                    
        except Exception as e:
            logger.error(f"❌ Redis storage test failed: {e}")
            
        # Check PostgreSQL for logs
        try:
            cursor = self.postgres_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM logs")
            pg_count = cursor.fetchone()[0]
            logger.info(f"✅ Found {pg_count} logs in PostgreSQL")
            
            if pg_count > 0:
                cursor.execute("SELECT service, level, message FROM logs LIMIT 1")
                sample = cursor.fetchone()
                logger.info(f"✅ Sample PostgreSQL log: {sample}")
                
        except Exception as e:
            logger.error(f"❌ PostgreSQL storage test failed: {e}")

    def test_query_api(self):
        """Test the query API endpoints"""
        logger.info("🔍 Testing query API...")
        
        # Test basic search
        try:
            response = requests.get(
                f"{self.config['log_query_url']}/logs",
                params={'limit': 10},
                timeout=10
            )
            
            if response.status_code == 200:
                logs = response.json()
                logger.info(f"✅ Query API returned {len(logs)} logs")
                
                # Test filtering
                response = requests.get(
                    f"{self.config['log_query_url']}/logs",
                    params={'service': 'test-service', 'limit': 5},
                    timeout=10
                )
                
                if response.status_code == 200:
                    filtered_logs = response.json()
                    logger.info(f"✅ Filtered query returned {len(filtered_logs)} logs")
                    
        except requests.RequestException as e:
            logger.error(f"❌ Query API test failed: {e}")

    async def test_websocket_streaming(self):
        """Test real-time log streaming via WebSocket"""
        logger.info("🔍 Testing WebSocket streaming...")
        
        try:
            uri = f"{self.config['log_query_ws']}/ws/logs"
            
            async with websockets.connect(uri) as websocket:
                logger.info("✅ WebSocket connection established")
                
                # Send a test log to trigger streaming
                test_log = self.generate_test_log("websocket-test")
                requests.post(
                    f"{self.config['log_ingestion_url']}/logs",
                    json=test_log
                )
                
                # Wait for streaming data
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=10)
                    data = json.loads(message)
                    logger.info(f"✅ Received WebSocket message: {data.get('service', 'unknown')}")
                    
                except asyncio.TimeoutError:
                    logger.warning("⚠️ No WebSocket message received within timeout")
                    
        except Exception as e:
            logger.error(f"❌ WebSocket test failed: {e}")

    def performance_test_throughput(self, duration_seconds: int = 60):
        """Test system throughput"""
        logger.info(f"🔍 Running throughput test for {duration_seconds} seconds...")
        
        start_time = time.time()
        messages_sent = 0
        
        def send_logs():
            nonlocal messages_sent
            while time.time() - start_time < duration_seconds:
                test_log = self.generate_test_log(f"perf-test-{messages_sent % 5}")
                
                try:
                    requests.post(
                        f"{self.config['log_ingestion_url']}/logs",
                        json=test_log,
                        timeout=1
                    )
                    messages_sent += 1
                except:
                    pass
                    
                time.sleep(0.01)  # ~100 messages/second per thread
        
        # Start multiple threads for load testing
        threads = []
        for i in range(5):  # 5 threads = ~500 messages/second
            thread = threading.Thread(target=send_logs)
            threads.append(thread)
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        elapsed = time.time() - start_time
        throughput = messages_sent / elapsed
        
        logger.info(f"✅ Throughput test completed:")
        logger.info(f"   Messages sent: {messages_sent}")
        logger.info(f"   Duration: {elapsed:.2f} seconds")
        logger.info(f"   Throughput: {throughput:.2f} messages/second")
        logger.info(f"   Hourly rate: {throughput * 3600:.0f} messages/hour")
        
        self.test_results['throughput'] = {
            'messages_sent': messages_sent,
            'duration': elapsed,
            'throughput_per_second': throughput,
            'hourly_rate': throughput * 3600
        }

    def performance_test_query_speed(self):
        """Test query performance: Redis vs PostgreSQL"""
        logger.info("🔍 Testing query performance...")
        
        # Test Redis query speed
        redis_times = []
        for i in range(100):
            start = time.time()
            try:
                # Simulate Redis query
                keys = self.redis_client.keys("log:*")
                if keys:
                    self.redis_client.get(keys[0])
                redis_times.append(time.time() - start)
            except:
                pass
        
        # Test PostgreSQL query speed
        pg_times = []
        cursor = self.postgres_conn.cursor()
        for i in range(100):
            start = time.time()
            try:
                cursor.execute("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 1")
                cursor.fetchone()
                pg_times.append(time.time() - start)
            except:
                pass
        
        if redis_times and pg_times:
            avg_redis = sum(redis_times) / len(redis_times) * 1000  # Convert to ms
            avg_pg = sum(pg_times) / len(pg_times) * 1000
            improvement = ((avg_pg - avg_redis) / avg_pg) * 100
            
            logger.info(f"✅ Query performance results:")
            logger.info(f"   Redis avg: {avg_redis:.2f}ms")
            logger.info(f"   PostgreSQL avg: {avg_pg:.2f}ms")
            logger.info(f"   Improvement: {improvement:.1f}%")
            
            self.test_results['query_performance'] = {
                'redis_avg_ms': avg_redis,
                'postgresql_avg_ms': avg_pg,
                'improvement_percent': improvement
            }

    def generate_test_report(self):
        """Generate a comprehensive test report"""
        logger.info("📊 Generating test report...")
        
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'test_results': self.test_results,
            'summary': {
                'throughput_target_met': self.test_results.get('throughput', {}).get('hourly_rate', 0) > 100000,
                'query_improvement_target_met': self.test_results.get('query_performance', {}).get('improvement_percent', 0) > 80
            }
        }
        
        # Save report
        with open('test_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info("✅ Test report saved to test_report.json")
        return report

    def cleanup(self):
        """Clean up connections"""
        if hasattr(self, 'kafka_producer'):
            self.kafka_producer.close()
        if hasattr(self, 'postgres_conn'):
            self.postgres_conn.close()

async def main():
    """Run the complete integration test suite"""
    tester = SystemIntegrationTester()
    
    try:
        # Setup
        logger.info("🚀 Starting integration test suite...")
        tester.setup_connections()
        
        # Basic functionality tests
        tester.test_service_health()
        tester.test_log_ingestion_api()
        tester.test_kafka_pipeline()
        tester.test_data_storage()
        tester.test_query_api()
        await tester.test_websocket_streaming()
        
        # Performance tests
        tester.performance_test_throughput(60)  # 1 minute test
        tester.performance_test_query_speed()
        
        # Generate report
        report = tester.generate_test_report()
        
        logger.info("🎉 Integration test suite completed!")
        logger.info(f"📊 Results summary:")
        logger.info(f"   Throughput target (100K/hour): {'✅' if report['summary']['throughput_target_met'] else '❌'}")
        logger.info(f"   Query improvement target (80%): {'✅' if report['summary']['query_improvement_target_met'] else '❌'}")
        
    except Exception as e:
        logger.error(f"❌ Test suite failed: {e}")
        raise
    finally:
        tester.cleanup()

if __name__ == "__main__":
    asyncio.run(main())