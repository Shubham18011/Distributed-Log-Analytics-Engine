#!/usr/bin/env python3
"""
Load Testing Script for Distributed Log Processing Engine
Generates high-volume log data to test system performance
"""

import asyncio
import aiohttp
import json
import time
import random
from datetime import datetime, timedelta
from typing import List, Dict
import logging
from concurrent.futures import ThreadPoolExecutor
import threading

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoadTester:
    def __init__(self):
        self.config = {
            'log_ingestion_url': 'http://localhost:8081',
            'log_query_url': 'http://localhost:8080',
            'target_rate': 100000,  # 100K events/hour
            'test_duration': 3600,   
            'concurrent_clients': 20
        }
        
        self.services = [
            'auth-service', 'user-service', 'payment-service', 
            'order-service', 'inventory-service', 'notification-service',
            'analytics-service', 'recommendation-service'
        ]
        
        self.log_levels = ['DEBUG', 'INFO', 'WARN', 'ERROR']
        self.endpoints = [
            '/api/auth/login', '/api/auth/logout', '/api/users/profile',
            '/api/orders/create', '/api/orders/status', '/api/payment/process',
            '/api/inventory/check', '/api/notifications/send'
        ]
        
        self.metrics = {
            'logs_sent': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'response_times': [],
            'start_time': None,
            'end_time': None
        }
        
        self.lock = threading.Lock()

    def generate_realistic_log(self) -> Dict:
        """Generate a realistic log entry"""
        timestamp = datetime.utcnow()
        service = random.choice(self.services)
        level = random.choices(
            self.log_levels, 
            weights=[10, 70, 15, 5]  # More INFO, fewer ERROR
        )[0]
        endpoint = random.choice(self.endpoints)
        
        # Generate realistic response times based on endpoint
        if 'auth' in endpoint:
            response_time = random.randint(50, 200)
        elif 'payment' in endpoint:
            response_time = random.randint(200, 800)
        else:
            response_time = random.randint(30, 300)
            
        # Generate realistic status codes
        if level == 'ERROR':
            status_code = random.choice([400, 401, 403, 404, 500, 502, 503])
        else:
            status_code = random.choices([200, 201, 204], weights=[80, 15, 5])[0]
        
        return {
            'timestamp': timestamp.isoformat() + 'Z',
            'service': service,
            'level': level,
            'message': self.generate_log_message(service, level, endpoint, status_code),
            'user_id': f'user_{random.randint(1000, 9999)}',
            'request_id': f'req_{int(time.time() * 1000000)}_{random.randint(100, 999)}',
            'response_time': response_time,
            'status_code': status_code,
            'endpoint': endpoint,
            'ip_address': f'192.168.{random.randint(1, 255)}.{random.randint(1, 255)}',
            'user_agent': random.choice([
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
            ])
        }

    def generate_log_message(self, service: str, level: str, endpoint: str, status_code: int) -> str:
        """Generate realistic log messages"""
        messages = {
            'INFO': [
                f'Request processed successfully for {endpoint}',
                f'User authentication successful',
                f'Database query completed',
                f'Cache hit for key',
                f'API response sent'
            ],
            'DEBUG': [
                f'Processing request for {endpoint}',
                f'Database connection established',
                f'Cache lookup initiated',
                f'Validation passed for request',
                f'Serializing response data'
            ],
            'WARN': [
                f'Slow response time detected for {endpoint}',
                f'Cache miss for frequently accessed data',
                f'Rate limiting applied to user',
                f'Deprecated API endpoint accessed',
                f'High memory usage detected'
            ],
            'ERROR': [
                f'Request failed for {endpoint} with status {status_code}',
                f'Database connection timeout',
                f'Authentication failed for user',
                f'Payment processing failed',
                f'Internal server error occurred'
            ]
        }
        
        return random.choice(messages.get(level, ['Unknown log message']))

    async def send_log_async(self, session: aiohttp.ClientSession, log_data: Dict):
        """Send a single log asynchronously"""
        try:
            start_time = time.time()
            async with session.post(
                f"{self.config['log_ingestion_url']}/logs",
                json=log_data,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                response_time = time.time() - start_time
                
                with self.lock:
                    self.metrics['logs_sent'] += 1
                    self.metrics['response_times'].append(response_time)
                    
                    if response.status == 200:
                        self.metrics['successful_requests'] += 1
                    else:
                        self.metrics['failed_requests'] += 1
                        
        except Exception as e:
            with self.lock:
                self.metrics['failed_requests'] += 1
                logger.debug(f"Request failed: {e}")

    async def run_load_test_async(self):
        """Run asynchronous load test"""
        logger.info(f"🚀 Starting async load test...")
        logger.info(f"   Target rate: {self.config['target_rate']} events/hour")
        logger.info(f"   Duration: {self.config['test_duration']} seconds")
        logger.info(f"   Concurrent clients: {self.config['concurrent_clients']}")
        
        # Calculate per-second rate
        target_per_second = self.config['target_rate'] / 3600
        interval = 1.0 / target_per_second
        
        self.metrics['start_time'] = time.time()
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.config['concurrent_clients'])
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            
            # Generate load for specified duration
            end_time = time.time() + self.config['test_duration']
            
            while time.time() < end_time:
                # Generate batch of logs
                batch_size = min(10, int(target_per_second))
                
                for _ in range(batch_size):
                    log_data = self.generate_realistic_log()
                    
                    # Create task with semaphore
                    task = asyncio.create_task(
                        self.send_with_semaphore(semaphore, session, log_data)
                    )
                    tasks.append(task)
                
                # Wait for interval
                await asyncio.sleep(interval * batch_size)
            
            # Wait for all tasks to complete
            logger.info("⏳ Waiting for all requests to complete...")
            await asyncio.gather(*tasks, return_exceptions=True)
        
        self.metrics['end_time'] = time.time()
        logger.info("✅ Async load test completed")

    async def send_with_semaphore(self, semaphore: asyncio.Semaphore, session: aiohttp.ClientSession, log_data: Dict):
        """Send log with semaphore to limit concurrency"""
        async with semaphore:
            await self.send_log_async(session, log_data)

    def run_burst_test(self, burst_size: int = 1000, burst_duration: int = 10):
        """Run a burst test to check system resilience"""
        logger.info(f"💥 Running burst test: {burst_size} logs in {burst_duration} seconds")
        
        import requests
        
        start_time = time.time()
        sent_count = 0
        
        for i in range(burst_size):
            if time.time() - start_time > burst_duration:
                break
                
            log_data = self.generate_realistic_log()
            
            try:
                response = requests.post(
                    f"{self.config['log_ingestion_url']}/logs",
                    json=log_data,
                    timeout=1
                )
                
                if response.status_code == 200:
                    sent_count += 1
                    
            except Exception as e:
                logger.debug(f"Burst request failed: {e}")
        
        elapsed = time.time() - start_time
        rate = sent_count / elapsed
        
        logger.info(f"✅ Burst test completed:")
        logger.info(f"   Sent: {sent_count}/{burst_size} logs")
        logger.info(f"   Duration: {elapsed:.2f} seconds")
        logger.info(f"   Rate: {rate:.2f} logs/second")
        
        return {
            'sent_count': sent_count,
            'target_count': burst_size,
            'duration': elapsed,
            'rate': rate
        }

    async def test_query_performance_under_load(self):
        """Test query API performance while system is under load"""
        logger.info("🔍 Testing query performance under load...")
        
        import aiohttp
        
        query_times = []
        
        async with aiohttp.ClientSession() as session:
            # Run queries while system is processing logs
            for i in range(50):
                start_time = time.time()
                
                try:
                    async with session.get(
                        f"{self.config['log_query_url']}/logs",
                        params={'limit': 100},
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        await response.json()
                        query_time = time.time() - start_time
                        query_times.append(query_time)
                        
                except Exception as e:
                    logger.debug(f"Query failed: {e}")
                
                await asyncio.sleep(0.1)  # Small delay between queries
        
        if query_times:
            avg_time = sum(query_times) / len(query_times)
            max_time = max(query_times)
            min_time = min(query_times)
            
            logger.info(f"✅ Query performance under load:")
            logger.info(f"   Average: {avg_time:.3f}s")
            logger.info(f"   Min: {min_time:.3f}s")
            logger.info(f"   Max: {max_time:.3f}s")
            logger.info(f"   Queries completed: {len(query_times)}/50")
            
            return {
                'avg_time': avg_time,
                'min_time': min_time,
                'max_time': max_time,
                'completed_queries': len(query_times)
            }

    def calculate_statistics(self):
        """Calculate comprehensive statistics"""
        duration = self.metrics['end_time'] - self.metrics['start_time']
        
        stats = {
            'total_logs_sent': self.metrics['logs_sent'],
            'successful_requests': self.metrics['successful_requests'],
            'failed_requests': self.metrics['failed_requests'],
            'success_rate': (self.metrics['successful_requests'] / max(self.metrics['logs_sent'], 1)) * 100,
            'duration_seconds': duration,
            'average_rate_per_second': self.metrics['logs_sent'] / duration,
            'projected_hourly_rate': (self.metrics['logs_sent'] / duration) * 3600,
            'target_met': ((self.metrics['logs_sent'] / duration) * 3600) >= self.config['target_rate']
        }
        
        if self.metrics['response_times']:
            stats.update({
                'avg_response_time': sum(self.metrics['response_times']) / len(self.metrics['response_times']),
                'min_response_time': min(self.metrics['response_times']),
                'max_response_time': max(self.metrics['response_times']),
                'p95_response_time': sorted(self.metrics['response_times'])[int(len(self.metrics['response_times']) * 0.95)]
            })
        
        return stats

    def generate_load_test_report(self, stats: Dict, burst_results: Dict = None, query_results: Dict = None):
        """Generate comprehensive load test report"""
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'test_configuration': self.config,
            'performance_stats': stats,
            'burst_test_results': burst_results,
            'query_under_load_results': query_results,
            'targets_met': {
                'throughput_target': stats['target_met'],
                'success_rate_target': stats['success_rate'] > 95,
                'response_time_target': stats.get('avg_response_time', 1) < 0.1
            }
        }
        
        # Save report
        with open('load_test_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        # Print summary
        logger.info("📊 Load Test Results Summary:")
        logger.info(f"   Total logs sent: {stats['total_logs_sent']:,}")
        logger.info(f"   Success rate: {stats['success_rate']:.1f}%")
        logger.info(f"   Average rate: {stats['average_rate_per_second']:.1f} logs/second")
        logger.info(f"   Projected hourly: {stats['projected_hourly_rate']:,.0f} logs/hour")
        logger.info(f"   Target met (100K/hour): {'✅' if stats['target_met'] else '❌'}")
        
        if 'avg_response_time' in stats:
            logger.info(f"   Avg response time: {stats['avg_response_time']:.3f}s")
            logger.info(f"   95th percentile: {stats['p95_response_time']:.3f}s")
        
        logger.info("✅ Load test report saved to load_test_report.json")
        return report

async def main():
    """Run comprehensive load testing suite"""
    tester = LoadTester()
    
    logger.info("🚀 Starting comprehensive load testing...")
    
    try:
        # Run main load test
        await tester.run_load_test_async()
        
        # Calculate statistics
        stats = tester.calculate_statistics()
        
        # Run burst test
        burst_results = tester.run_burst_test()
        
        # Test query performance under load
        query_results = await tester.test_query_performance_under_load()
        
        # Generate comprehensive report
        report = tester.generate_load_test_report(stats, burst_results, query_results)
        
        logger.info("🎉 Load testing completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Load testing failed: {e}")
        raise

if __name__ == "__main__":