# Distributed-Log-Analytics-Engine

A distributed system for real-time log processing and analytics, demonstrating concepts including message queuing, data partitioning, caching strategies, and microservices orchestration.
## Real world use cases: 
Large companies with dozens of microservices need to centralize and analyze logs from: Web applications (authentication, payments, user management), API gateways, database services, third-party integrations.
This can be helpful for incident response and debugging, performance monitoring, security, business modelling. 

## Architecture

Log Generator: Simulates web apps producing 30+ logs/second (100K+ logs/hour)

Log Parser: Kafka consumer that structures raw logs into JSON

Log Storage: Manages Redis (24hr hot data) + PostgreSQL (cold data)

Query API: REST endpoints + WebSocket streaming for real-time logs

Infrastructure: Kafka, Redis, PostgreSQL, all containerized with Docker

## Tech Stack

Backend: Python 3.11, FastAPI, Asyncio

Message Broker: Apache Kafka + Zookeeper

Databases: Redis (cache), PostgreSQL (persistent storage)

Containerization: Docker, Docker Compose

Libraries: kafka-python, redis-py, psycopg2, WebSockets

## Containerized microservices:

### Log Generator Service (log-generator): 

Simulates multiple web applications generating logs

Produces 30+ events/second to Kafka


### Log Ingestion Service (log-ingestion): 

HTTP API endpoint for receiving logs from external applications

FastAPI service that validates and forwards logs to Kafka


### Log Parser Service (log-parser)

Kafka consumer that processes raw logs

Transforms unstructured logs into structured JSON format


### Log Storage Service (log-storage)

Manages data storage lifecycle

Routes hot data (24 hours) to Redis, cold data to PostgreSQL


### Log Query Service (log-query)

REST API for searching and retrieving logs

WebSocket support for real-time log streaming
