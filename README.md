# News Feed Service - System Architecture & Implementation

> **Project Purpose**: This repository is dedicated to learning, analyzing, and practicing large-scale distributed system architecture. It models and implements a production-grade, Facebook-like News Feed system with high availability, sub-millisecond read latency, asynchronous fan-out streaming, and eventual consistency.

---

## 🎯 Project Objectives & Focus

The primary goal of this project is to explore how to design high-throughput, read-heavy social systems by examining:
- **Write Path (Feed Publishing)**: Asynchronous fan-out pipelines, decoupling write operations using Apache Kafka, and caching strategies.
- **Read Path (Feed Retrieval & Building)**: Reverse-chronological timeline generation, low-latency reads using Redis Sorted Sets (`ZSET`), and cursor-based pagination.
- **Social Graph Engine**: 1-hop social graph traversal using **RedisGraph** via openCypher (`GRAPH.QUERY social_graph`), combined with Redis in-memory cache and PostgreSQL durability.
- **Resilience & Storage**: 64-bit distributed Snowflake IDs, cache eviction/trimming policies (`ZREMRANGEBYRANK`), and automatic cold-start / cache-miss fallback.
- **Full Implementation**: Implemented in **.NET Core 10 (C# / ASP.NET Core 10)** with an automated end-to-end test certification suite.

---

## 📂 Repository Structure

```text
.
├── README.md                          # Project overview, purpose, and local setup guide
├── docker-compose.yml                 # Multi-container orchestration stack
├── .env.example                       # Environment variables template
├── NewsFeed.slnx                      # .NET 10 solution file
├── docker/
│   ├── Dockerfile.api                 # Multi-stage .NET 10 build for Web API
│   ├── Dockerfile.worker              # Multi-stage .NET 10 build for Background Worker
│   ├── postgres/
│   │   └── init.sql                   # PostgreSQL schemas (users, friendships, posts) & seed data
│   └── redisgraph/
│       ├── init.cypher                # RedisGraph openCypher seed queries
│       └── seed.sh                    # Graph seed execution script
├── docs/
│   └── design.md                      # Complete end-to-end System Design & Architecture
├── src/
│   ├── NewsFeed.Common/               # Shared models, DTOs, Snowflake ID generator, & clients
│   │   ├── Configuration/             # Environment settings (Postgres, Redis, Kafka)
│   │   ├── Models/                    # Pydantic-like C# record DTOs with snake_case JSON serialization
│   │   ├── Services/                  # Npgsql connection pool, StackExchange.Redis, Kafka producer
│   │   └── Utils/                     # SnowflakeGenerator & RedisGraph RESP multi-bulk parser
│   ├── NewsFeed.Api/                  # ASP.NET Core 10 Minimal REST API
│   │   ├── Program.cs                 # API routing, Swagger UI, CORS, and middleware
│   │   └── Services/FeedService.cs    # Feed building, Redis hydration, & cache-miss fallback
│   └── NewsFeed.Worker/               # .NET 10 Background Service
│       └── Worker.cs                  # Kafka consumer streaming from 'post.created' topic
└── tests/
    └── test_e2e.py                    # Comprehensive 68-assertion automated test suite
```

---

## 📖 Architecture Documentation

The full technical specification and deep-dive diagrams are documented in **[docs/design.md](file:///d:/projects/system-design/news-feed/docs/design.md)**:

1. **Requirements & Capacity Estimations**: QPS, storage, and bandwidth back-of-the-envelope calculations.
2. **High-Level Architecture**: Overall component topology and service responsibilities.
3. **Data Models & Storage Schema**: Relational database schemas, RedisGraph property graph model, and Redis cache data structures.
4. **Flow 1: Feed Publishing (Write Path)**:
   - Sequence diagrams & step-by-step lifecycle.
   - Message brokers (Kafka) and worker pools.
   - The "Celebrity Problem" and Push vs. Pull trade-offs.
5. **Flow 2: Feed Retrieval (Read Path)**:
   - Cache lookup via `ZREVRANGEBYSCORE`.
   - Hydration patterns (multi-get against post and user profile caches).
   - Cursor-based pagination vs. traditional SQL offset pagination.
   - Cold starts and inactive user cache misses.
6. **API Specifications**: Request and response payloads for `POST /v1/feed` and `GET /v1/feed`.
7. **Reliability & Sharding Deep Dives**: Snowflake ID generation, database sharding keys, and fault tolerance.

---

## 🛠️ Infrastructure & Service Stack (Docker Compose)

The system runs entirely in Docker via `docker compose`:

| Service | Technology / Image | Host Port | Internal Port | Purpose | Status |
| :--- | :--- | :--- | :--- | :--- | :---: |
| **API** | `.NET 10` (`NewsFeed.Api`) | `8000` | `8000` | REST API for publishing & feed retrieval + Swagger UI | `healthy` |
| **Worker** | `.NET 10` (`NewsFeed.Worker`)| - | - | Background Kafka consumer for 1-hop RedisGraph fan-out | `running` |
| **PostgreSQL** | `postgres:16-alpine` | `5432` | `5432` | Primary DB for users, friendships, and posts (auto-seeded) | `healthy` |
| **Redis (Cache)**| `redis:7-alpine` | `6379` | `6379` | In-memory cache for feed sorted sets & post metadata | `healthy` |
| **RedisGraph** | `redislabs/redisgraph:latest`| `6380` | `6379` | In-memory Graph database using Cypher via `GRAPH.QUERY` | `healthy` |
| **Apache Kafka**| `apache/kafka:latest` | `9092` | `29092` | KRaft-mode message streaming broker for fan-out jobs | `healthy` |
| **Kafka UI** | `provectuslabs/kafka-ui` | `8085` | `8080` | Web UI for viewing topics, messages, consumer lag | `running` |
| **Redis Commander** | `rediscommander/redis-commander`| `8081` | `8081` | Web UI for inspecting both Redis Cache and RedisGraph | `healthy` |

---

## 🚀 Quick Start Guide

### 1. Start all services
```bash
docker compose up -d --build
```

### 2. Verify container health
```bash
docker compose ps
```

### 3. Access Web Consoles & APIs
- **Swagger UI (Interactive API Docs)**: [http://localhost:8000/swagger](http://localhost:8000/swagger)
- **API Health Check**: [http://localhost:8000/health](http://localhost:8000/health)
- **Redis Commander**: [http://localhost:8081](http://localhost:8081) *(connected to both `cache:6379` and `graph:6380`)*
- **Kafka UI**: [http://localhost:8085](http://localhost:8085) *(view topic `post.created` and consumer group `newsfeed-fanout-workers`)*

### 4. Query RedisGraph via Cypher
```bash
# Query direct 1-hop friends of Alice (user_id = 1001)
docker exec -it newsfeed-redis-graph redis-cli GRAPH.QUERY social_graph "MATCH (u:User {user_id: 1001})-[:FRIENDS_WITH]-(f:User) RETURN f.user_id, f.display_name"
```

### 5. Inspect PostgreSQL Database
```bash
docker exec -it newsfeed-postgres psql -U postgres -d newsfeed_db -c "SELECT post_id, author_id, content, created_at FROM posts;"
```

---

## 🧪 Automated Testing & Production Certification

The system includes a comprehensive end-to-end automated certification suite (`tests/test_e2e.py`).

### Run the tests:
```bash
python tests/test_e2e.py
```

### Verification Matrix (68 / 68 Passed):
- [x] **Health Check**: PostgreSQL, Redis Cache, RedisGraph, and Kafka producer all verified `healthy`.
- [x] **Flow 1 (Feed Publishing)**: Post creation with 64-bit Snowflake ID generation, persistence in PostgreSQL, caching in Redis (`post:{post_id}`).
- [x] **Flow 1 (Kafka Fan-out)**: Kafka event emission on topic `post.created`, consumer group consumption, 1-hop Cypher traversal in RedisGraph, pipelined updates to friends' feeds (`feed:1002`, `feed:1003`, `feed:1004`).
- [x] **Friend Isolation**: Verified stranger Eva (`1005`) received 0 post leakage.
- [x] **Flow 2 (Feed Retrieval)**: Reverse-chronological timeline retrieval with real-time profile hydration (`username`, `display_name`, `avatar_url`) backed by user cache (`user:{author_id}`).
- [x] **Cursor Pagination**: Multi-page pagination (`limit=2`, `cursor`) with **zero duplicate posts** and strict temporal ordering ($T_0 \ge T_1 \ge T_2 \dots$).
- [x] **Cold Start / Cache Miss Fallback**: Purged Redis cache; verified API seamlessly falls back to RedisGraph + PostgreSQL, repopulates Redis with TTL, and serves the feed.
- [x] **Validation & Error Handling**: Verified 400 Bad Request, 404 Not Found, 422 Unprocessable Entity, and header-based authentication via `X-User-Id`.
- [x] **Log Auditing**: Audited `newsfeed-api` and `newsfeed-worker` logs with **0 unhandled exceptions, 0 errors, and 0 connection leaks**.

---

## 💡 Key Architectural Concepts Practiced

- **Fan-out on Write**: Pre-computing feeds so read operations are $O(1)$ lookups rather than heavy, multi-table joins across hundreds of friends.
- **RedisGraph 1-Hop Traversal**: Using in-memory linear algebra (GraphBLAS matrices) with openCypher to discover direct connections in $< 1\text{ ms}$.
- **Redis Sorted Sets (`ZSET`)**: Using timestamps as scores for natural reverse-chronological ordering and bounded list maintenance (`ZREMRANGEBYRANK`).
- **Cursor-based Pagination**: Using `Exclude.Stop` score bounds to ensure scrolling consistency without duplicate or skipped posts when new content is published concurrently.
- **Hydration Pattern**: Storing only lightweight `post_id`s in the feed cache to minimize memory overhead, fetching post entities in bulk via `MGET`.
- **Cold-Start Resilience**: Gracefully recovering from cache evictions by querying RedisGraph and PostgreSQL on demand.
