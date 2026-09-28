import asyncio
import json
import os
import sys
import time
import urllib.request
import urllib.error
import asyncpg
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "newsfeed_db")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

REDIS_GRAPH_HOST = os.getenv("REDIS_GRAPH_HOST", "localhost")
REDIS_GRAPH_PORT = int(os.getenv("REDIS_GRAPH_PORT", "6379" if os.getenv("POSTGRES_HOST") == "postgres" else "6380"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092" if os.getenv("POSTGRES_HOST") == "postgres" else "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "post.created")


def http_request(method: str, path: str, data: dict = None, headers: dict = None, raw_data: bytes = None):
    url = f"{API_BASE_URL}{path}"
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    
    body = None
    if raw_data is not None:
        body = raw_data
    elif data is not None:
        body = json.dumps(data).encode("utf-8")
        
    req = urllib.request.Request(url, data=body, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            status_code = response.getcode()
            resp_body = response.read().decode("utf-8")
            return status_code, json.loads(resp_body) if resp_body else {}
    except urllib.error.HTTPError as e:
        resp_body = e.read().decode("utf-8")
        try:
            parsed = json.loads(resp_body)
        except Exception:
            parsed = {"raw": resp_body}
        return e.code, parsed
    except Exception as e:
        return 0, {"error": str(e)}


async def run_tests():
    print("=" * 80)
    print("NEWS FEED SERVICE — COMPREHENSIVE PRODUCTION TEST CERTIFICATION SUITE")
    print(f"API Target: {API_BASE_URL}")
    print(f"PostgreSQL Target: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print(f"Redis Cache Target: {REDIS_HOST}:{REDIS_PORT}")
    print(f"Kafka Target: {KAFKA_BOOTSTRAP_SERVERS} (Topic: {KAFKA_TOPIC})")
    print("=" * 80)

    # 0. Connect to DB, Redis, Kafka
    print("\n[Setup] Establishing direct connections to PostgreSQL, Redis, and Kafka...")
    pg_conn = await asyncpg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
    )
    redis_client = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    await redis_client.ping()

    kafka_consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"test-verifier-{int(time.time()*1000)}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await kafka_consumer.start()
    print(" Connected to PostgreSQL, Redis, and Kafka successfully.")

    passed_count = 0
    failed_count = 0

    def assert_true(condition, message):
        nonlocal passed_count, failed_count
        if condition:
            print(f"  [PASS] {message}")
            passed_count += 1
        else:
            print(f"  [FAIL] {message}")
            failed_count += 1
            raise AssertionError(message)

    try:
        # TEST 1: Health Check Endpoint
        print("\n=== SUITE 1: Health Check Endpoint (GET /health) ===")
        status, body = http_request("GET", "/health")
        assert_true(status == 200, f"GET /health returns HTTP 200 (Got {status})")
        assert_true(body.get("status") == "ok", f"Health status is 'ok' (Got {body.get('status')})")
        services = body.get("services", {})
        assert_true(services.get("postgres") == "healthy", "PostgreSQL healthcheck reports 'healthy'")
        assert_true(services.get("redis") == "healthy", "Redis cache healthcheck reports 'healthy'")
        assert_true(services.get("redis_graph") == "healthy", "RedisGraph healthcheck reports 'healthy'")
        assert_true(services.get("kafka") == "healthy", "Kafka producer healthcheck reports 'healthy'")

        # TEST 2: Flow 1 - Feed Publishing & 1-Hop Fan-out
        print("\n=== SUITE 2: Flow 1 - Feed Publishing & 1-Hop Fan-out ===")
        alice_id = 1001
        bob_id = 1002
        charlie_id = 1003
        david_id = 1004
        eva_id = 1005

        test_msg_content = f"Post created at {time.time()} by Alice Johnson"
        post_payload = {
            "author_id": alice_id,
            "content": test_msg_content,
            "media_urls": ["https://images.unsplash.com/photo-test-feed.jpg"]
        }
        status, create_resp = http_request("POST", "/v1/feed", data=post_payload)
        assert_true(status == 201, f"POST /v1/feed returns 201 Created (Got {status})")
        post_id = create_resp.get("post_id")
        created_at = create_resp.get("created_at")
        assert_true(isinstance(post_id, int) and post_id > 0, f"Post ID is a valid Snowflake integer: {post_id}")
        assert_true(create_resp.get("author_id") == alice_id, f"author_id matches {alice_id}")
        assert_true(create_resp.get("content") == test_msg_content, "Post content strictly matches")
        assert_true(create_resp.get("media_urls") == ["https://images.unsplash.com/photo-test-feed.jpg"], "media_urls match")
        assert_true(isinstance(created_at, int) and created_at > 1700000000000, f"Timestamp in valid epoch ms: {created_at}")

        # 2a. Verify Kafka message published to 'post.created' topic
        print("  Checking Kafka topic 'post.created' for event...")
        kafka_matched = False
        kafka_start = time.time()
        while time.time() - kafka_start < 8:
            records = await kafka_consumer.getmany(timeout_ms=1000, max_records=20)
            for tp, msgs in records.items():
                for msg in msgs:
                    val = msg.value
                    if val.get("post_id") == post_id:
                        kafka_matched = True
                        assert_true(val.get("author_id") == alice_id, f"Kafka event author_id == {alice_id}")
                        assert_true(val.get("created_at") == created_at, f"Kafka event created_at matches {created_at}")
                        break
                if kafka_matched:
                    break
            if kafka_matched:
                break
        assert_true(kafka_matched, f"Kafka topic '{KAFKA_TOPIC}' received event for post {post_id}")

        # 2b. Verify PostgreSQL persistence
        print("  Checking PostgreSQL posts table...")
        pg_row = await pg_conn.fetchrow("SELECT * FROM posts WHERE post_id = $1", post_id)
        assert_true(pg_row is not None, f"Post {post_id} successfully persisted in PostgreSQL")
        assert_true(pg_row["author_id"] == alice_id, "PostgreSQL author_id is Alice (1001)")
        assert_true(pg_row["content"] == test_msg_content, "PostgreSQL content matches exactly")

        # 2c. Verify Redis post detail hash/string cache
        print("  Checking Redis post:{post_id} cache...")
        cached_post_str = await redis_client.get(f"post:{post_id}")
        assert_true(cached_post_str is not None, f"Redis key post:{post_id} exists")
        cached_post_obj = json.loads(cached_post_str)
        assert_true(cached_post_obj.get("post_id") == post_id, "Cached post_id matches")
        assert_true(cached_post_obj.get("author_id") == alice_id, "Cached author_id matches")
        assert_true(cached_post_obj.get("content") == test_msg_content, "Cached content matches")

        # 2d. Verify 1-Hop Fan-out to Bob, Charlie, David
        print("  Checking Fan-out Worker delivery to 1-hop friends: Bob, Charlie, David...")
        fanout_friends = [bob_id, charlie_id, david_id]
        fanout_complete = False
        for _ in range(20):
            all_received = True
            for fid in fanout_friends:
                score = await redis_client.zscore(f"feed:{fid}", str(post_id))
                if score is None:
                    all_received = False
                    break
            if all_received:
                fanout_complete = True
                break
            await asyncio.sleep(0.5)

        for fid in fanout_friends:
            score = await redis_client.zscore(f"feed:{fid}", str(post_id))
            assert_true(score is not None, f"Friend {fid} received post {post_id} in feed:{fid} (Score: {score})")
            assert_true(int(score) == created_at, f"Feed score for friend {fid} matches created_at timestamp")

        # 2e. Verify Stranger Eva (1005) did NOT receive the post
        print("  Checking Stranger Isolation: Eva (1005)...")
        eva_score = await redis_client.zscore(f"feed:{eva_id}", str(post_id))
        assert_true(eva_score is None, f"Eva (1005 - stranger) correctly did NOT receive post {post_id} in feed:1005")

        # TEST 3: Flow 2 - Feed Retrieval & Author Profile Hydration
        print("\n=== SUITE 3: Flow 2 - Feed Retrieval & Profile Hydration ===")
        status, feed_resp = http_request("GET", f"/v1/feed?user_id={bob_id}&limit=10")
        assert_true(status == 200, f"GET /v1/feed?user_id={bob_id} returns 200 OK")
        feed_items = feed_resp.get("data", [])
        assert_true(len(feed_items) >= 1, f"Bob's feed returned {len(feed_items)} posts")
        
        top_item = feed_items[0]
        assert_true(top_item.get("post_id") == post_id, f"Alice's latest post {post_id} is at the top of Bob's feed")
        assert_true(top_item.get("content") == test_msg_content, "Top post content matches")
        
        author_profile = top_item.get("author", {})
        assert_true(author_profile.get("user_id") == 1001, "Hydrated author profile user_id is 1001")
        assert_true(author_profile.get("username") == "alice", "Hydrated author profile username is 'alice'")
        assert_true(author_profile.get("display_name") == "Alice Johnson", "Hydrated author profile display_name is 'Alice Johnson'")
        assert_true(author_profile.get("avatar_url") is not None, f"Hydrated author avatar_url present: {author_profile.get('avatar_url')}")

        # Also verify user profile caching in Redis (user:1001)
        cached_author_str = await redis_client.get(f"user:{alice_id}")
        assert_true(cached_author_str is not None, f"Author profile cached in Redis at user:{alice_id}")

        # TEST 4: Cursor-based Pagination & Ordering
        print("\n=== SUITE 4: Multi-item Reverse-Chronological Ordering & Pagination ===")
        # Seed 3 additional posts from Alice to ensure a rich pagination test
        print("  Publishing additional posts to populate pagination test dataset...")
        pagination_post_ids = []
        for i in range(1, 4):
            time.sleep(0.05) # ensure distinct timestamp
            st, res = http_request("POST", "/v1/feed", data={
                "author_id": alice_id,
                "content": f"Pagination Batch Post #{i} - {time.time()}"
            })
            assert_true(st == 201, f"Published batch post #{i}")
            pagination_post_ids.append(res["post_id"])

        # Wait for worker fanout of batch posts to Bob
        for pid in pagination_post_ids:
            for _ in range(15):
                if await redis_client.zscore(f"feed:{bob_id}", str(pid)) is not None:
                    break
                await asyncio.sleep(0.3)

        # Page 1: limit = 2
        print("  Fetching Page 1 (limit=2)...")
        status, page1 = http_request("GET", f"/v1/feed?user_id={bob_id}&limit=2")
        assert_true(status == 200, "Page 1 returned 200 OK")
        page1_data = page1.get("data", [])
        page1_pagination = page1.get("pagination", {})
        assert_true(len(page1_data) == 2, f"Page 1 contains exactly 2 items (Got {len(page1_data)})")
        assert_true(page1_pagination.get("has_more") is True, "Page 1 has_more is True")
        cursor_p1 = page1_pagination.get("next_cursor")
        assert_true(cursor_p1 is not None and cursor_p1 != "", f"Page 1 returned next_cursor: {cursor_p1}")

        # Verify page 1 reverse chronological order
        assert_true(page1_data[0]["created_at"] >= page1_data[1]["created_at"], "Page 1 posts in reverse-chronological order")

        # Page 2: using cursor
        print(f"  Fetching Page 2 (limit=2, cursor={cursor_p1})...")
        status, page2 = http_request("GET", f"/v1/feed?user_id={bob_id}&limit=2&cursor={cursor_p1}")
        assert_true(status == 200, "Page 2 returned 200 OK")
        page2_data = page2.get("data", [])
        assert_true(len(page2_data) > 0, f"Page 2 contains items (Got {len(page2_data)})")

        # Verify no overlap between Page 1 and Page 2
        p1_ids = {p["post_id"] for p in page1_data}
        p2_ids = {p["post_id"] for p in page2_data}
        duplicate_ids = p1_ids.intersection(p2_ids)
        assert_true(len(duplicate_ids) == 0, f"No duplicate post IDs between Page 1 and Page 2 (Duplicates: {duplicate_ids})")

        # Verify cross-page strict chronological order
        min_p1_timestamp = min(p["created_at"] for p in page1_data)
        max_p2_timestamp = max(p["created_at"] for p in page2_data)
        assert_true(min_p1_timestamp >= max_p2_timestamp, f"Page 2 timestamps ({max_p2_timestamp}) strictly <= Page 1 timestamps ({min_p1_timestamp})")

        # TEST 5: Cold Start / Cache Miss Fallback (RedisGraph + PostgreSQL)
        print("\n=== SUITE 5: Cold Start / Cache Miss Fallback ===")
        # Check Bob's feed cache deletion
        print(f"  Flushing Bob's feed cache: DEL feed:{bob_id}...")
        await redis_client.delete(f"feed:{bob_id}")
        exists_before = await redis_client.exists(f"feed:{bob_id}")
        assert_true(exists_before == 0, f"feed:{bob_id} is completely purged from Redis")

        # Cold start request to GET /v1/feed
        print(f"  Triggering Cold Start feed rebuild via GET /v1/feed?user_id={bob_id}...")
        status, cold_resp = http_request("GET", f"/v1/feed?user_id={bob_id}&limit=20")
        assert_true(status == 200, f"Cold start request returned 200 OK (Got {status})")
        cold_data = cold_resp.get("data", [])
        assert_true(len(cold_data) > 0, f"Cold start returned {len(cold_data)} hydrated feed items")

        # Verify Redis feed cache is back in place
        cache_card = await redis_client.zcard(f"feed:{bob_id}")
        assert_true(cache_card > 0, f"Redis feed:{bob_id} cache successfully reconstructed with {cache_card} items")
        ttl = await redis_client.ttl(f"feed:{bob_id}")
        assert_true(ttl > 0, f"feed:{bob_id} has valid TTL set ({ttl}s)")

        # Verify that Alice's posts are present in reconstructed feed
        reconstructed_ids = {p["post_id"] for p in cold_data}
        assert_true(post_id in reconstructed_ids, f"Original Alice post {post_id} successfully restored from PostgreSQL")

        # TEST 6: Header-Based User ID Resolution (X-User-Id)
        print("\n=== SUITE 6: Header-Based User ID Resolution (X-User-Id) ===")
        # POST with X-User-Id header and no author_id in body
        status, header_post_resp = http_request(
            "POST",
            "/v1/feed",
            data={"content": "Post published using X-User-Id header auth"},
            headers={"X-User-Id": str(alice_id)}
        )
        assert_true(status == 201, f"POST /v1/feed with X-User-Id returns 201 Created (Got {status})")
        assert_true(header_post_resp.get("author_id") == alice_id, "Author resolved from X-User-Id header")

        # GET /v1/feed with X-User-Id header and no user_id query param
        status, header_feed_resp = http_request(
            "GET",
            "/v1/feed?limit=5",
            headers={"X-User-Id": str(bob_id)}
        )
        assert_true(status == 200, f"GET /v1/feed with X-User-Id returns 200 OK (Got {status})")
        assert_true(len(header_feed_resp.get("data", [])) > 0, "Feed items returned using header auth")

        # TEST 7: Input Validation & Edge Case Handling
        print("\n=== SUITE 7: Input Validation & Error Handling ===")
        # 7a. Missing author_id in both body and header
        st, res = http_request("POST", "/v1/feed", data={"content": "No author given"})
        assert_true(st in (400, 422), f"POST without author_id returns 400 or 422 Bad Request (Got {st})")

        # 7b. Invalid format for X-User-Id header
        st, res = http_request("POST", "/v1/feed", data={"content": "Bad header"}, headers={"X-User-Id": "not-an-int"})
        assert_true(st in (400, 422), f"POST with non-numeric X-User-Id returns 400 or 422 (Got {st})")

        # 7c. Empty content string
        st, res = http_request("POST", "/v1/feed", data={"author_id": alice_id, "content": ""})
        assert_true(st in (400, 422), f"POST with empty content returns 400 or 422 (Got {st})")

        # 7d. Malformed JSON payload
        st, res = http_request("POST", "/v1/feed", raw_data=b"{invalid: json syntax,}")
        assert_true(st in (400, 422), f"POST with malformed JSON returns 400 or 422 (Got {st})")

        # 7e. Non-existent author_id
        st, res = http_request("POST", "/v1/feed", data={"author_id": 987654321, "content": "Ghost author"})
        assert_true(st == 404, f"POST with non-existent author returns 404 Not Found (Got {st})")

        # 7f. Missing user_id in GET /v1/feed
        st, res = http_request("GET", "/v1/feed")
        assert_true(st in (400, 422), f"GET without user_id returns 400 or 422 Bad Request (Got {st})")

        # 7g. Invalid pagination limit > 100
        st, res = http_request("GET", f"/v1/feed?user_id={bob_id}&limit=101")
        assert_true(st in (400, 422), f"GET with limit > 100 returns 400 or 422 (Got {st})")

        # 7h. Invalid pagination limit <= 0
        st, res = http_request("GET", f"/v1/feed?user_id={bob_id}&limit=0")
        assert_true(st in (400, 422), f"GET with limit <= 0 returns 400 or 422 (Got {st})")

        print("\n" + "=" * 80)
        print(f"FINAL RESULT: ALL {passed_count} ASSERTIONS PASSED, {failed_count} FAILED")
        print("=" * 80)
        return True

    except Exception as e:
        print(f"\n[EXCEPTION CAUGHT] {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        await kafka_consumer.stop()
        await pg_conn.close()
        await redis_client.aclose()


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
