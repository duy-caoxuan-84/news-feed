using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using NewsFeed.Common.Configuration;
using NewsFeed.Common.Models;
using NewsFeed.Common.Services;
using NewsFeed.Common.Utils;
using Npgsql;
using StackExchange.Redis;

namespace NewsFeed.Api.Services;

public class FeedService
{
    private readonly SnowflakeGenerator _snowflake = new(NewsFeedConfig.WorkerId);
    private readonly KafkaProducerService _kafkaProducer;

    public FeedService(KafkaProducerService kafkaProducer)
    {
        _kafkaProducer = kafkaProducer;
    }

    public async Task<PostCreateResponse?> CreatePostAsync(long authorId, string content, List<string>? mediaUrls)
    {
        var dataSource = DbProvider.GetDataSource();
        var cacheDb = RedisProvider.GetCacheDb();

        await using var conn = await dataSource.OpenConnectionAsync();

        // 1. Verify author exists
        await using (var checkCmd = new NpgsqlCommand("SELECT 1 FROM users WHERE user_id = $1", conn))
        {
            checkCmd.Parameters.AddWithValue(authorId);
            var exists = await checkCmd.ExecuteScalarAsync();
            if (exists == null)
            {
                return null; // Author not found
            }
        }

        // 2. Generate Snowflake post_id and timestamps
        long postId = _snowflake.Generate();
        var now = DateTimeOffset.UtcNow;
        long createdAtMs = now.ToUnixTimeMilliseconds();
        var cleanMediaUrls = mediaUrls ?? new List<string>();
        string mediaUrlsJson = JsonSerializer.Serialize(cleanMediaUrls);

        // 3. Persist post to PostgreSQL
        await using (var insertCmd = new NpgsqlCommand(
            "INSERT INTO posts (post_id, author_id, content, media_urls, like_count, comment_count, created_at) " +
            "VALUES ($1, $2, $3, $4::jsonb, 0, 0, $5)", conn))
        {
            insertCmd.Parameters.AddWithValue(postId);
            insertCmd.Parameters.AddWithValue(authorId);
            insertCmd.Parameters.AddWithValue(content);
            insertCmd.Parameters.AddWithValue(mediaUrlsJson);
            insertCmd.Parameters.AddWithValue(now.UtcDateTime);
            await insertCmd.ExecuteNonQueryAsync();
        }

        var postObj = new CachedPost
        {
            PostId = postId,
            AuthorId = authorId,
            Content = content,
            MediaUrls = cleanMediaUrls,
            LikeCount = 0,
            CommentCount = 0,
            CreatedAt = createdAtMs
        };

        // 4. Cache post in Redis
        try
        {
            await cacheDb.StringSetAsync(
                $"post:{postId}",
                JsonSerializer.Serialize(postObj),
                NewsFeedConfig.RedisPostTtl
            );
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FeedService] Warning: Failed to cache post {postId} in Redis: {ex.Message}");
        }

        // 5. Publish event to Kafka
        try
        {
            await _kafkaProducer.PublishPostCreatedAsync(authorId, postId, createdAtMs);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FeedService] Error: Failed to publish Kafka event for post {postId}: {ex.Message}");
        }

        return new PostCreateResponse
        {
            PostId = postId,
            AuthorId = authorId,
            Content = content,
            MediaUrls = cleanMediaUrls,
            CreatedAt = createdAtMs
        };
    }

    public async Task<FeedResponse> GetFeedAsync(long userId, int limit = 20, string? cursor = null)
    {
        var cacheDb = RedisProvider.GetCacheDb();
        string feedKey = $"feed:{userId}";

        // 1. Check if user's feed cache is populated
        long card = await cacheDb.SortedSetLengthAsync(feedKey);
        if (card == 0)
        {
            await RebuildFeedCacheAsync(userId);
        }

        // 2. Query Redis Sorted Set using ZREVRANGEBYSCORE
        double maxScore = double.PositiveInfinity;
        var exclude = Exclude.None;
        if (!string.IsNullOrWhiteSpace(cursor) && double.TryParse(cursor, out double parsedCursor))
        {
            maxScore = parsedCursor;
            exclude = Exclude.Stop; // Exclude the upper bound (cursor score) so the cursor post is not re-fetched
        }
        double minScore = double.NegativeInfinity;

        // Fetch limit + 1 items to determine if next page exists
        var entries = await cacheDb.SortedSetRangeByScoreWithScoresAsync(
            feedKey,
            minScore,
            maxScore,
            exclude,
            Order.Descending,
            0,
            limit + 1
        );

        bool hasMore = entries.Length > limit;
        var pageEntries = hasMore ? entries.Take(limit).ToArray() : entries;
        string? nextCursor = null;
        if (hasMore && pageEntries.Length > 0)
        {
            nextCursor = ((long)pageEntries[^1].Score).ToString();
        }

        var postIds = new List<long>();
        foreach (var entry in pageEntries)
        {
            if (entry.Element.HasValue && long.TryParse(entry.Element.ToString(), out var pid))
            {
                postIds.Add(pid);
            }
        }

        if (postIds.Count == 0)
        {
            return new FeedResponse
            {
                Data = new List<FeedPostItem>(),
                Pagination = new PaginationInfo
                {
                    NextCursor = null,
                    HasMore = false
                }
            };
        }

        // 3. Hydrate posts
        var postsById = await HydratePostsAsync(postIds);

        // 4. Hydrate authors
        var authorIds = postsById.Values.Select(p => p.AuthorId).Distinct().ToList();
        var authorsById = await HydrateAuthorsAsync(authorIds);

        // 5. Assemble feed in order of postIds
        var feedItems = new List<FeedPostItem>();
        foreach (var pid in postIds)
        {
            if (!postsById.TryGetValue(pid, out var post)) continue;

            authorsById.TryGetValue(post.AuthorId, out var author);
            author ??= new AuthorProfile
            {
                UserId = post.AuthorId,
                Username = $"user_{post.AuthorId}",
                DisplayName = $"User {post.AuthorId}",
                AvatarUrl = null
            };

            feedItems.Add(new FeedPostItem
            {
                PostId = post.PostId,
                Author = author,
                Content = post.Content,
                MediaUrls = post.MediaUrls ?? new List<string>(),
                LikeCount = post.LikeCount,
                CommentCount = post.CommentCount,
                CreatedAt = post.CreatedAt
            });
        }

        return new FeedResponse
        {
            Data = feedItems,
            Pagination = new PaginationInfo
            {
                NextCursor = nextCursor,
                HasMore = hasMore
            }
        };
    }

    private async Task RebuildFeedCacheAsync(long userId)
    {
        var friendIds = new List<long>();

        // Step A: Query RedisGraph for 1-hop friends
        try
        {
            var graphDb = RedisProvider.GetGraphDb();
            string cypher = $"MATCH (u:User {{user_id: {userId}}})-[:FRIENDS_WITH]-(f:User) RETURN f.user_id";
            var result = await graphDb.ExecuteAsync("GRAPH.QUERY", NewsFeedConfig.RedisGraphKey, cypher);
            friendIds = RedisGraphHelper.ParseFriendIds(result);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FeedService] Warning: RedisGraph query failed for user {userId}: {ex.Message}");
        }

        // Step B: Fallback to PostgreSQL friendships table
        if (friendIds.Count == 0)
        {
            try
            {
                await using var conn = await DbProvider.GetDataSource().OpenConnectionAsync();
                await using var cmd = new NpgsqlCommand(
                    "SELECT friend_id FROM friendships WHERE user_id = $1 AND status = 'ACCEPTED'", conn);
                cmd.Parameters.AddWithValue(userId);
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    friendIds.Add(reader.GetInt64(0));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[FeedService] Error: Postgres friendships query failed: {ex.Message}");
            }
        }

        if (friendIds.Count == 0) return;

        // Step C: Query PostgreSQL for posts by friends
        try
        {
            var posts = new List<CachedPost>();
            await using (var conn = await DbProvider.GetDataSource().OpenConnectionAsync())
            {
                await using var cmd = new NpgsqlCommand(
                    "SELECT post_id, author_id, content, media_urls::text, like_count, comment_count, created_at " +
                    "FROM posts WHERE author_id = ANY($1) ORDER BY created_at DESC LIMIT $2", conn);
                cmd.Parameters.AddWithValue(friendIds.ToArray());
                cmd.Parameters.AddWithValue(NewsFeedConfig.FeedMaxSize);

                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    long pid = reader.GetInt64(0);
                    long aid = reader.GetInt64(1);
                    string content = reader.GetString(2);
                    string urlsJson = reader.IsDBNull(3) ? "[]" : reader.GetString(3);
                    int likes = reader.GetInt32(4);
                    int comments = reader.GetInt32(5);
                    var dt = reader.GetDateTime(6);
                    long ms = new DateTimeOffset(dt).ToUnixTimeMilliseconds();

                    List<string> urls;
                    try
                    {
                        urls = JsonSerializer.Deserialize<List<string>>(urlsJson) ?? new List<string>();
                    }
                    catch
                    {
                        urls = new List<string>();
                    }

                    posts.Add(new CachedPost
                    {
                        PostId = pid,
                        AuthorId = aid,
                        Content = content,
                        MediaUrls = urls,
                        LikeCount = likes,
                        CommentCount = comments,
                        CreatedAt = ms
                    });
                }
            }

            if (posts.Count == 0) return;

            var cacheDb = RedisProvider.GetCacheDb();
            string feedKey = $"feed:{userId}";

            var entries = posts.Select(p => new SortedSetEntry(p.PostId.ToString(), p.CreatedAt)).ToArray();
            await cacheDb.SortedSetAddAsync(feedKey, entries);
            await cacheDb.SortedSetRemoveRangeByRankAsync(feedKey, 0, -(NewsFeedConfig.FeedMaxSize + 1));
            await cacheDb.KeyExpireAsync(feedKey, NewsFeedConfig.RedisFeedTtl);

            // Also cache post objects
            foreach (var p in posts)
            {
                await cacheDb.StringSetAsync($"post:{p.PostId}", JsonSerializer.Serialize(p), NewsFeedConfig.RedisPostTtl);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FeedService] Error populating feed cache for user {userId}: {ex.Message}");
        }
    }

    private async Task<Dictionary<long, CachedPost>> HydratePostsAsync(List<long> postIds)
    {
        var postsById = new Dictionary<long, CachedPost>();
        var missingIds = new List<long>();
        var cacheDb = RedisProvider.GetCacheDb();

        try
        {
            var keys = postIds.Select(id => (RedisKey)$"post:{id}").ToArray();
            var cached = await cacheDb.StringGetAsync(keys);

            for (int i = 0; i < postIds.Count; i++)
            {
                if (cached[i].HasValue)
                {
                    try
                    {
                        var post = JsonSerializer.Deserialize<CachedPost>((string)cached[i]!);
                        if (post != null)
                        {
                            postsById[postIds[i]] = post;
                            continue;
                        }
                    }
                    catch { }
                }
                missingIds.Add(postIds[i]);
            }
        }
        catch
        {
            missingIds = postIds.ToList();
        }

        if (missingIds.Count > 0)
        {
            try
            {
                await using var conn = await DbProvider.GetDataSource().OpenConnectionAsync();
                await using var cmd = new NpgsqlCommand(
                    "SELECT post_id, author_id, content, media_urls::text, like_count, comment_count, created_at " +
                    "FROM posts WHERE post_id = ANY($1)", conn);
                cmd.Parameters.AddWithValue(missingIds.ToArray());

                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    long pid = reader.GetInt64(0);
                    long aid = reader.GetInt64(1);
                    string content = reader.GetString(2);
                    string urlsJson = reader.IsDBNull(3) ? "[]" : reader.GetString(3);
                    int likes = reader.GetInt32(4);
                    int comments = reader.GetInt32(5);
                    var dt = reader.GetDateTime(6);
                    long ms = new DateTimeOffset(dt).ToUnixTimeMilliseconds();

                    List<string> urls;
                    try
                    {
                        urls = JsonSerializer.Deserialize<List<string>>(urlsJson) ?? new List<string>();
                    }
                    catch
                    {
                        urls = new List<string>();
                    }

                    var post = new CachedPost
                    {
                        PostId = pid,
                        AuthorId = aid,
                        Content = content,
                        MediaUrls = urls,
                        LikeCount = likes,
                        CommentCount = comments,
                        CreatedAt = ms
                    };

                    postsById[pid] = post;
                    _ = cacheDb.StringSetAsync($"post:{pid}", JsonSerializer.Serialize(post), NewsFeedConfig.RedisPostTtl);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[FeedService] Error fetching missing posts from DB: {ex.Message}");
            }
        }

        return postsById;
    }

    private async Task<Dictionary<long, AuthorProfile>> HydrateAuthorsAsync(List<long> authorIds)
    {
        var authorsById = new Dictionary<long, AuthorProfile>();
        var missingIds = new List<long>();
        var cacheDb = RedisProvider.GetCacheDb();

        try
        {
            var keys = authorIds.Select(id => (RedisKey)$"user:{id}").ToArray();
            var cached = await cacheDb.StringGetAsync(keys);

            for (int i = 0; i < authorIds.Count; i++)
            {
                if (cached[i].HasValue)
                {
                    try
                    {
                        var author = JsonSerializer.Deserialize<AuthorProfile>((string)cached[i]!);
                        if (author != null)
                        {
                            authorsById[authorIds[i]] = author;
                            continue;
                        }
                    }
                    catch { }
                }
                missingIds.Add(authorIds[i]);
            }
        }
        catch
        {
            missingIds = authorIds.ToList();
        }

        if (missingIds.Count > 0)
        {
            try
            {
                await using var conn = await DbProvider.GetDataSource().OpenConnectionAsync();
                await using var cmd = new NpgsqlCommand(
                    "SELECT user_id, username, display_name, avatar_url FROM users WHERE user_id = ANY($1)", conn);
                cmd.Parameters.AddWithValue(missingIds.ToArray());

                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    long uid = reader.GetInt64(0);
                    string username = reader.GetString(1);
                    string displayName = reader.GetString(2);
                    string? avatarUrl = reader.IsDBNull(3) ? null : reader.GetString(3);

                    var author = new AuthorProfile
                    {
                        UserId = uid,
                        Username = username,
                        DisplayName = displayName,
                        AvatarUrl = avatarUrl
                    };

                    authorsById[uid] = author;
                    _ = cacheDb.StringSetAsync($"user:{uid}", JsonSerializer.Serialize(author), NewsFeedConfig.RedisUserTtl);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[FeedService] Error fetching missing authors from DB: {ex.Message}");
            }
        }

        return authorsById;
    }

    public async Task<HealthResponse> CheckHealthAsync()
    {
        var services = new Dictionary<string, string>();

        // Postgres
        try
        {
            await using var conn = await DbProvider.GetDataSource().OpenConnectionAsync();
            await using var cmd = new NpgsqlCommand("SELECT 1", conn);
            var val = await cmd.ExecuteScalarAsync();
            services["postgres"] = val != null ? "healthy" : "unhealthy";
        }
        catch
        {
            services["postgres"] = "unhealthy";
        }

        // Redis cache
        try
        {
            var ping = await RedisProvider.GetCacheDb().PingAsync();
            services["redis"] = ping.TotalMilliseconds >= 0 ? "healthy" : "unhealthy";
        }
        catch
        {
            services["redis"] = "unhealthy";
        }

        // RedisGraph
        try
        {
            var ping = await RedisProvider.GetGraphDb().PingAsync();
            services["redis_graph"] = ping.TotalMilliseconds >= 0 ? "healthy" : "unhealthy";
        }
        catch
        {
            services["redis_graph"] = "unhealthy";
        }

        // Kafka
        services["kafka"] = _kafkaProducer.IsHealthy() ? "healthy" : "unhealthy";

        bool allHealthy = services.Values.All(v => v == "healthy");
        return new HealthResponse
        {
            Status = allHealthy ? "ok" : "degraded",
            Services = services
        };
    }
}
