using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NewsFeed.Common.Configuration;
using NewsFeed.Common.Models;
using NewsFeed.Common.Services;
using NewsFeed.Common.Utils;
using Npgsql;
using StackExchange.Redis;

namespace NewsFeed.Worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("News Feed Fan-out Worker starting...");

        var conf = new ConsumerConfig
        {
            GroupId = NewsFeedConfig.KafkaGroupId,
            BootstrapServers = NewsFeedConfig.KafkaBootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnablePartitionEof = false
        };

        IConsumer<string, string>? consumer = null;

        // Retry connecting to Kafka
        for (int attempt = 1; attempt <= 20; attempt++)
        {
            if (stoppingToken.IsCancellationRequested) return;

            try
            {
                consumer = new ConsumerBuilder<string, string>(conf).Build();
                consumer.Subscribe(NewsFeedConfig.KafkaTopic);
                _logger.LogInformation("Connected to Kafka and subscribed to {Topic}", NewsFeedConfig.KafkaTopic);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Kafka connection attempt {Attempt}/20 failed: {Message}. Retrying in 2s...", attempt, ex.Message);
                consumer?.Dispose();
                consumer = null;
                await Task.Delay(2000, stoppingToken);
            }
        }

        if (consumer == null)
        {
            _logger.LogError("Could not connect to Kafka broker. Exiting worker.");
            return;
        }

        using (consumer)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(500));
                    if (consumeResult == null || consumeResult.IsPartitionEOF || string.IsNullOrEmpty(consumeResult.Message?.Value))
                    {
                        await Task.Delay(50, stoppingToken);
                        continue;
                    }

                    var evt = JsonSerializer.Deserialize<PostCreatedEvent>(consumeResult.Message.Value);
                    if (evt == null || evt.AuthorId <= 0 || evt.PostId <= 0)
                    {
                        consumer.Commit(consumeResult);
                        continue;
                    }

                    _logger.LogInformation("Processing post.created event: post_id={PostId}, author_id={AuthorId}", evt.PostId, evt.AuthorId);

                    await ProcessPostCreatedEventAsync(evt);
                    consumer.Commit(consumeResult);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing Kafka message");
                    await Task.Delay(500, stoppingToken);
                }
            }
        }

        _logger.LogInformation("News Feed Fan-out Worker stopped.");
    }

    private async Task ProcessPostCreatedEventAsync(PostCreatedEvent evt)
    {
        var friendIds = new List<long>();

        // 1. Query RedisGraph for 1-hop friends
        try
        {
            var graphDb = RedisProvider.GetGraphDb();
            string cypher = $"MATCH (u:User {{user_id: {evt.AuthorId}}})-[:FRIENDS_WITH]-(f:User) RETURN f.user_id";
            var result = await graphDb.ExecuteAsync("GRAPH.QUERY", NewsFeedConfig.RedisGraphKey, cypher);
            friendIds = RedisGraphHelper.ParseFriendIds(result);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("RedisGraph query failed for author {AuthorId}: {Message}", evt.AuthorId, ex.Message);
        }

        // 2. Fallback to PostgreSQL friendships table
        if (friendIds.Count == 0)
        {
            try
            {
                await using var conn = await DbProvider.GetDataSource().OpenConnectionAsync();
                await using var cmd = new NpgsqlCommand(
                    "SELECT friend_id FROM friendships WHERE user_id = $1 AND status = 'ACCEPTED'", conn);
                cmd.Parameters.AddWithValue(evt.AuthorId);
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    friendIds.Add(reader.GetInt64(0));
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("PostgreSQL friendships query failed for author {AuthorId}: {Message}", evt.AuthorId, ex.Message);
            }
        }

        if (friendIds.Count == 0)
        {
            _logger.LogInformation("Author {AuthorId} has no friends to fan out to.", evt.AuthorId);
            return;
        }

        // 3. Fan-out to friends' feed sorted sets in Redis Cache
        try
        {
            var cacheDb = RedisProvider.GetCacheDb();
            var batch = cacheDb.CreateBatch();
            var tasks = new List<Task>();

            foreach (var friendId in friendIds)
            {
                string feedKey = $"feed:{friendId}";
                // ZADD feed:{friend_id} <created_at_epoch_ms> <post_id>
                tasks.Add(batch.SortedSetAddAsync(feedKey, evt.PostId.ToString(), evt.CreatedAt));
                // ZREMRANGEBYRANK feed:{friend_id} 0 -501
                tasks.Add(batch.SortedSetRemoveRangeByRankAsync(feedKey, 0, -(NewsFeedConfig.FeedMaxSize + 1)));
                tasks.Add(batch.KeyExpireAsync(feedKey, NewsFeedConfig.RedisFeedTtl));
            }

            batch.Execute();
            await Task.WhenAll(tasks);

            _logger.LogInformation("Successfully fanned out post {PostId} to {Count} friends: [{Friends}]",
                evt.PostId, friendIds.Count, string.Join(", ", friendIds));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update Redis feed caches for post {PostId}", evt.PostId);
        }
    }
}
