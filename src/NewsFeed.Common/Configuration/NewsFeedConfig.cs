using System;

namespace NewsFeed.Common.Configuration;

public static class NewsFeedConfig
{
    public static string PostgresHost => Environment.GetEnvironmentVariable("POSTGRES_HOST") ?? "postgres";
    public static int PostgresPort => int.TryParse(Environment.GetEnvironmentVariable("POSTGRES_PORT"), out var port) ? port : 5432;
    public static string PostgresUser => Environment.GetEnvironmentVariable("POSTGRES_USER") ?? "postgres";
    public static string PostgresPassword => Environment.GetEnvironmentVariable("POSTGRES_PASSWORD") ?? "postgres";
    public static string PostgresDb => Environment.GetEnvironmentVariable("POSTGRES_DB") ?? "newsfeed_db";

    public static string PostgresConnectionString =>
        $"Host={PostgresHost};Port={PostgresPort};Username={PostgresUser};Password={PostgresPassword};Database={PostgresDb};Pooling=true;Minimum Pool Size=2;Maximum Pool Size=20;";

    public static string RedisHost => Environment.GetEnvironmentVariable("REDIS_HOST") ?? "redis";
    public static int RedisPort => int.TryParse(Environment.GetEnvironmentVariable("REDIS_PORT"), out var port) ? port : 6379;
    public static string RedisPassword => Environment.GetEnvironmentVariable("REDIS_PASSWORD") ?? "";
    public static string RedisConnectionString =>
        string.IsNullOrEmpty(RedisPassword) ? $"{RedisHost}:{RedisPort},abortConnect=false" : $"{RedisHost}:{RedisPort},password={RedisPassword},abortConnect=false";

    public static string RedisGraphHost => Environment.GetEnvironmentVariable("REDIS_GRAPH_HOST") ?? "redis-graph";
    public static int RedisGraphPort => int.TryParse(Environment.GetEnvironmentVariable("REDIS_GRAPH_PORT"), out var port) ? port : 6379;
    public static string RedisGraphKey => Environment.GetEnvironmentVariable("REDIS_GRAPH_KEY") ?? "social_graph";
    public static string RedisGraphConnectionString => $"{RedisGraphHost}:{RedisGraphPort},abortConnect=false";

    public static string KafkaBootstrapServers => Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "kafka:29092";
    public static string KafkaTopic => Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "post.created";
    public static string KafkaGroupId => Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") ?? "newsfeed-fanout-workers";

    public static int FeedMaxSize => int.TryParse(Environment.GetEnvironmentVariable("FEED_MAX_SIZE"), out var s) ? s : 500;
    public static int WorkerId => int.TryParse(Environment.GetEnvironmentVariable("WORKER_ID"), out var w) ? w : 1;

    public static TimeSpan RedisFeedTtl => TimeSpan.FromDays(7);
    public static TimeSpan RedisPostTtl => TimeSpan.FromDays(7);
    public static TimeSpan RedisUserTtl => TimeSpan.FromDays(7);
}
