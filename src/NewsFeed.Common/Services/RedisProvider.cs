using System;
using NewsFeed.Common.Configuration;
using StackExchange.Redis;

namespace NewsFeed.Common.Services;

public static class RedisProvider
{
    private static ConnectionMultiplexer? _cacheMux;
    private static ConnectionMultiplexer? _graphMux;
    private static readonly object _lock = new();

    public static ConnectionMultiplexer GetCacheMux()
    {
        if (_cacheMux != null && _cacheMux.IsConnected) return _cacheMux;

        lock (_lock)
        {
            if (_cacheMux != null && _cacheMux.IsConnected) return _cacheMux;
            _cacheMux = ConnectionMultiplexer.Connect(NewsFeedConfig.RedisConnectionString);
            return _cacheMux;
        }
    }

    public static IDatabase GetCacheDb() => GetCacheMux().GetDatabase();

    public static ConnectionMultiplexer GetGraphMux()
    {
        if (_graphMux != null && _graphMux.IsConnected) return _graphMux;

        lock (_lock)
        {
            if (_graphMux != null && _graphMux.IsConnected) return _graphMux;
            _graphMux = ConnectionMultiplexer.Connect(NewsFeedConfig.RedisGraphConnectionString);
            return _graphMux;
        }
    }

    public static IDatabase GetGraphDb() => GetGraphMux().GetDatabase();
}
