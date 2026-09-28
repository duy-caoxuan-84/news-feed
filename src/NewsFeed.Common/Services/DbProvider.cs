using Npgsql;
using NewsFeed.Common.Configuration;

namespace NewsFeed.Common.Services;

public static class DbProvider
{
    private static NpgsqlDataSource? _dataSource;
    private static readonly object _lock = new();

    public static NpgsqlDataSource GetDataSource()
    {
        if (_dataSource != null) return _dataSource;

        lock (_lock)
        {
            if (_dataSource != null) return _dataSource;
            var builder = new NpgsqlDataSourceBuilder(NewsFeedConfig.PostgresConnectionString);
            _dataSource = builder.Build();
            return _dataSource;
        }
    }
}
