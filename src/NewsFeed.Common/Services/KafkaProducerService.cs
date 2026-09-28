using System;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using NewsFeed.Common.Configuration;
using NewsFeed.Common.Models;

namespace NewsFeed.Common.Services;

public class KafkaProducerService : IDisposable
{
    private readonly IProducer<string, string> _producer;

    public KafkaProducerService()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = NewsFeedConfig.KafkaBootstrapServers,
            Acks = Acks.All,
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 1000
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task PublishPostCreatedAsync(long authorId, long postId, long createdAt)
    {
        var evt = new PostCreatedEvent
        {
            AuthorId = authorId,
            PostId = postId,
            CreatedAt = createdAt
        };

        var message = new Message<string, string>
        {
            Key = authorId.ToString(),
            Value = JsonSerializer.Serialize(evt)
        };

        await _producer.ProduceAsync(NewsFeedConfig.KafkaTopic, message);
    }

    public bool IsHealthy()
    {
        try
        {
            // Simple check: producer handle is valid
            return _producer.Handle != null;
        }
        catch
        {
            return false;
        }
    }

    public void Dispose()
    {
        try
        {
            _producer?.Flush(TimeSpan.FromSeconds(5));
            _producer?.Dispose();
        }
        catch { }
    }
}
