using System;

namespace NewsFeed.Common.Utils;

public class SnowflakeGenerator
{
    private const long Epoch = 1704067200000L; // 2024-01-01 00:00:00 UTC in ms
    private const int WorkerIdBits = 10;
    private const int SequenceBits = 12;

    private const long MaxWorkerId = (1L << WorkerIdBits) - 1;
    private const long MaxSequence = (1L << SequenceBits) - 1;

    private const int WorkerIdShift = SequenceBits;
    private const int TimestampShift = SequenceBits + WorkerIdBits;

    private readonly long _workerId;
    private long _sequence = 0L;
    private long _lastTimestamp = -1L;
    private readonly object _lock = new();

    public SnowflakeGenerator(long workerId = 1)
    {
        if (workerId < 0 || workerId > MaxWorkerId)
        {
            throw new ArgumentOutOfRangeException(nameof(workerId), $"workerId must be between 0 and {MaxWorkerId}");
        }
        _workerId = workerId;
    }

    private static long CurrentTimestampMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    public long Generate()
    {
        lock (_lock)
        {
            long timestamp = CurrentTimestampMs();
            if (timestamp < _lastTimestamp)
            {
                timestamp = _lastTimestamp;
            }

            if (timestamp == _lastTimestamp)
            {
                _sequence = (_sequence + 1) & MaxSequence;
                if (_sequence == 0)
                {
                    while (timestamp <= _lastTimestamp)
                    {
                        timestamp = CurrentTimestampMs();
                    }
                }
            }
            else
            {
                _sequence = 0;
            }

            _lastTimestamp = timestamp;

            return ((timestamp - Epoch) << TimestampShift)
                   | (_workerId << WorkerIdShift)
                   | _sequence;
        }
    }
}
