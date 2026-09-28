using System.Collections.Generic;
using StackExchange.Redis;

namespace NewsFeed.Common.Utils;

public static class RedisGraphHelper
{
    public static List<long> ParseFriendIds(RedisResult result)
    {
        var friendIds = new List<long>();
        if (result.IsNull) return friendIds;

        if (result.Type == ResultType.MultiBulk)
        {
            var array = (RedisResult[])result;
            // Standard RedisGraph response format: [0]=headers, [1]=rows, [2]=stats
            if (array.Length >= 2 && array[1].Type == ResultType.MultiBulk)
            {
                var rows = (RedisResult[])array[1];
                foreach (var row in rows)
                {
                    if (row.Type == ResultType.MultiBulk)
                    {
                        var cols = (RedisResult[])row;
                        if (cols.Length > 0 && long.TryParse(cols[0].ToString(), out var id))
                        {
                            friendIds.Add(id);
                        }
                    }
                    else if (long.TryParse(row.ToString(), out var id))
                    {
                        friendIds.Add(id);
                    }
                }
                return friendIds;
            }

            // Fallback for flat array or alternative layout
            foreach (var item in array)
            {
                if (item.Type == ResultType.MultiBulk)
                {
                    var sub = (RedisResult[])item;
                    foreach (var s in sub)
                    {
                        if (long.TryParse(s.ToString(), out var id))
                        {
                            friendIds.Add(id);
                        }
                    }
                }
                else if (long.TryParse(item.ToString(), out var id))
                {
                    friendIds.Add(id);
                }
            }
        }

        return friendIds;
    }
}
