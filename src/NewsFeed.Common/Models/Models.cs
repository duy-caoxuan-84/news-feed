using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace NewsFeed.Common.Models;

public class PostCreateRequest
{
    [JsonPropertyName("author_id")]
    public long? AuthorId { get; set; }

    [JsonPropertyName("content")]
    public string Content { get; set; } = string.Empty;

    [JsonPropertyName("media_urls")]
    public List<string>? MediaUrls { get; set; } = new();
}

public class PostCreateResponse
{
    [JsonPropertyName("post_id")]
    public long PostId { get; set; }

    [JsonPropertyName("author_id")]
    public long AuthorId { get; set; }

    [JsonPropertyName("content")]
    public string Content { get; set; } = string.Empty;

    [JsonPropertyName("media_urls")]
    public List<string> MediaUrls { get; set; } = new();

    [JsonPropertyName("created_at")]
    public long CreatedAt { get; set; }
}

public class AuthorProfile
{
    [JsonPropertyName("user_id")]
    public long UserId { get; set; }

    [JsonPropertyName("username")]
    public string Username { get; set; } = string.Empty;

    [JsonPropertyName("display_name")]
    public string DisplayName { get; set; } = string.Empty;

    [JsonPropertyName("avatar_url")]
    public string? AvatarUrl { get; set; }
}

public class FeedPostItem
{
    [JsonPropertyName("post_id")]
    public long PostId { get; set; }

    [JsonPropertyName("author")]
    public AuthorProfile Author { get; set; } = new();

    [JsonPropertyName("content")]
    public string Content { get; set; } = string.Empty;

    [JsonPropertyName("media_urls")]
    public List<string> MediaUrls { get; set; } = new();

    [JsonPropertyName("like_count")]
    public int LikeCount { get; set; } = 0;

    [JsonPropertyName("comment_count")]
    public int CommentCount { get; set; } = 0;

    [JsonPropertyName("created_at")]
    public long CreatedAt { get; set; }
}

public class PaginationInfo
{
    [JsonPropertyName("next_cursor")]
    public string? NextCursor { get; set; }

    [JsonPropertyName("has_more")]
    public bool HasMore { get; set; } = false;
}

public class FeedResponse
{
    [JsonPropertyName("data")]
    public List<FeedPostItem> Data { get; set; } = new();

    [JsonPropertyName("pagination")]
    public PaginationInfo Pagination { get; set; } = new();
}

public class PostCreatedEvent
{
    [JsonPropertyName("author_id")]
    public long AuthorId { get; set; }

    [JsonPropertyName("post_id")]
    public long PostId { get; set; }

    [JsonPropertyName("created_at")]
    public long CreatedAt { get; set; }
}

public class CachedPost
{
    [JsonPropertyName("post_id")]
    public long PostId { get; set; }

    [JsonPropertyName("author_id")]
    public long AuthorId { get; set; }

    [JsonPropertyName("content")]
    public string Content { get; set; } = string.Empty;

    [JsonPropertyName("media_urls")]
    public List<string> MediaUrls { get; set; } = new();

    [JsonPropertyName("like_count")]
    public int LikeCount { get; set; } = 0;

    [JsonPropertyName("comment_count")]
    public int CommentCount { get; set; } = 0;

    [JsonPropertyName("created_at")]
    public long CreatedAt { get; set; }
}

public class HealthResponse
{
    [JsonPropertyName("status")]
    public string Status { get; set; } = "ok";

    [JsonPropertyName("services")]
    public Dictionary<string, string> Services { get; set; } = new();
}
