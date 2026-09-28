using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NewsFeed.Api.Services;
using NewsFeed.Common.Models;
using NewsFeed.Common.Services;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.UseUrls("http://0.0.0.0:8000");

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower;
    options.SerializerOptions.DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull;
});

builder.Services.AddSingleton<KafkaProducerService>();
builder.Services.AddSingleton<FeedService>();

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod();
    });
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

app.UseCors();

app.UseSwagger();
app.UseSwaggerUI(c =>
{
    c.SwaggerEndpoint("/swagger/v1/swagger.json", "News Feed API v1");
    c.RoutePrefix = "swagger";
});

app.MapGet("/", () => Results.Redirect("/swagger"));

app.MapGet("/health", async (FeedService feedService) =>
{
    var health = await feedService.CheckHealthAsync();
    return Results.Ok(health);
})
.WithName("GetHealth")
.WithTags("Health");

app.MapPost("/v1/feed", async (
    [FromBody] PostCreateRequest request,
    [FromHeader(Name = "X-User-Id")] string? xUserId,
    FeedService feedService) =>
{
    long? authorId = request.AuthorId;
    if (authorId == null && !string.IsNullOrWhiteSpace(xUserId))
    {
        if (long.TryParse(xUserId, out long parsedId))
        {
            authorId = parsedId;
        }
        else
        {
            return Results.BadRequest(new { detail = "Invalid X-User-Id header format" });
        }
    }

    if (authorId == null)
    {
        return Results.BadRequest(new { detail = "author_id is required in request body or X-User-Id header" });
    }

    if (string.IsNullOrWhiteSpace(request.Content))
    {
        return Results.BadRequest(new { detail = "content is required" });
    }

    var result = await feedService.CreatePostAsync(authorId.Value, request.Content, request.MediaUrls);
    if (result == null)
    {
        return Results.NotFound(new { detail = $"User {authorId} not found" });
    }

    return Results.Json(result, statusCode: StatusCodes.Status201Created);
})
.WithName("PublishFeedPost")
.WithTags("Feed");

app.MapGet("/v1/feed", async (
    [FromQuery(Name = "user_id")] long? userId,
    [FromQuery(Name = "limit")] int? limit,
    [FromQuery(Name = "cursor")] string? cursor,
    [FromHeader(Name = "X-User-Id")] string? xUserId,
    FeedService feedService) =>
{
    long? targetUserId = userId;
    if (targetUserId == null && !string.IsNullOrWhiteSpace(xUserId))
    {
        if (long.TryParse(xUserId, out long parsedId))
        {
            targetUserId = parsedId;
        }
        else
        {
            return Results.BadRequest(new { detail = "Invalid X-User-Id header format" });
        }
    }

    if (targetUserId == null)
    {
        return Results.BadRequest(new { detail = "user_id is required in query parameter or X-User-Id header" });
    }

    int effectiveLimit = limit ?? 20;
    if (effectiveLimit < 1 || effectiveLimit > 100)
    {
        return Results.BadRequest(new { detail = "limit must be between 1 and 100" });
    }

    var feed = await feedService.GetFeedAsync(targetUserId.Value, effectiveLimit, cursor);
    return Results.Ok(feed);
})
.WithName("GetNewsFeed")
.WithTags("Feed");

app.Run();
