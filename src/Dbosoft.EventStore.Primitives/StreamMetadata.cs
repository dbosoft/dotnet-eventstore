using System.Text.Json;
using System.Text.Json.Serialization;

namespace Dbosoft.EventStore;

/// <summary>
/// Per-stream metadata: a retention policy (keep at most so many events, or drop events older than an
/// age) plus arbitrary custom key/values. Stored and returned by a provider that implements
/// <c>IStreamMetadataStore</c>, and applied by a scavenge with retention enabled.
/// </summary>
[PublicAPI]
public sealed record StreamMetadata
{
    private static readonly JsonSerializerOptions Json =
        new(JsonSerializerDefaults.Web) { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };

    /// <summary>Keep at most this many of the newest events; older ones are truncated by retention. Non-negative.</summary>
    public long? MaxCount
    {
        get;
        init
        {
            if (value is < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "MaxCount cannot be negative.");
            field = value;
        }
    }

    /// <summary>Truncate events older than this age (by their recorded time) during retention. Non-negative.</summary>
    public TimeSpan? MaxAge
    {
        get;
        init
        {
            if (value is { } age && age < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(value), value, "MaxAge cannot be negative.");
            field = value;
        }
    }

    /// <summary>Arbitrary caller-defined key/value metadata, ignored by retention.</summary>
    public IReadOnlyDictionary<string, string>? Custom { get; init; }

    /// <summary>Whether any retention rule is set, so a scavenge has something to apply.</summary>
    [JsonIgnore]
    public bool HasRetention => MaxCount is not null || MaxAge is not null;

    /// <summary>Serializes to the canonical JSON a provider stores.</summary>
    public string ToJson() => JsonSerializer.Serialize(this, Json);

    /// <summary>Deserializes provider-stored JSON, or <c>null</c> when there is none.</summary>
    public static StreamMetadata? FromJson(string? json) =>
        string.IsNullOrEmpty(json) ? null : JsonSerializer.Deserialize<StreamMetadata>(json, Json);
}
