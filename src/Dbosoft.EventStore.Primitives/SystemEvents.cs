namespace Dbosoft.EventStore;

/// <summary>
/// Reserved event types the store itself emits into <c>$all</c> when it destroys or rewrites data,
/// so downstream read models (over the bus) learn to forget or reproject. They are distinguished by
/// a leading <c>$</c> and materialize to the marker records below regardless of the domain serializer.
/// </summary>
[PublicAPI]
public static class SystemEventTypes
{
    /// <summary>The reserved stream that carries system marker events; it shows up in <c>$all</c>.</summary>
    public const string SystemStream = "$system";

    /// <summary>Emitted when a stream's events below a version are truncated.</summary>
    public const string StreamTruncated = "$stream-truncated";

    /// <summary>Emitted when a whole stream is deleted.</summary>
    public const string StreamDeleted = "$stream-deleted";

    /// <summary>Emitted when a single event's payload is redacted in place (PII erasure).</summary>
    public const string EventRedacted = "$event-redacted";

    /// <summary>Whether an event type is a reserved system type (leading <c>$</c>).</summary>
    public static bool IsSystem(string eventType) => eventType.StartsWith('$');
}

/// <summary>Payload of a <see cref="SystemEventTypes.StreamTruncated"/> marker.</summary>
/// <param name="StreamId">The truncated stream.</param>
/// <param name="BeforeVersion">Events below this version were removed.</param>
[PublicAPI]
public sealed record StreamTruncatedMarker(string StreamId, long BeforeVersion);

/// <summary>Payload of a <see cref="SystemEventTypes.StreamDeleted"/> marker.</summary>
/// <param name="StreamId">The deleted stream.</param>
/// <param name="Mode">How it was deleted.</param>
[PublicAPI]
public sealed record StreamDeletedMarker(string StreamId, DeleteMode Mode);

/// <summary>Payload of a <see cref="SystemEventTypes.EventRedacted"/> marker.</summary>
/// <param name="StreamId">The stream whose event was redacted.</param>
/// <param name="Version">The version of the event whose payload was overwritten.</param>
[PublicAPI]
public sealed record EventRedactedMarker(string StreamId, long Version);
