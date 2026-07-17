namespace Dbosoft.EventStore;

/// <summary>An event as read back from the store, carrying the identity the store assigned it.</summary>
/// <param name="StreamId">The stream this event belongs to.</param>
/// <param name="StreamVersion">The 1-based position of this event within its stream.</param>
/// <param name="Position">The event's place in the store's global ordering.</param>
/// <param name="EventId">The stable event identity supplied at append time.</param>
/// <param name="EventType">The logical type name.</param>
/// <param name="Data">The serialized event payload.</param>
/// <param name="Metadata">Optional serialized metadata.</param>
/// <param name="ContentType">The media type of <see cref="Data"/> and <see cref="Metadata"/>.</param>
/// <param name="CreatedAt">When the store recorded the event.</param>
public sealed record StoredEvent(
    string StreamId,
    long StreamVersion,
    GlobalPosition Position,
    Guid EventId,
    string EventType,
    ReadOnlyMemory<byte> Data,
    ReadOnlyMemory<byte>? Metadata,
    string ContentType,
    DateTimeOffset CreatedAt);
