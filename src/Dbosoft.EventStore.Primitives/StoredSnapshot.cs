namespace Dbosoft.EventStore;

/// <summary>A snapshot as read back from the store.</summary>
/// <param name="StreamId">The stream the snapshot summarizes.</param>
/// <param name="StreamVersion">The stream version the snapshot's state reflects.</param>
/// <param name="Payload">The serialized snapshot state.</param>
/// <param name="ContentType">The media type of <see cref="Payload"/>.</param>
/// <param name="CreatedAt">When the snapshot was written.</param>
public sealed record StoredSnapshot(
    string StreamId,
    long StreamVersion,
    ReadOnlyMemory<byte> Payload,
    string ContentType,
    DateTimeOffset CreatedAt);
