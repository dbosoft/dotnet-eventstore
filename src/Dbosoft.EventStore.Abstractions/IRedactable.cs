namespace Dbosoft.EventStore;

/// <summary>
/// A store in which a single event's payload can be overwritten in place (PII erasure by redaction).
/// Optional — a provider supports redaction iff it implements this interface.
/// </summary>
[PublicAPI]
public interface IRedactable
{
    /// <summary>
    /// Overwrites the payload of a single event in place — its version, position and id are kept, only
    /// <see cref="StoredEvent.Data"/> (and metadata) change. This is PII erasure by redaction, not a
    /// deletion: the event's slot survives, so the stream shape is preserved. A
    /// <see cref="SystemEventTypes.EventRedacted"/> marker is emitted so read models reproject.
    /// </summary>
    /// <exception cref="StreamNotFoundException">The stream or the version does not exist.</exception>
    Task RedactEventAsync(
        string streamId,
        long version,
        ReadOnlyMemory<byte> newData,
        ReadOnlyMemory<byte>? newMetadata = null,
        CancellationToken ct = default);
}
