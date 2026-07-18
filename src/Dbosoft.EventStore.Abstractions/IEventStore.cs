namespace Dbosoft.EventStore;

/// <summary>
/// The core event store: append events to a stream under optimistic concurrency, and read a stream
/// back. Every provider implements this. Optional features (global reads, snapshots, maintenance)
/// are separate interfaces gated by <see cref="Capabilities"/>. Live delivery is not the store's
/// job — a message bus is presupposed; the store provides catch-up reads to feed and replay it.
/// </summary>
[PublicAPI]
public interface IEventStore
{
    /// <summary>The optional features this provider supports.</summary>
    StoreCapabilities Capabilities { get; }

    /// <summary>
    /// Appends <paramref name="events"/> to the end of a stream, provided the stream is at
    /// <paramref name="expectedVersion"/> (see <see cref="ExpectedVersion"/> for the sentinels).
    /// Idempotency is scoped to the whole batch, keyed by <see cref="EventData.EventId"/>: re-appending
    /// a batch whose ids are all already present is a no-op that returns the original outcome, a batch
    /// that only partially overlaps stored events is rejected, and a duplicate id within one batch is
    /// rejected. Dedup holds only while the events are retained (truncation drops their ids). An empty
    /// batch writes nothing, but is still a checked no-op: it honours <paramref name="expectedVersion"/>
    /// (throwing on a mismatch) and returns the current version.
    /// </summary>
    /// <exception cref="WrongExpectedVersionException">The stream is not at the expected version.</exception>
    /// <exception cref="StreamDeletedException">The stream has been hard-deleted.</exception>
    /// <exception cref="InvalidOperationException">The batch partially overlaps stored events, or repeats an id.</exception>
    Task<AppendResult> AppendToStreamAsync(
        string streamId,
        long expectedVersion,
        IReadOnlyList<EventData> events,
        CancellationToken ct = default);

    /// <summary>
    /// Reads a stream in the given direction. Forward reads start at <paramref name="fromVersion"/>
    /// (inclusive, 1-based; <c>0</c> means from the start); backward reads start at that version and
    /// descend (<c>long.MaxValue</c> means from the end). Yields nothing for a missing stream.
    /// </summary>
    IAsyncEnumerable<StoredEvent> ReadStreamAsync(
        string streamId,
        ReadDirection direction = ReadDirection.Forwards,
        long fromVersion = 0,
        long maxCount = long.MaxValue,
        CancellationToken ct = default);

    /// <summary>Returns a stream's current state, or <c>null</c> if it does not exist.</summary>
    Task<StreamInfo?> GetStreamInfoAsync(string streamId, CancellationToken ct = default);
}
