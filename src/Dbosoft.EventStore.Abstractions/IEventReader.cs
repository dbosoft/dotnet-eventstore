namespace Dbosoft.EventStore;

/// <summary>
/// The read-only surface of an event store: read a stream back and inspect its state. A read-model or relay
/// component can be wired with just this (keyed by domain) and is then structurally unable to write. Reading
/// the global sequence is the separate optional <see cref="IAllStreamReader"/>.
/// </summary>
[PublicAPI]
public interface IEventReader
{
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
