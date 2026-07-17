namespace Dbosoft.EventStore;

/// <summary>
/// Reads events across all streams in global order. Optional; gated by
/// <see cref="StoreCapabilities.AllStreamReads"/>.
/// </summary>
[PublicAPI]
public interface IAllStreamReader
{
    /// <summary>
    /// Reads the global sequence from <paramref name="from"/> (exclusive) in the given direction.
    /// Use <see cref="GlobalPosition.Start"/> or <see cref="GlobalPosition.End"/> for the ends.
    /// </summary>
    IAsyncEnumerable<StoredEvent> ReadAllAsync(
        GlobalPosition from,
        ReadDirection direction = ReadDirection.Forwards,
        long maxCount = long.MaxValue,
        CancellationToken ct = default);
}
