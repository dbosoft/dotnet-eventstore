namespace Dbosoft.EventStore;

/// <summary>
/// Locates a global position from a wall-clock timestamp, so callers can replay from a point in
/// time. Optional; gated by <see cref="StoreCapabilities.TimeNavigation"/>.
/// </summary>
public interface ITimeNavigable
{
    /// <summary>
    /// Returns a checkpoint to feed to <see cref="IAllStreamReader.ReadAllAsync"/> so that the first
    /// event delivered is the earliest one recorded at or after <paramref name="timestamp"/>. Because
    /// reads are exclusive of their starting position, this is the position of the last event before
    /// <paramref name="timestamp"/>, or <see cref="GlobalPosition.Start"/> if there is none.
    /// </summary>
    Task<GlobalPosition> FindPositionAtAsync(DateTimeOffset timestamp, CancellationToken ct = default);
}
