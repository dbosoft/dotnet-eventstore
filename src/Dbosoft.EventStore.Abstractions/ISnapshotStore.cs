namespace Dbosoft.EventStore;

/// <summary>
/// Stores folded stream state so an aggregate can be rehydrated without replaying from version 1.
/// A provider may keep only the latest snapshot per stream, or a history of them
/// (<see cref="StoreCapabilities.SnapshotHistory"/>).
/// </summary>
[PublicAPI]
public interface ISnapshotStore
{
    /// <summary>Writes a snapshot of <paramref name="streamId"/> at <paramref name="streamVersion"/>.</summary>
    Task SaveAsync(
        string streamId,
        long streamVersion,
        ReadOnlyMemory<byte> payload,
        string contentType = "application/json",
        CancellationToken ct = default);

    /// <summary>Returns the most recent snapshot for a stream, or <c>null</c> if none exists.</summary>
    Task<StoredSnapshot?> LoadLatestAsync(string streamId, CancellationToken ct = default);

    /// <summary>
    /// Returns the most recent snapshot at or before <paramref name="maxVersion"/>, or <c>null</c>.
    /// With only latest-snapshot support this behaves like <see cref="LoadLatestAsync"/> filtered by
    /// version.
    /// </summary>
    Task<StoredSnapshot?> LoadAtOrBeforeAsync(string streamId, long maxVersion, CancellationToken ct = default);

    /// <summary>
    /// Keeps the newest <paramref name="keep"/> snapshots of a stream and removes older ones. A no-op
    /// on providers that keep only the latest.
    /// </summary>
    Task PruneAsync(string streamId, int keep, CancellationToken ct = default);
}
