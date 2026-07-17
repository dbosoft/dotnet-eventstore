namespace Dbosoft.EventStore.InMemory;

/// <summary>
/// Snapshot store backed by the same data as its <see cref="InMemoryEventStore"/>, keeping a version
/// history per stream.
/// </summary>
public sealed class InMemorySnapshotStore : ISnapshotStore
{
    private readonly InMemoryEventStore _store;

    internal InMemorySnapshotStore(InMemoryEventStore store) => _store = store;

    /// <inheritdoc />
    public Task SaveAsync(
        string streamId,
        long streamVersion,
        ReadOnlyMemory<byte> payload,
        string contentType = "application/json",
        CancellationToken ct = default)
    {
        _store.SaveSnapshot(streamId, streamVersion, payload, contentType);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<StoredSnapshot?> LoadLatestAsync(string streamId, CancellationToken ct = default) =>
        Task.FromResult(_store.LoadSnapshot(streamId, long.MaxValue));

    /// <inheritdoc />
    public Task<StoredSnapshot?> LoadAtOrBeforeAsync(string streamId, long maxVersion, CancellationToken ct = default) =>
        Task.FromResult(_store.LoadSnapshot(streamId, maxVersion));

    /// <inheritdoc />
    public Task PruneAsync(string streamId, int keep, CancellationToken ct = default)
    {
        _store.PruneSnapshots(streamId, keep);
        return Task.CompletedTask;
    }
}
