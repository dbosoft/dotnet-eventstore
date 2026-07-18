using Azure.Data.Tables;

namespace Dbosoft.EventStore.AzureTables;

/// <summary>
/// Snapshot store for one domain, backed by that domain's snapshots table and blob container. Each
/// snapshot is a row in the stream's partition, keyed by its reversed version so the newest sorts
/// first — giving version history (<see cref="StoreCapabilities.SnapshotHistory"/>) and a cheap
/// "latest" read. Large snapshot payloads spill to a blob.
/// </summary>
internal sealed class AzureTablesSnapshotStore(TableClient snapshots, PayloadStore payloads, TimeProvider timeProvider)
    : ISnapshotStore
{
    public async Task SaveAsync(
        string streamId,
        long streamVersion,
        ReadOnlyMemory<byte> payload,
        string contentType = "application/json",
        CancellationToken ct = default)
    {
        var slot = await payloads.PutAsync(TableSchema.SnapshotBlobName(streamId, streamVersion), payload, ct).ConfigureAwait(false);
        var entity = TableSchema.SnapshotEntity(streamId, streamVersion, slot, contentType, timeProvider.GetUtcNow());
        await snapshots.UpsertEntityAsync(entity, TableUpdateMode.Replace, ct).ConfigureAwait(false);
    }

    public Task<StoredSnapshot?> LoadLatestAsync(string streamId, CancellationToken ct = default) =>
        LoadAtOrBeforeAsync(streamId, long.MaxValue, ct);

    public async Task<StoredSnapshot?> LoadAtOrBeforeAsync(string streamId, long maxVersion, CancellationToken ct = default)
    {
        // Reversed row keys sort newest-first, so the newest snapshot with version <= maxVersion is the
        // first row at or after that version's key.
        var filter = $"PartitionKey eq '{Escape(streamId)}' and RowKey ge '{TableSchema.SnapshotRowKey(maxVersion)}'";
        await foreach (var entity in snapshots
                           .QueryAsync<TableEntity>(filter, maxPerPage: 1, cancellationToken: ct)
                           .ConfigureAwait(false))
        {
            var payload = await payloads.GetAsync(TableSchema.ReadSlot(entity, "Payload"), ct).ConfigureAwait(false)
                          ?? ReadOnlyMemory<byte>.Empty;
            return TableSchema.ToSnapshot(entity, payload);
        }

        return null;
    }

    public async Task PruneAsync(string streamId, int keep, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(keep);

        var stale = new List<TableEntity>();
        var seen = 0;
        // Ascending row key is newest-first, so the first `keep` are retained and the rest removed.
        await foreach (var entity in snapshots
                           .QueryAsync<TableEntity>($"PartitionKey eq '{Escape(streamId)}'", cancellationToken: ct)
                           .ConfigureAwait(false))
        {
            if (seen++ >= keep)
                stale.Add(entity);
        }

        foreach (var chunk in stale.Chunk(100))
        {
            await snapshots.SubmitTransactionAsync(
                    chunk.Select(e => new TableTransactionAction(TableTransactionActionType.Delete, e, e.ETag)), ct)
                .ConfigureAwait(false);
        }

        foreach (var entity in stale)
        {
            var slot = TableSchema.ReadSlot(entity, "Payload");
            if (slot.Mode == PayloadMode.Blob)
                await payloads.DeleteAsync(slot.BlobName!, ct).ConfigureAwait(false);
        }
    }

    private static string Escape(string value) => value.Replace("'", "''");
}
