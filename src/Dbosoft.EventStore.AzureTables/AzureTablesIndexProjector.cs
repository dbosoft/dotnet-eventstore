using System.Text.Json;
using Azure.Data.Tables;

namespace Dbosoft.EventStore.AzureTables;

/// <summary>
/// Maintains a domain's per-stream descending index by tailing its <c>$all</c> stream from a durable
/// checkpoint — the same shape as a bus relay, but projecting into an index table instead of onto a
/// bus. Each event gets a reverse-version index row so a backward stream read is a server-side range
/// scan; truncate/delete markers prune the pointers they invalidate. It is idempotent: re-projecting an
/// event upserts the same row, so a checkpoint replay is harmless. Run it as a hosted service (or off a
/// bus feed) — the index is eventually consistent, and backward reads fall back to the events table for
/// the not-yet-indexed tail, so they are always correct, just faster once caught up.
/// </summary>
[PublicAPI]
public sealed class AzureTablesIndexProjector
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly AzureTablesEventStore _store;
    private readonly TableClient _index;
    private readonly TableClient _allIndex;
    private readonly ICheckpointStore _checkpoints;
    private readonly string _checkpointName;

    internal AzureTablesIndexProjector(
        AzureTablesEventStore store,
        TableClient index,
        TableClient allIndex,
        ICheckpointStore checkpoints,
        string checkpointName)
    {
        _store = store;
        _index = index;
        _allIndex = allIndex;
        _checkpoints = checkpoints;
        _checkpointName = checkpointName;
    }

    /// <summary>Indexes every event after the saved checkpoint, then advances it. Returns how many were indexed.</summary>
    public async Task<int> PumpAsync(CancellationToken ct = default)
    {
        var from = await _checkpoints.GetAsync(_checkpointName, ct).ConfigureAwait(false) ?? GlobalPosition.Start;
        var last = from;
        var count = 0;

        // Tail the strongly-consistent source scan (not the store's index-backed ReadAllAsync, which this
        // projector is what fills).
        await foreach (var stored in _store.ScanAllAsync(from, ct).ConfigureAwait(false))
        {
            await _index.UpsertEntityAsync(
                    TableSchema.IndexEntity(stored.StreamId, stored.StreamVersion), TableUpdateMode.Replace, ct)
                .ConfigureAwait(false);
            await _allIndex.UpsertEntityAsync(
                    TableSchema.AllIndexEntity(stored.Position, stored.StreamId, stored.StreamVersion), TableUpdateMode.Replace, ct)
                .ConfigureAwait(false);
            if (SystemEventTypes.IsSystem(stored.EventType))
                await PruneAsync(stored, ct).ConfigureAwait(false);
            last = stored.Position;
            count++;
        }

        if (count > 0)
            await _checkpoints.SetAsync(_checkpointName, last, ct).ConfigureAwait(false);
        return count;
    }

    private async Task PruneAsync(StoredEvent marker, CancellationToken ct)
    {
        switch (marker.EventType)
        {
            case SystemEventTypes.StreamTruncated:
                var truncated = JsonSerializer.Deserialize<StreamTruncatedMarker>(marker.Data.Span, Json)!;
                // Version < boundary reverses to a row key greater than the boundary's key.
                await DeleteIndexAsync(truncated.StreamId, $"RowKey gt '{TableSchema.IndexRowKey(truncated.BeforeVersion)}'", ct)
                    .ConfigureAwait(false);
                break;
            case SystemEventTypes.StreamDeleted:
                var deleted = JsonSerializer.Deserialize<StreamDeletedMarker>(marker.Data.Span, Json)!;
                await DeleteIndexAsync(deleted.StreamId, rowKeyPredicate: null, ct).ConfigureAwait(false);
                break;
        }
    }

    private async Task DeleteIndexAsync(string streamId, string? rowKeyPredicate, CancellationToken ct)
    {
        var filter = $"PartitionKey eq '{streamId.Replace("'", "''")}'"
                     + (rowKeyPredicate is null ? "" : $" and {rowKeyPredicate}");
        var stale = new List<TableEntity>();
        await foreach (var entity in _index.QueryAsync<TableEntity>(filter, cancellationToken: ct).ConfigureAwait(false))
            stale.Add(entity);

        foreach (var chunk in stale.Chunk(100))
        {
            await _index.SubmitTransactionAsync(
                    chunk.Select(e => new TableTransactionAction(TableTransactionActionType.Delete, e, e.ETag)), ct)
                .ConfigureAwait(false);
        }
    }
}
