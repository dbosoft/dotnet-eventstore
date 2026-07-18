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
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _safetyWindow;

    internal AzureTablesIndexProjector(
        AzureTablesEventStore store,
        TableClient index,
        TableClient allIndex,
        ICheckpointStore checkpoints,
        string checkpointName,
        TimeProvider timeProvider,
        TimeSpan safetyWindow)
    {
        _store = store;
        _index = index;
        _allIndex = allIndex;
        _checkpoints = checkpoints;
        _checkpointName = checkpointName;
        _timeProvider = timeProvider;
        _safetyWindow = safetyWindow;
    }

    /// <summary>
    /// Indexes events after the saved checkpoint that are older than the safety window, then advances the
    /// checkpoint. Returns how many were indexed. A position is allocated before the append commits, so an
    /// event whose position is newer than the horizon may have a not-yet-committed lower sibling; holding
    /// it (and not advancing past it) until it ages out of the window prevents that sibling from being
    /// skipped permanently. Events newer than the horizon are left for a later pump.
    /// </summary>
    public Task<int> PumpAsync(CancellationToken ct = default) => PumpCoreAsync(applySafetyWindow: true, ct);

    /// <summary>
    /// Indexes everything currently available, ignoring the safety window. Only safe when no append is in
    /// flight (a controlled drain, e.g. tests or shutdown after writers have stopped); otherwise use
    /// <see cref="PumpAsync"/>.
    /// </summary>
    public Task<int> DrainAsync(CancellationToken ct = default) => PumpCoreAsync(applySafetyWindow: false, ct);

    private async Task<int> PumpCoreAsync(bool applySafetyWindow, CancellationToken ct)
    {
        var from = await _checkpoints.GetAsync(_checkpointName, ct).ConfigureAwait(false) ?? GlobalPosition.Start;
        var horizon = applySafetyWindow
            ? GlobalPosition.FromSequence(Math.Max(0, (_timeProvider.GetUtcNow() - _safetyWindow).UtcTicks))
            : GlobalPosition.End;
        var last = from;
        var count = 0;

        // Tail the strongly-consistent source scan (not the store's index-backed ReadAllAsync, which this
        // projector is what fills). The scan yields in ascending position order, so the first event past the
        // horizon means every later one is too — stop and leave them for a later pump.
        await foreach (var stored in _store.ScanAllAsync(from, ct).ConfigureAwait(false))
        {
            if (stored.Position > horizon)
                break;

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
