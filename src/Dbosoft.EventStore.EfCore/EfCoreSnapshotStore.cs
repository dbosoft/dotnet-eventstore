using Microsoft.EntityFrameworkCore;

namespace Dbosoft.EventStore.EfCore;

/// <summary>
/// Snapshot store for one domain, keeping a version history. Like the event store, it runs on the caller's
/// borrowed context when a unit-of-work frame is ambient — so a snapshot commits, or rolls back, together
/// with the events it summarises (never stranded ahead of a stream whose append was rolled back) and shares
/// the one connection instead of self-locking against the frame's pending write. Without a frame it opens a
/// context per operation.
/// </summary>
internal sealed class EfCoreSnapshotStore(
    EventStoreContextProvider context,
    string domain,
    TimeProvider timeProvider) : ISnapshotStore
{
    public async Task SaveAsync(
        string streamId,
        long streamVersion,
        ReadOnlyMemory<byte> payload,
        string contentType = "application/json",
        CancellationToken ct = default)
    {
        // Under a frame the snapshot joins the caller's transaction: it commits — or rolls back — with the
        // events it summarises, and a conflict propagates so the whole frame rolls back (no retry against a
        // doomed transaction). The version is the one the enlisted append just claimed, so no other writer can
        // hold the same (stream, version) within a committed frame — there is nothing to race.
        if (context.Enlisted is { } enlisted)
        {
            await UpsertAsync(enlisted, streamId, streamVersion, payload, contentType, ct).ConfigureAwait(false);
            await enlisted.SaveEnlistedChangesAsync(ct).ConfigureAwait(false);
            return;
        }

        // Autonomous find-then-upsert. Two writers crossing the snapshot boundary for the same (stream,
        // version) concurrently would collide on the primary key; retry, where the loser updates instead
        // (last wins — a snapshot is disposable derived data, so either payload is fine).
        const int maxAttempts = 3;
        for (var attempt = 1; ; attempt++)
        {
            await using var lease = await context.LeaseAsync(ct).ConfigureAwait(false);
            await UpsertAsync(lease.Context, streamId, streamVersion, payload, contentType, ct).ConfigureAwait(false);
            try
            {
                await lease.Context.SaveChangesAsync(ct).ConfigureAwait(false);
                return;
            }
            catch (DbUpdateException) when (attempt < maxAttempts)
            {
                // A concurrent writer inserted the row first — retry as an update.
            }
        }
    }

    private async Task UpsertAsync(
        EventStoreDbContext ctx,
        string streamId,
        long streamVersion,
        ReadOnlyMemory<byte> payload,
        string contentType,
        CancellationToken ct)
    {
        var existing = await ctx.Snapshots.FindAsync([domain, streamId, streamVersion], ct).ConfigureAwait(false);
        if (existing is null)
        {
            ctx.Snapshots.Add(new SnapshotRow
            {
                Domain = domain,
                StreamId = streamId,
                StreamVersion = streamVersion,
                Payload = payload.ToArray(),
                ContentType = contentType,
                CreatedAt = timeProvider.GetUtcNow()
            });
        }
        else
        {
            existing.Payload = payload.ToArray();
            existing.ContentType = contentType;
            existing.CreatedAt = timeProvider.GetUtcNow();
        }
    }

    public Task<StoredSnapshot?> LoadLatestAsync(string streamId, CancellationToken ct = default) =>
        LoadAtOrBeforeAsync(streamId, long.MaxValue, ct);

    public Task<StoredSnapshot?> LoadAtOrBeforeAsync(string streamId, long maxVersion, CancellationToken ct = default) =>
        context.ExecuteAsync<StoredSnapshot?>(async ctx =>
        {
            var row = await ctx.Snapshots.AsNoTracking()
                .Where(s => s.Domain == domain && s.StreamId == streamId && s.StreamVersion <= maxVersion)
                .OrderByDescending(s => s.StreamVersion)
                .FirstOrDefaultAsync(ct).ConfigureAwait(false);

            return row is null
                ? null
                : new StoredSnapshot(row.StreamId, row.StreamVersion, row.Payload, row.ContentType, row.CreatedAt);
        }, ct);

    public Task PruneAsync(string streamId, int keep, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(keep);
        return context.ExecuteAsync(async ctx =>
        {
            var stale = await ctx.Snapshots
                .Where(s => s.Domain == domain && s.StreamId == streamId)
                .OrderByDescending(s => s.StreamVersion)
                .Skip(keep)
                .Select(s => s.StreamVersion)
                .ToListAsync(ct).ConfigureAwait(false);

            if (stale.Count == 0)
                return;

            await ctx.Snapshots
                .Where(s => s.Domain == domain && s.StreamId == streamId && stale.Contains(s.StreamVersion))
                .ExecuteDeleteAsync(ct).ConfigureAwait(false);
        }, ct);
    }
}
