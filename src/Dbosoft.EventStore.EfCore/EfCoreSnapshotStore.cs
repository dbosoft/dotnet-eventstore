using Microsoft.EntityFrameworkCore;

namespace Dbosoft.EventStore.EfCore;

/// <summary>Snapshot store for one domain, keeping a version history. Opens a context per operation.</summary>
internal sealed class EfCoreSnapshotStore(
    IDbContextFactory<EventStoreDbContext> factory,
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
        // Find-then-upsert. Two writers crossing the snapshot boundary for the same (stream, version)
        // concurrently would collide on the primary key; retry, where the loser updates instead (last
        // wins — a snapshot is disposable derived data, so either payload is fine).
        const int maxAttempts = 3;
        for (var attempt = 1; ; attempt++)
        {
            await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
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

            try
            {
                await ctx.SaveChangesAsync(ct).ConfigureAwait(false);
                return;
            }
            catch (DbUpdateException) when (attempt < maxAttempts)
            {
                // A concurrent writer inserted the row first — retry as an update.
            }
        }
    }

    public Task<StoredSnapshot?> LoadLatestAsync(string streamId, CancellationToken ct = default) =>
        LoadAtOrBeforeAsync(streamId, long.MaxValue, ct);

    public async Task<StoredSnapshot?> LoadAtOrBeforeAsync(string streamId, long maxVersion, CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var row = await ctx.Snapshots.AsNoTracking()
            .Where(s => s.Domain == domain && s.StreamId == streamId && s.StreamVersion <= maxVersion)
            .OrderByDescending(s => s.StreamVersion)
            .FirstOrDefaultAsync(ct).ConfigureAwait(false);

        return row is null
            ? null
            : new StoredSnapshot(row.StreamId, row.StreamVersion, row.Payload, row.ContentType, row.CreatedAt);
    }

    public async Task PruneAsync(string streamId, int keep, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(keep);
        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
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
    }
}
