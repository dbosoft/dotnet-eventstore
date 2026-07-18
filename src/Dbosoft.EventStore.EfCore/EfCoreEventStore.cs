using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;

namespace Dbosoft.EventStore.EfCore;

/// <summary>
/// The EF Core event store for one domain. It holds no context — every operation opens a fresh
/// context from the factory and disposes it, so the store is safe to share and never keeps a global
/// context. Appending is race-free via the unique index on (domain, stream, version): a lost race is
/// a unique violation, translated to a concurrency conflict. A registration may add a version
/// concurrency token for tidier conflict detection.
/// </summary>
internal sealed class EfCoreEventStore(
    IDbContextFactory<EventStoreDbContext> factory,
    string domain,
    TimeProvider timeProvider,
    bool inBandMarkers,
    TimeSpan tombstoneRetention)
    : IEventStore, IAllStreamReader, ITimeNavigable, IStreamMaintenance
{
    public StoreCapabilities Capabilities =>
        StoreCapabilities.AllStreamReads
        | StoreCapabilities.TimeNavigation
        | StoreCapabilities.Truncation
        | StoreCapabilities.StreamDeletion
        | StoreCapabilities.SnapshotHistory
        | StoreCapabilities.Redaction;

    public async Task<AppendResult> AppendToStreamAsync(
        string streamId,
        long expectedVersion,
        IReadOnlyList<EventData> events,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(streamId);
        ArgumentNullException.ThrowIfNull(events);

        var batchIds = new HashSet<Guid>();
        foreach (var evt in events)
        {
            if (!batchIds.Add(evt.EventId))
                throw new InvalidOperationException(
                    $"The append to stream '{streamId}' contains EventId {evt.EventId} more than once.");
        }

        if (events.Count == 0)
        {
            // An empty append writes nothing, so it never materializes a stream or clears a soft-
            // deletion — but it is still a concurrency-checked no-op, honouring the expected version.
            await using var peek = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
            var peekHead = await peek.Streams.FindAsync([domain, streamId], ct).ConfigureAwait(false);
            if (peekHead is { HardDeleted: true })
                throw new StreamDeletedException(streamId);
            var peekVersion = peekHead?.Version ?? 0;
            EnsureExpectedVersion(streamId, expectedVersion, peekVersion);
            return new AppendResult(peekVersion, null);
        }

        // A lost race is detected at commit as a unique-index violation and classified by re-reading.
        // With ExpectedVersion.Any (or a satisfied StreamExists) the caller does not care which version
        // it lands on, so a version conflict is retried against the new head rather than surfaced.
        const int maxAttempts = 64;
        for (var attempt = 1; ; attempt++)
        {
            await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);

            var existing = await ctx.Events
                .Where(e => e.Domain == domain && e.StreamId == streamId && batchIds.Contains(e.EventId))
                .Select(e => new { e.StreamVersion, e.Position })
                .ToListAsync(ct).ConfigureAwait(false);

            if (existing.Count == events.Count)
            {
                // Exact replay of an already-stored batch: return the outcome of the original append.
                var last = existing.MaxBy(e => e.StreamVersion)!;
                return new AppendResult(last.StreamVersion, GlobalPosition.FromSequence(last.Position));
            }

            if (existing.Count > 0)
                throw new InvalidOperationException(
                    $"The append to stream '{streamId}' partially overlaps events already stored; an append "
                    + "must be either entirely new or an exact replay of a prior batch.");

            var head = await ctx.Streams.FindAsync([domain, streamId], ct).ConfigureAwait(false);
            if (head is { HardDeleted: true })
                throw new StreamDeletedException(streamId);

            var current = head?.Version ?? 0;
            EnsureExpectedVersion(streamId, expectedVersion, current);

            var version = current;
            EventRow? lastRow = null;
            foreach (var evt in events)
            {
                version++;
                lastRow = new EventRow
                {
                    Domain = domain,
                    StreamId = streamId,
                    StreamVersion = version,
                    EventId = evt.EventId,
                    EventType = evt.EventType,
                    Data = evt.Data.ToArray(),
                    Metadata = evt.Metadata?.ToArray(),
                    ContentType = evt.ContentType,
                    CreatedAt = timeProvider.GetUtcNow()
                };
                ctx.Events.Add(lastRow);
            }

            if (head is null)
                ctx.Streams.Add(new StreamRow { Domain = domain, StreamId = streamId, Version = version });
            else
            {
                head.Version = version;
                head.IsDeleted = false;
            }

            try
            {
                await ctx.SaveChangesAsync(ct).ConfigureAwait(false);
                return new AppendResult(version, GlobalPosition.FromSequence(lastRow!.Position));
            }
            catch (Exception ex) when (ex is DbUpdateConcurrencyException or DbUpdateException)
            {
                // The save failed. Rather than depend on provider-specific error codes to tell a unique-
                // index violation from anything else, re-read and classify by the resulting state.
                await using var check = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
                var stored = await check.Events
                    .Where(e => e.Domain == domain && e.StreamId == streamId && batchIds.Contains(e.EventId))
                    .Select(e => new { e.StreamVersion, e.Position })
                    .ToListAsync(ct).ConfigureAwait(false);

                if (stored.Count == events.Count)
                {
                    // A concurrent writer stored this exact batch first: idempotent replay.
                    var last = stored.MaxBy(e => e.StreamVersion)!;
                    return new AppendResult(last.StreamVersion, GlobalPosition.FromSequence(last.Position));
                }

                var actual = await check.Streams.AsNoTracking()
                    .Where(s => s.Domain == domain && s.StreamId == streamId)
                    .Select(s => (long?)s.Version)
                    .FirstOrDefaultAsync(ct).ConfigureAwait(false) ?? 0;
                if (stored.Count == 0 && actual == current)
                    throw; // nothing changed and the batch is absent — not a concurrency conflict

                // A genuine version conflict. Retry against the new head when the caller did not pin a
                // specific version; otherwise report it.
                if (IsVersionAgnostic(expectedVersion, actual) && attempt < maxAttempts)
                    continue;

                throw new WrongExpectedVersionException(streamId, expectedVersion, actual);
            }
        }
    }

    // Whether the caller left the target version open, so a lost race can be retried rather than failed.
    private static bool IsVersionAgnostic(long expectedVersion, long actualVersion) =>
        expectedVersion == ExpectedVersion.Any
        || (expectedVersion == ExpectedVersion.StreamExists && actualVersion > 0);

    public async IAsyncEnumerable<StoredEvent> ReadStreamAsync(
        string streamId,
        ReadDirection direction = ReadDirection.Forwards,
        long fromVersion = 0,
        long maxCount = long.MaxValue,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);

        var query = ctx.Events.AsNoTracking().Where(e => e.Domain == domain && e.StreamId == streamId);
        query = direction == ReadDirection.Forwards
            ? query.Where(e => e.StreamVersion >= Math.Max(1, fromVersion)).OrderBy(e => e.StreamVersion)
            : query.Where(e => e.StreamVersion <= (fromVersion <= 0 ? long.MaxValue : fromVersion))
                .OrderByDescending(e => e.StreamVersion);

        if (maxCount < int.MaxValue)
            query = query.Take((int)Math.Max(0, maxCount));

        await foreach (var row in query.AsAsyncEnumerable().WithCancellation(ct).ConfigureAwait(false))
            yield return ToStored(row);
    }

    public async Task<StreamInfo?> GetStreamInfoAsync(string streamId, CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var head = await ctx.Streams.AsNoTracking()
            .FirstOrDefaultAsync(s => s.Domain == domain && s.StreamId == streamId, ct).ConfigureAwait(false);

        return head is null || head.HardDeleted
            ? null
            : new StreamInfo(streamId, head.Version, head.TruncatedBefore, head.IsDeleted, Metadata: null);
    }

    public async IAsyncEnumerable<StoredEvent> ReadAllAsync(
        GlobalPosition from,
        ReadDirection direction = ReadDirection.Forwards,
        long maxCount = long.MaxValue,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var fromSequence = ToSequence(from);

        var query = ctx.Events.AsNoTracking().Where(e => e.Domain == domain);
        query = direction == ReadDirection.Forwards
            ? query.Where(e => e.Position > fromSequence).OrderBy(e => e.Position)
            : query.Where(e => e.Position < fromSequence).OrderByDescending(e => e.Position);

        if (maxCount < int.MaxValue)
            query = query.Take((int)Math.Max(0, maxCount));

        await foreach (var row in query.AsAsyncEnumerable().WithCancellation(ct).ConfigureAwait(false))
            yield return ToStored(row);
    }

    public async Task<GlobalPosition> FindPositionAtAsync(DateTimeOffset timestamp, CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var before = await ctx.Events.AsNoTracking()
            .Where(e => e.Domain == domain && e.CreatedAt < timestamp)
            .OrderByDescending(e => e.Position)
            .Select(e => (long?)e.Position)
            .FirstOrDefaultAsync(ct).ConfigureAwait(false);

        return before is { } sequence ? GlobalPosition.FromSequence(sequence) : GlobalPosition.Start;
    }

    public async Task TruncateStreamAsync(
        string streamId,
        long beforeVersion,
        bool force = false,
        CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var head = await ctx.Streams.FindAsync([domain, streamId], ct).ConfigureAwait(false);
        if (head is null || head.HardDeleted)
            throw new StreamNotFoundException(streamId);

        // A truncation point past the head means "drop everything"; clamp it so the recorded boundary
        // never runs beyond the end of the stream.
        beforeVersion = Math.Min(beforeVersion, head.Version + 1);

        // A truncation that would remove nothing is a no-op — checked before the snapshot-coverage guard,
        // so repeating it never fails just because the covering snapshot was later pruned.
        if (beforeVersion <= head.TruncatedBefore)
            return;

        if (!force)
        {
            var covering = await LatestSnapshotVersionAsync(ctx, streamId, ct).ConfigureAwait(false);
            if (covering < beforeVersion - 1)
                throw new NotSupportedByProviderException(
                    $"Truncating stream '{streamId}' before version {beforeVersion} would leave it impossible "
                    + "to reconstruct: no snapshot at or after that boundary exists. Pass force to override.");
        }

        // The physical delete and the head/marker update must be all-or-nothing: without a wrapping
        // transaction, ExecuteDelete commits on its own and a later failure would leave events gone
        // with no tombstone — the exact state the marker exists to prevent.
        await using var tx = await ctx.Database.BeginTransactionAsync(ct).ConfigureAwait(false);
        await ctx.Events
            .Where(e => e.Domain == domain && e.StreamId == streamId && e.StreamVersion < beforeVersion)
            .ExecuteDeleteAsync(ct).ConfigureAwait(false);

        head.TruncatedBefore = beforeVersion;
        await EmitMarkerAsync(ctx, SystemEventTypes.StreamTruncated, new StreamTruncatedMarker(streamId, beforeVersion), streamId, ct)
            .ConfigureAwait(false);
        await ctx.SaveChangesAsync(ct).ConfigureAwait(false);
        await tx.CommitAsync(ct).ConfigureAwait(false);
    }

    public async Task DeleteStreamAsync(string streamId, DeleteMode mode, CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var head = await ctx.Streams.FindAsync([domain, streamId], ct).ConfigureAwait(false);
        if (head is null || head.HardDeleted)
            throw new StreamNotFoundException(streamId);

        // Deleting an already-soft-deleted stream is a no-op — don't re-emit a tombstone. A stream
        // recreated after deletion clears IsDeleted on append, so this only skips a true repeat.
        if (mode == DeleteMode.Soft && head.IsDeleted)
            return;

        // All-or-nothing, as with truncation: the physical deletes and the head/marker update must
        // commit together, or a mid-op failure would strand rows with no tombstone.
        await using var tx = await ctx.Database.BeginTransactionAsync(ct).ConfigureAwait(false);
        await ctx.Events.Where(e => e.Domain == domain && e.StreamId == streamId)
            .ExecuteDeleteAsync(ct).ConfigureAwait(false);
        await ctx.Snapshots.Where(s => s.Domain == domain && s.StreamId == streamId)
            .ExecuteDeleteAsync(ct).ConfigureAwait(false);

        head.IsDeleted = true;
        head.TruncatedBefore = head.Version + 1;
        if (mode == DeleteMode.Hard)
            head.HardDeleted = true;

        await EmitMarkerAsync(ctx, SystemEventTypes.StreamDeleted, new StreamDeletedMarker(streamId, mode), inBandStream: null, ct)
            .ConfigureAwait(false);
        await ctx.SaveChangesAsync(ct).ConfigureAwait(false);
        await tx.CommitAsync(ct).ConfigureAwait(false);
    }

    public async Task RedactEventAsync(
        string streamId,
        long version,
        ReadOnlyMemory<byte> newData,
        ReadOnlyMemory<byte>? newMetadata = null,
        CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var row = await ctx.Events
            .FirstOrDefaultAsync(e => e.Domain == domain && e.StreamId == streamId && e.StreamVersion == version, ct)
            .ConfigureAwait(false);
        if (row is null)
            throw new StreamNotFoundException(streamId);

        row.Data = newData.ToArray();
        row.Metadata = newMetadata?.ToArray();
        await EmitMarkerAsync(ctx, SystemEventTypes.EventRedacted, new EventRedactedMarker(streamId, version), streamId, ct)
            .ConfigureAwait(false);
        await ctx.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    public async Task<ScavengeReport> ScavengeAsync(ScavengeOptions? options = null, CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var cutoff = timeProvider.GetUtcNow() - tombstoneRetention;

        var removed = await ctx.Events
            .Where(e => e.Domain == domain && e.StreamId == SystemEventTypes.SystemStream && e.CreatedAt < cutoff)
            .ExecuteDeleteAsync(ct).ConfigureAwait(false);

        var streams = await ctx.Streams.CountAsync(s => s.Domain == domain, ct).ConfigureAwait(false);
        return new ScavengeReport(streams, removed);
    }

    private async Task EmitMarkerAsync(
        EventStoreDbContext ctx,
        string eventType,
        object marker,
        string? inBandStream,
        CancellationToken ct)
    {
        var streamId = inBandMarkers && inBandStream is not null ? inBandStream : SystemEventTypes.SystemStream;
        var head = ctx.Streams.Local.FirstOrDefault(s => s.Domain == domain && s.StreamId == streamId)
                   ?? await ctx.Streams.FindAsync([domain, streamId], ct).ConfigureAwait(false);
        if (head is null)
        {
            head = new StreamRow { Domain = domain, StreamId = streamId };
            ctx.Streams.Add(head);
        }

        head.Version++;
        ctx.Events.Add(new EventRow
        {
            Domain = domain,
            StreamId = streamId,
            StreamVersion = head.Version,
            EventId = Guid.NewGuid(),
            EventType = eventType,
            Data = JsonSerializer.SerializeToUtf8Bytes(marker, marker.GetType()),
            ContentType = "application/json",
            CreatedAt = timeProvider.GetUtcNow()
        });
    }

    private async Task<long> LatestSnapshotVersionAsync(EventStoreDbContext ctx, string streamId, CancellationToken ct) =>
        await ctx.Snapshots.AsNoTracking()
            .Where(s => s.Domain == domain && s.StreamId == streamId)
            .Select(s => (long?)s.StreamVersion)
            .OrderByDescending(v => v)
            .FirstOrDefaultAsync(ct).ConfigureAwait(false) ?? 0;

    private static void EnsureExpectedVersion(string streamId, long expectedVersion, long currentVersion)
    {
        var satisfied = expectedVersion switch
        {
            ExpectedVersion.Any => true,
            ExpectedVersion.NoStream => currentVersion == 0,
            ExpectedVersion.StreamExists => currentVersion > 0,
            _ => expectedVersion == currentVersion
        };

        if (!satisfied)
            throw new WrongExpectedVersionException(streamId, expectedVersion, currentVersion);
    }

    private static StoredEvent ToStored(EventRow row) =>
        new(row.StreamId, row.StreamVersion, GlobalPosition.FromSequence(row.Position), row.EventId,
            row.EventType, row.Data, row.Metadata, row.ContentType, row.CreatedAt);

    private static long ToSequence(GlobalPosition position)
    {
        // A defaulted position has a null token; treat it (and Start) as "before the first event".
        if (position == GlobalPosition.Start || string.IsNullOrEmpty(position.Token))
            return 0;
        return position == GlobalPosition.End
            ? long.MaxValue
            : long.Parse(position.Token, CultureInfo.InvariantCulture);
    }
}
