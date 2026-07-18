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
    : IEventStore, IAllStreamReader, ITimeNavigable, IStreamMaintenance, IMultiStreamAppend, IStreamMetadataStore
{
    public StoreCapabilities Capabilities => StoreCapabilities.SnapshotHistory;

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

    public async Task<IReadOnlyList<AppendResult>> AppendToStreamsAsync(
        IReadOnlyList<StreamAppend> appends,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(appends);
        if (appends.Count == 0)
            return [];

        var seenStreams = new HashSet<string>(StringComparer.Ordinal);
        foreach (var append in appends)
        {
            ArgumentException.ThrowIfNullOrEmpty(append.StreamId);
            ArgumentNullException.ThrowIfNull(append.Events);
            if (!seenStreams.Add(append.StreamId))
                throw new InvalidOperationException(
                    $"Stream '{append.StreamId}' appears more than once in a multi-stream append.");
            var ids = new HashSet<Guid>();
            foreach (var evt in append.Events)
            {
                if (!ids.Add(evt.EventId))
                    throw new InvalidOperationException(
                        $"The append to stream '{append.StreamId}' contains EventId {evt.EventId} more than once.");
            }
        }

        // One context, one transaction: every stream's rows and head update commit together or not at all.
        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
        await using var tx = await ctx.Database.BeginTransactionAsync(ct).ConfigureAwait(false);

        var results = new AppendResult[appends.Count];
        var stagedLastRow = new EventRow?[appends.Count];
        var stagedVersion = new long[appends.Count];
        var preVersions = new long[appends.Count]; // head version seen before staging, per entry
        var anyStaged = false;

        for (var i = 0; i < appends.Count; i++)
        {
            var append = appends[i];
            var batchIds = append.Events.Select(e => e.EventId).ToHashSet();

            var existing = await ctx.Events
                .Where(e => e.Domain == domain && e.StreamId == append.StreamId && batchIds.Contains(e.EventId))
                .Select(e => new { e.StreamVersion, e.Position })
                .ToListAsync(ct).ConfigureAwait(false);

            if (append.Events.Count > 0 && existing.Count == append.Events.Count)
            {
                // Already stored — idempotent replay of this entry; stage nothing for it.
                var last = existing.MaxBy(e => e.StreamVersion)!;
                results[i] = new AppendResult(last.StreamVersion, GlobalPosition.FromSequence(last.Position));
                continue;
            }

            if (existing.Count > 0)
                throw new InvalidOperationException(
                    $"The append to stream '{append.StreamId}' partially overlaps events already stored; an append "
                    + "must be either entirely new or an exact replay of a prior batch.");

            var head = await ctx.Streams.FindAsync([domain, append.StreamId], ct).ConfigureAwait(false);
            if (head is { HardDeleted: true })
                throw new StreamDeletedException(append.StreamId);

            var current = head?.Version ?? 0;
            preVersions[i] = current;
            EnsureExpectedVersion(append.StreamId, append.ExpectedVersion, current);

            if (append.Events.Count == 0)
            {
                results[i] = new AppendResult(current, null);
                continue;
            }

            var version = current;
            EventRow? lastRow = null;
            foreach (var evt in append.Events)
            {
                version++;
                lastRow = new EventRow
                {
                    Domain = domain,
                    StreamId = append.StreamId,
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
                ctx.Streams.Add(new StreamRow { Domain = domain, StreamId = append.StreamId, Version = version });
            else
            {
                head.Version = version;
                head.IsDeleted = false;
            }

            stagedLastRow[i] = lastRow;
            stagedVersion[i] = version;
            anyStaged = true;
        }

        if (anyStaged)
        {
            try
            {
                await ctx.SaveChangesAsync(ct).ConfigureAwait(false);
                await tx.CommitAsync(ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is DbUpdateException or DbUpdateConcurrencyException)
            {
                // A concurrent writer touched one of these streams; the whole batch rolled back. Re-read
                // to tell an idempotent race (the same work already committed) from a genuine conflict.
                await tx.RollbackAsync(ct).ConfigureAwait(false);
                var reconciled = await ReconcileMultiAppendAsync(appends, preVersions, ct).ConfigureAwait(false);
                if (reconciled is null)
                    throw; // not a version race (a transient/other DB error) — surface the real cause
                return reconciled;
            }
        }
        else
        {
            await tx.CommitAsync(ct).ConfigureAwait(false); // nothing written — a clean close of the empty tx
        }

        for (var i = 0; i < appends.Count; i++)
        {
            if (stagedLastRow[i] is { } row)
                results[i] = new AppendResult(stagedVersion[i], GlobalPosition.FromSequence(row.Position));
        }

        return results;
    }

    // After an atomic multi-append fails at commit, re-read to classify — mirroring the single-stream
    // path. If every entry is satisfied (each non-empty batch fully stored, each empty entry at a
    // version that still meets its expectation) a concurrent writer committed the same work, so return
    // that idempotent outcome. Otherwise surface a concrete per-entry cause (hard delete, partial
    // overlap, or version conflict). Returns null when no entry explains the failure, meaning it was not
    // a version race but a transient/other DB error the caller should see rethrown.
    private async Task<IReadOnlyList<AppendResult>?> ReconcileMultiAppendAsync(
        IReadOnlyList<StreamAppend> appends, long[] preVersions, CancellationToken ct)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);

        var actualVersions = new long[appends.Count];
        var hardDeleted = new bool[appends.Count];
        var storedCounts = new int[appends.Count];
        var replays = new AppendResult?[appends.Count];

        for (var i = 0; i < appends.Count; i++)
        {
            var append = appends[i];
            var head = await ctx.Streams.AsNoTracking()
                .FirstOrDefaultAsync(s => s.Domain == domain && s.StreamId == append.StreamId, ct).ConfigureAwait(false);
            actualVersions[i] = head?.Version ?? 0;
            hardDeleted[i] = head?.HardDeleted ?? false;

            if (append.Events.Count == 0)
                continue;

            var batchIds = append.Events.Select(e => e.EventId).ToHashSet();
            var stored = await ctx.Events.AsNoTracking()
                .Where(e => e.Domain == domain && e.StreamId == append.StreamId && batchIds.Contains(e.EventId))
                .Select(e => new { e.StreamVersion, e.Position })
                .ToListAsync(ct).ConfigureAwait(false);
            storedCounts[i] = stored.Count;
            if (stored.Count == append.Events.Count)
            {
                var last = stored.MaxBy(e => e.StreamVersion)!;
                replays[i] = new AppendResult(last.StreamVersion, GlobalPosition.FromSequence(last.Position));
            }
        }

        // Whole-batch idempotent replay: everything a concurrent writer committed satisfies this call.
        var allSatisfied = true;
        for (var i = 0; i < appends.Count && allSatisfied; i++)
            allSatisfied = appends[i].Events.Count == 0
                ? IsExpectedVersionSatisfied(appends[i].ExpectedVersion, actualVersions[i])
                : replays[i] is not null;

        if (allSatisfied)
        {
            var results = new AppendResult[appends.Count];
            for (var i = 0; i < appends.Count; i++)
                results[i] = appends[i].Events.Count == 0
                    ? new AppendResult(actualVersions[i], null)
                    : replays[i]!;
            return results;
        }

        // Genuine failure — surface the most specific per-entry cause, matching the pre-commit taxonomy.
        for (var i = 0; i < appends.Count; i++)
        {
            var append = appends[i];
            if (hardDeleted[i])
                throw new StreamDeletedException(append.StreamId);
            if (storedCounts[i] > 0 && storedCounts[i] < append.Events.Count)
                throw new InvalidOperationException(
                    $"The append to stream '{append.StreamId}' partially overlaps events already stored; an append "
                    + "must be either entirely new or an exact replay of a prior batch.");
            if (!IsExpectedVersionSatisfied(append.ExpectedVersion, actualVersions[i]))
                throw new WrongExpectedVersionException(append.StreamId, append.ExpectedVersion, actualVersions[i]);

            // A version-agnostic entry (Any/StreamExists) whose batch never landed and whose head advanced
            // since we staged lost the version race — surface it as a conflict instead of a raw DB error,
            // just as the single-stream path does. An unchanged head means this entry did not cause the
            // rollback (another entry, or a transient error, did).
            if (append.Events.Count > 0 && storedCounts[i] == 0 && actualVersions[i] != preVersions[i])
                throw new WrongExpectedVersionException(append.StreamId, append.ExpectedVersion, actualVersions[i]);
        }

        // No entry explains the rollback → it was not a version race. Signal the caller to rethrow.
        return null;
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
            : new StreamInfo(streamId, head.Version, head.TruncatedBefore, head.IsDeleted, head.Metadata);
    }

    public async Task<StreamMetadata?> GetStreamMetadataAsync(string streamId, CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var json = await ctx.Streams.AsNoTracking()
            .Where(s => s.Domain == domain && s.StreamId == streamId && !s.HardDeleted)
            .Select(s => s.Metadata)
            .FirstOrDefaultAsync(ct).ConfigureAwait(false);
        return StreamMetadata.FromJson(json);
    }

    public async Task SetStreamMetadataAsync(string streamId, StreamMetadata metadata, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(streamId);
        ArgumentNullException.ThrowIfNull(metadata);

        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var head = await ctx.Streams.FindAsync([domain, streamId], ct).ConfigureAwait(false);
        if (head is { HardDeleted: true })
            throw new StreamDeletedException(streamId);

        var json = metadata.ToJson();
        if (head is null)
        {
            try
            {
                // Metadata may be set before a stream has events — create the head at version 0.
                ctx.Streams.Add(new StreamRow { Domain = domain, StreamId = streamId, Metadata = json });
                await ctx.SaveChangesAsync(ct).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (ex is DbUpdateException or DbUpdateConcurrencyException)
            {
                // A concurrent writer created the head first; fall through to update its metadata column.
                ctx.ChangeTracker.Clear();
            }
        }

        // Update only the metadata column, bypassing the version concurrency token — metadata is not
        // version-controlled, so a concurrent append must not make setting it fail.
        await ctx.Streams
            .Where(s => s.Domain == domain && s.StreamId == streamId)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.Metadata, json), ct).ConfigureAwait(false);
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

    public Task TruncateStreamAsync(
        string streamId,
        long beforeVersion,
        bool force = false,
        CancellationToken ct = default) =>
        ExecuteMaintenanceAsync(async (ctx, token) =>
        {
            var head = await ctx.Streams.FindAsync([domain, streamId], token).ConfigureAwait(false);
            if (head is null || head.HardDeleted)
                throw new StreamNotFoundException(streamId);

            // A truncation point past the head means "drop everything"; clamp it so the recorded boundary
            // never runs beyond the end of the stream.
            var boundary = Math.Min(beforeVersion, head.Version + 1);

            // A truncation that would remove nothing is a no-op — checked before the snapshot-coverage
            // guard, so repeating it never fails just because the covering snapshot was later pruned.
            if (boundary <= head.TruncatedBefore)
                return false;

            if (!force)
            {
                var covering = await LatestSnapshotVersionAsync(ctx, streamId, token).ConfigureAwait(false);
                if (covering < boundary - 1)
                    throw new NotSupportedByProviderException(
                        $"Truncating stream '{streamId}' before version {boundary} would leave it impossible "
                        + "to reconstruct: no snapshot at or after that boundary exists. Pass force to override.");
            }

            await ctx.Events
                .Where(e => e.Domain == domain && e.StreamId == streamId && e.StreamVersion < boundary)
                .ExecuteDeleteAsync(token).ConfigureAwait(false);
            head.TruncatedBefore = boundary;
            await EmitMarkerAsync(ctx, SystemEventTypes.StreamTruncated, new StreamTruncatedMarker(streamId, boundary), streamId, token)
                .ConfigureAwait(false);
            return true;
        }, transactional: true, ct);

    public Task DeleteStreamAsync(string streamId, DeleteMode mode, CancellationToken ct = default) =>
        ExecuteMaintenanceAsync(async (ctx, token) =>
        {
            var head = await ctx.Streams.FindAsync([domain, streamId], token).ConfigureAwait(false);
            if (head is null || head.HardDeleted)
                throw new StreamNotFoundException(streamId);

            // Deleting an already-soft-deleted stream is a no-op — don't re-emit a tombstone. A stream
            // recreated after deletion clears IsDeleted on append, so this only skips a true repeat.
            if (mode == DeleteMode.Soft && head.IsDeleted)
                return false;

            await ctx.Events.Where(e => e.Domain == domain && e.StreamId == streamId)
                .ExecuteDeleteAsync(token).ConfigureAwait(false);
            await ctx.Snapshots.Where(s => s.Domain == domain && s.StreamId == streamId)
                .ExecuteDeleteAsync(token).ConfigureAwait(false);

            head.IsDeleted = true;
            head.TruncatedBefore = head.Version + 1;
            if (mode == DeleteMode.Hard)
                head.HardDeleted = true;

            await EmitMarkerAsync(ctx, SystemEventTypes.StreamDeleted, new StreamDeletedMarker(streamId, mode), inBandStream: null, token)
                .ConfigureAwait(false);
            return true;
        }, transactional: true, ct);

    public Task RedactEventAsync(
        string streamId,
        long version,
        ReadOnlyMemory<byte> newData,
        ReadOnlyMemory<byte>? newMetadata = null,
        CancellationToken ct = default) =>
        ExecuteMaintenanceAsync(async (ctx, token) =>
        {
            var row = await ctx.Events
                .FirstOrDefaultAsync(e => e.Domain == domain && e.StreamId == streamId && e.StreamVersion == version, token)
                .ConfigureAwait(false);
            if (row is null)
                throw new StreamNotFoundException(streamId);

            row.Data = newData.ToArray();
            row.Metadata = newMetadata?.ToArray();
            await EmitMarkerAsync(ctx, SystemEventTypes.EventRedacted, new EventRedactedMarker(streamId, version), streamId, token)
                .ConfigureAwait(false);
            return true;
        }, transactional: false, ct);

    public async Task<ScavengeReport> ScavengeAsync(ScavengeOptions? options = null, CancellationToken ct = default)
    {
        var applyRetention = options?.ApplyRetention ?? true;
        long removed;
        int streams;
        var plan = new List<(string StreamId, long Boundary)>();

        await using (var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false))
        {
            var cutoff = timeProvider.GetUtcNow() - tombstoneRetention;
            removed = await ctx.Events
                .Where(e => e.Domain == domain && e.StreamId == SystemEventTypes.SystemStream && e.CreatedAt < cutoff)
                .ExecuteDeleteAsync(ct).ConfigureAwait(false);
            streams = await ctx.Streams.CountAsync(s => s.Domain == domain, ct).ConfigureAwait(false);

            if (applyRetention)
            {
                var now = timeProvider.GetUtcNow();
                var withPolicy = await ctx.Streams.AsNoTracking()
                    .Where(s => s.Domain == domain && s.Metadata != null && !s.IsDeleted && !s.HardDeleted)
                    .Select(s => new { s.StreamId, s.Version, s.TruncatedBefore, s.Metadata })
                    .ToListAsync(ct).ConfigureAwait(false);

                foreach (var row in withPolicy)
                {
                    var meta = StreamMetadata.FromJson(row.Metadata);
                    if (meta is not { HasRetention: true })
                        continue;

                    var boundary = row.TruncatedBefore;
                    if (meta.MaxCount is { } max && row.Version > max)
                        boundary = Math.Max(boundary, row.Version - max + 1);
                    if (meta.MaxAge is { } age)
                    {
                        var ageCutoff = now - age;
                        var firstKept = await ctx.Events.AsNoTracking()
                            .Where(e => e.Domain == domain && e.StreamId == row.StreamId && e.CreatedAt >= ageCutoff)
                            .Select(e => (long?)e.StreamVersion)
                            .OrderBy(v => v)
                            .FirstOrDefaultAsync(ct).ConfigureAwait(false);
                        boundary = Math.Max(boundary, firstKept ?? row.Version + 1);
                    }

                    if (boundary > row.TruncatedBefore)
                    {
                        removed += boundary - row.TruncatedBefore; // the contiguous versions being dropped
                        plan.Add((row.StreamId, boundary));
                    }
                }
            }
        }

        // Force-truncate outside the query context; each call opens its own transaction and emits the
        // retention truncation's marker so read models learn to forget. A stream deleted between planning
        // and truncating is skipped, so one race does not sink the whole pass.
        foreach (var (streamId, boundary) in plan)
        {
            try
            {
                await TruncateStreamAsync(streamId, boundary, force: true, ct).ConfigureAwait(false);
            }
            catch (StreamNotFoundException)
            {
                // The stream was deleted after it was planned; nothing to retain-truncate.
            }
        }

        return new ScavengeReport(streams, removed);
    }

    // Runs a destructive operation that also emits a system marker, retrying if a concurrent maintenance
    // op advanced the shared $system marker head between the read and the write (a unique-index or
    // concurrency-token conflict on the marker), so callers see a domain outcome, never a raw EF error.
    // The operation returns true when it made changes to persist, or false for a no-op. Destructive
    // ops that use ExecuteDelete pass transactional: true so the delete and the marker commit together.
    private async Task ExecuteMaintenanceAsync(
        Func<EventStoreDbContext, CancellationToken, Task<bool>> operation,
        bool transactional,
        CancellationToken ct)
    {
        // Marker writes contend only with other (rarer) maintenance ops on the one $system head, so a
        // smaller budget than the append path's suffices; retries exist to absorb that contention, not
        // sustained load.
        const int maxAttempts = 32;
        for (var attempt = 1; ; attempt++)
        {
            await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
            await using var tx = transactional
                ? await ctx.Database.BeginTransactionAsync(ct).ConfigureAwait(false)
                : null;

            if (!await operation(ctx, ct).ConfigureAwait(false))
                return;

            try
            {
                await ctx.SaveChangesAsync(ct).ConfigureAwait(false);
                if (tx is not null)
                    await tx.CommitAsync(ct).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (ex is DbUpdateException or DbUpdateConcurrencyException && attempt < maxAttempts)
            {
                // A concurrent maintenance op took the marker version; retry against the new head.
            }
        }
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

    private static bool IsExpectedVersionSatisfied(long expectedVersion, long currentVersion) =>
        expectedVersion switch
        {
            ExpectedVersion.Any => true,
            ExpectedVersion.NoStream => currentVersion == 0,
            ExpectedVersion.StreamExists => currentVersion > 0,
            _ => expectedVersion == currentVersion
        };

    private static void EnsureExpectedVersion(string streamId, long expectedVersion, long currentVersion)
    {
        if (!IsExpectedVersionSatisfied(expectedVersion, currentVersion))
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
