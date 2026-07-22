using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;

namespace Dbosoft.EventStore.EfCore;

/// <summary>
/// The EF Core event store for one domain. It holds no context — every operation leases one from the
/// <see cref="EventStoreContextProvider"/> (the caller's borrowed context under a unit-of-work frame, else a
/// fresh one) and releases it, so the store is safe to share and never keeps a global context. Appending is
/// race-free via the unique index on (domain, stream, version): a lost race is a unique violation, translated
/// to a concurrency conflict. A registration may add a version concurrency token for tidier conflict detection.
/// </summary>
internal sealed class EfCoreEventStore(
    EventStoreContextProvider context,
    string domain,
    TimeProvider timeProvider,
    bool inBandMarkers,
    TimeSpan tombstoneRetention,
    IChangeNotifier? changeNotifier = null)
    : EventStoreBase(changeNotifier),
        IAllStreamReader, ITimeNavigable, IStreamMaintenance, IMultiStreamAppend, IStreamMetadataStore,
        IEnlistableStore
{
    protected override string NotifyDomain => domain;

    public override StoreCapabilities Capabilities => StoreCapabilities.SnapshotHistory;

    protected override async Task<AppendResult> AppendToStreamCoreAsync(
        string streamId,
        long expectedVersion,
        IReadOnlyList<EventData> events,
        CancellationToken ct)
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

        // Under an ambient unit of work the append joins the caller's transaction and neither retries nor
        // reclassifies in place (a failed save dooms that transaction); a conflict propagates and the whole
        // frame rolls back. Without one, it owns its context and transaction and retries as before.
        if (context.Enlisted is { } enlisted)
            return await AppendEnlistedAsync(enlisted, streamId, expectedVersion, events, batchIds, ct)
                .ConfigureAwait(false);
        return await AppendAutonomousAsync(streamId, expectedVersion, events, batchIds, ct).ConfigureAwait(false);
    }

    private async Task<AppendResult> AppendEnlistedAsync(
        EventStoreDbContext ctx,
        string streamId,
        long expectedVersion,
        IReadOnlyList<EventData> events,
        HashSet<Guid> batchIds,
        CancellationToken ct)
    {
        var outcome = await StageAppendAsync(ctx, streamId, expectedVersion, events, batchIds, ct).ConfigureAwait(false);
        if (outcome.Completed is { } completed)
            return completed;

        // A conflict surfaces as a DbUpdateException and is left to propagate: the caller's frame rolls back
        // and retries as a whole, so there is no in-place reclassification against the frame's own uncommitted
        // (and, after a failed save, doomed) transaction.
        await ctx.SaveEnlistedChangesAsync(ct).ConfigureAwait(false);
        return new AppendResult(outcome.StagedVersion, GlobalPosition.FromSequence(outcome.StagedLastRow!.Position));
    }

    private async Task<AppendResult> AppendAutonomousAsync(
        string streamId,
        long expectedVersion,
        IReadOnlyList<EventData> events,
        HashSet<Guid> batchIds,
        CancellationToken ct)
    {
        // A lost race is detected at commit as a unique-index violation and classified by re-reading.
        // With ExpectedVersion.Any (or a satisfied StreamExists) the caller does not care which version
        // it lands on, so a version conflict is retried against the new head rather than surfaced.
        const int maxAttempts = 64;
        for (var attempt = 1; ; attempt++)
        {
            await using var lease = await context.LeaseAsync(ct).ConfigureAwait(false);
            var ctx = lease.Context;

            var outcome = await StageAppendAsync(ctx, streamId, expectedVersion, events, batchIds, ct).ConfigureAwait(false);
            if (outcome.Completed is { } completed)
                return completed;

            try
            {
                await ctx.SaveChangesAsync(ct).ConfigureAwait(false);
                return new AppendResult(outcome.StagedVersion, GlobalPosition.FromSequence(outcome.StagedLastRow!.Position));
            }
            catch (Exception ex) when (ex is DbUpdateConcurrencyException or DbUpdateException)
            {
                // The save failed. Rather than depend on provider-specific error codes to tell a unique-
                // index violation from anything else, re-read and classify by the resulting state.
                await using var checkLease = await context.LeaseAsync(ct).ConfigureAwait(false);
                var check = checkLease.Context;

                // Read the head version before the batch (see StageAppendAsync): committed data is visible to
                // every later query on the connection, so if the stream has advanced the batch read below is
                // guaranteed to see the winner's events. This keeps a concurrent idempotent replay from being
                // misread as a version conflict when the winner commits between the reads.
                var actual = await check.Streams.AsNoTracking()
                    .Where(s => s.Domain == domain && s.StreamId == streamId)
                    .Select(s => (long?)s.Version)
                    .FirstOrDefaultAsync(ct).ConfigureAwait(false) ?? 0;

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

                if (stored.Count == 0 && actual == outcome.CurrentVersion)
                    throw; // nothing changed and the batch is absent — not a concurrency conflict

                // A genuine version conflict. Retry against the new head when the caller did not pin a
                // specific version; otherwise report it.
                if (IsVersionAgnostic(expectedVersion, actual) && attempt < maxAttempts)
                    continue;

                throw new WrongExpectedVersionException(streamId, expectedVersion, actual);
            }
        }
    }

    // Reads the stream head and stages an append into the given context WITHOUT saving — the shared core of
    // every append path. Returns a completed result when there is nothing to stage (an exact idempotent
    // replay of an already-stored batch, or an empty but concurrency-checked append); otherwise the staged
    // last row and new version. The caller saves, and — autonomously — retries and reclassifies a failed save.
    private async Task<StageOutcome> StageAppendAsync(
        EventStoreDbContext ctx,
        string streamId,
        long expectedVersion,
        IReadOnlyList<EventData> events,
        HashSet<Guid> batchIds,
        CancellationToken ct)
    {
        // Read the stream head before the batch. Under READ COMMITTED each query sees its own snapshot, and
        // committed data stays visible across successive queries on one connection. Reading the head first
        // means that if a concurrent writer has advanced the stream, the batch read that follows is guaranteed
        // to see that writer's events too — so an identical batch is recognized as an idempotent replay here
        // rather than tripping the expected-version check when a commit lands between the two reads.
        var head = await ctx.Streams.FindAsync([domain, streamId], ct).ConfigureAwait(false);
        if (head is { HardDeleted: true })
            throw new StreamDeletedException(streamId);
        var current = head?.Version ?? 0;

        if (events.Count > 0)
        {
            var existing = await ctx.Events
                .Where(e => e.Domain == domain && e.StreamId == streamId && batchIds.Contains(e.EventId))
                .Select(e => new { e.StreamVersion, e.Position })
                .ToListAsync(ct).ConfigureAwait(false);

            if (existing.Count == events.Count)
            {
                // Exact replay of an already-stored batch: return the outcome of the original append.
                var last = existing.MaxBy(e => e.StreamVersion)!;
                return StageOutcome.Done(
                    new AppendResult(last.StreamVersion, GlobalPosition.FromSequence(last.Position)), current);
            }

            if (existing.Count > 0)
                throw new InvalidOperationException(
                    $"The append to stream '{streamId}' partially overlaps events already stored; an append "
                    + "must be either entirely new or an exact replay of a prior batch.");
        }

        EnsureExpectedVersion(streamId, expectedVersion, current);

        // An empty append writes nothing, so it never materializes a stream or clears a soft-deletion — but
        // it is still a concurrency-checked no-op, honouring the expected version.
        if (events.Count == 0)
            return StageOutcome.Done(new AppendResult(current, null), current);

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

        return StageOutcome.Staged(lastRow!, version, current);
    }

    // The result of staging one append: either a completed result (nothing left to save) or the staged last
    // row and its new version. CurrentVersion is the head version read before staging — the autonomous path
    // needs it to tell "nothing changed" from a genuine conflict when reclassifying a failed save.
    private readonly record struct StageOutcome(
        AppendResult? Completed,
        EventRow? StagedLastRow,
        long StagedVersion,
        long CurrentVersion)
    {
        public static StageOutcome Done(AppendResult result, long currentVersion) =>
            new(result, null, 0, currentVersion);

        public static StageOutcome Staged(EventRow lastRow, long version, long currentVersion) =>
            new(null, lastRow, version, currentVersion);
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

        // Under an ambient unit of work the batch stages onto the caller's transaction and commits with it;
        // otherwise it owns one context and transaction so every stream's rows commit together or not at all.
        if (context.Enlisted is { } enlisted)
            return await AppendToStreamsEnlistedAsync(enlisted, appends, ct).ConfigureAwait(false);
        return await AppendToStreamsAutonomousAsync(appends, ct).ConfigureAwait(false);
    }

    private async Task<IReadOnlyList<AppendResult>> AppendToStreamsAutonomousAsync(
        IReadOnlyList<StreamAppend> appends,
        CancellationToken ct)
    {
        // One context, one transaction: every stream's rows and head update commit together or not at all.
        await using var lease = await context.LeaseAsync(ct).ConfigureAwait(false);
        var ctx = lease.Context;
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
            var outcome = await StageAppendAsync(ctx, append.StreamId, append.ExpectedVersion, append.Events, batchIds, ct)
                .ConfigureAwait(false);
            preVersions[i] = outcome.CurrentVersion;
            if (outcome.Completed is { } completed)
            {
                results[i] = completed;
                continue;
            }
            stagedLastRow[i] = outcome.StagedLastRow;
            stagedVersion[i] = outcome.StagedVersion;
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

        return BuildMultiAppendResults(appends, results, stagedLastRow, stagedVersion);
    }

    private async Task<IReadOnlyList<AppendResult>> AppendToStreamsEnlistedAsync(
        EventStoreDbContext ctx,
        IReadOnlyList<StreamAppend> appends,
        CancellationToken ct)
    {
        var results = new AppendResult[appends.Count];
        var stagedLastRow = new EventRow?[appends.Count];
        var stagedVersion = new long[appends.Count];
        var anyStaged = false;

        for (var i = 0; i < appends.Count; i++)
        {
            var append = appends[i];
            var batchIds = append.Events.Select(e => e.EventId).ToHashSet();
            var outcome = await StageAppendAsync(ctx, append.StreamId, append.ExpectedVersion, append.Events, batchIds, ct)
                .ConfigureAwait(false);
            if (outcome.Completed is { } completed)
            {
                results[i] = completed;
                continue;
            }
            stagedLastRow[i] = outcome.StagedLastRow;
            stagedVersion[i] = outcome.StagedVersion;
            anyStaged = true;
        }

        // A conflict propagates (the frame rolls back and the caller retries as a whole); there is no in-place
        // reconciliation against the frame's own uncommitted, and after a failed save doomed, transaction.
        if (anyStaged)
            await ctx.SaveEnlistedChangesAsync(ct).ConfigureAwait(false);

        return BuildMultiAppendResults(appends, results, stagedLastRow, stagedVersion);
    }

    // Fills in the positions of staged entries (known only after save) and signals each stream that actually
    // wrote — a pure idempotent replay staged nothing and is left untouched. Under a frame the signal is
    // deferred to commit (see EventStoreBase.NotifyChanged).
    private IReadOnlyList<AppendResult> BuildMultiAppendResults(
        IReadOnlyList<StreamAppend> appends,
        AppendResult[] results,
        EventRow?[] stagedLastRow,
        long[] stagedVersion)
    {
        var anyStaged = false;
        for (var i = 0; i < appends.Count; i++)
        {
            if (stagedLastRow[i] is not { } row)
                continue;
            results[i] = new AppendResult(stagedVersion[i], GlobalPosition.FromSequence(row.Position));
            anyStaged = true;
        }

        if (anyStaged)
            foreach (var append in appends)
                if (append.Events.Count > 0)
                    NotifyChanged(append.StreamId);
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
        await using var lease = await context.LeaseAsync(ct).ConfigureAwait(false);
        var ctx = lease.Context;

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

    private IQueryable<EventRow> StreamQuery(
        EventStoreDbContext ctx, string streamId, ReadDirection direction, long fromVersion, long maxCount)
    {
        var query = ctx.Events.AsNoTracking().Where(e => e.Domain == domain && e.StreamId == streamId);
        query = direction == ReadDirection.Forwards
            ? query.Where(e => e.StreamVersion >= Math.Max(1, fromVersion)).OrderBy(e => e.StreamVersion)
            : query.Where(e => e.StreamVersion <= (fromVersion <= 0 ? long.MaxValue : fromVersion))
                .OrderByDescending(e => e.StreamVersion);

        return maxCount < int.MaxValue ? query.Take((int)Math.Max(0, maxCount)) : query;
    }

    private IQueryable<EventRow> AllQuery(
        EventStoreDbContext ctx, GlobalPosition from, ReadDirection direction, long maxCount)
    {
        var fromSequence = ToSequence(from);
        var query = ctx.Events.AsNoTracking().Where(e => e.Domain == domain);
        query = direction == ReadDirection.Forwards
            ? query.Where(e => e.Position > fromSequence).OrderBy(e => e.Position)
            : query.Where(e => e.Position < fromSequence).OrderByDescending(e => e.Position);

        return maxCount < int.MaxValue ? query.Take((int)Math.Max(0, maxCount)) : query;
    }

    public override async IAsyncEnumerable<StoredEvent> ReadStreamAsync(
        string streamId,
        ReadDirection direction = ReadDirection.Forwards,
        long fromVersion = 0,
        long maxCount = long.MaxValue,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        // As an iterator, this body runs at enumeration time, so the frame is captured when the caller starts
        // reading, not when the method is called — read the enumerable, not the call site, for which frame it
        // joins. The common "await foreach immediately" makes the two coincide.
        await using var lease = await context.LeaseAsync(ct).ConfigureAwait(false);
        var query = StreamQuery(lease.Context, streamId, direction, fromVersion, maxCount);

        if (lease.Shared)
        {
            // On the frame's shared connection, materialize before yielding: it must stay free for the caller's
            // next write, and a data reader held open across yields would block it. The in-memory loop then
            // observes cancellation per item, since the buffering read already ran.
            foreach (var row in await query.ToListAsync(ct).ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();
                yield return ToStored(row);
            }
            yield break;
        }

        await foreach (var row in query.AsAsyncEnumerable().WithCancellation(ct).ConfigureAwait(false))
            yield return ToStored(row);
    }

    public override Task<StreamInfo?> GetStreamInfoAsync(string streamId, CancellationToken ct = default) =>
        context.ExecuteAsync<StreamInfo?>(async ctx =>
        {
            var head = await ctx.Streams.AsNoTracking()
                .FirstOrDefaultAsync(s => s.Domain == domain && s.StreamId == streamId, ct).ConfigureAwait(false);

            return head is null || head.HardDeleted
                ? null
                : new StreamInfo(streamId, head.Version, head.TruncatedBefore, head.IsDeleted, head.Metadata);
        }, ct);

    public Task<StreamMetadata?> GetStreamMetadataAsync(string streamId, CancellationToken ct = default) =>
        context.ExecuteAsync<StreamMetadata?>(async ctx =>
        {
            var json = await ctx.Streams.AsNoTracking()
                .Where(s => s.Domain == domain && s.StreamId == streamId && !s.HardDeleted)
                .Select(s => s.Metadata)
                .FirstOrDefaultAsync(ct).ConfigureAwait(false);
            return StreamMetadata.FromJson(json);
        }, ct);

    public async Task SetStreamMetadataAsync(string streamId, StreamMetadata metadata, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(streamId);
        ArgumentNullException.ThrowIfNull(metadata);

        var json = metadata.ToJson();

        // Under a frame, write on the borrowed context: a concurrent-create race cannot be recovered in place
        // (a failed save dooms that transaction), so it propagates and the caller retries the whole frame.
        if (context.Enlisted is { } enlisted)
        {
            await WriteStreamMetadataAsync(enlisted, streamId, json, recoverFromConcurrentCreate: false, ct)
                .ConfigureAwait(false);
            return;
        }

        await using var lease = await context.LeaseAsync(ct).ConfigureAwait(false);
        var ctx = lease.Context;
        await WriteStreamMetadataAsync(ctx, streamId, json, recoverFromConcurrentCreate: true, ct).ConfigureAwait(false);
    }

    private async Task WriteStreamMetadataAsync(
        EventStoreDbContext ctx,
        string streamId,
        string json,
        bool recoverFromConcurrentCreate,
        CancellationToken ct)
    {
        var head = await ctx.Streams.FindAsync([domain, streamId], ct).ConfigureAwait(false);
        if (head is { HardDeleted: true })
            throw new StreamDeletedException(streamId);

        if (head is null)
        {
            try
            {
                // Metadata may be set before a stream has events — create the head at version 0.
                ctx.Streams.Add(new StreamRow { Domain = domain, StreamId = streamId, Metadata = json });
                await ctx.SaveChangesAsync(ct).ConfigureAwait(false);
                return;
            }
            catch (Exception ex)
            {
                // Whatever failed the save, the staged row must not stay tracked — on the shared context a
                // later save would re-flush it. Only the autonomous path recovers from a lost concurrent-create
                // race by falling through to update the winner's metadata column; everything else propagates.
                ctx.ChangeTracker.Clear();
                if (!recoverFromConcurrentCreate || ex is not (DbUpdateException or DbUpdateConcurrencyException))
                    throw;
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
        // Frame captured at enumeration time, and materialize-before-yield on the shared connection — see
        // ReadStreamAsync.
        await using var lease = await context.LeaseAsync(ct).ConfigureAwait(false);
        var query = AllQuery(lease.Context, from, direction, maxCount);

        if (lease.Shared)
        {
            foreach (var row in await query.ToListAsync(ct).ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();
                yield return ToStored(row);
            }
            yield break;
        }

        await foreach (var row in query.AsAsyncEnumerable().WithCancellation(ct).ConfigureAwait(false))
            yield return ToStored(row);
    }

    public Task<GlobalPosition> FindPositionAtAsync(DateTimeOffset timestamp, CancellationToken ct = default) =>
        context.ExecuteAsync(async ctx =>
        {
            var before = await ctx.Events.AsNoTracking()
                .Where(e => e.Domain == domain && e.CreatedAt < timestamp)
                .OrderByDescending(e => e.Position)
                .Select(e => (long?)e.Position)
                .FirstOrDefaultAsync(ct).ConfigureAwait(false);

            return before is { } sequence ? GlobalPosition.FromSequence(sequence) : GlobalPosition.Start;
        }, ct);

    public Task TruncateStreamAsync(
        string streamId,
        long beforeVersion,
        bool force = false,
        CancellationToken ct = default) =>
        ExecuteMaintenanceAsync(streamId, async (ctx, token) =>
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
        ExecuteMaintenanceAsync(streamId, async (ctx, token) =>
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
        ExecuteMaintenanceAsync(streamId, async (ctx, token) =>
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

        // The tombstone prune (a write) and retention planning (reads) run on the enlisted context under a
        // frame, so a scavenge invoked inside a caller's unit of work joins its transaction rather than opening
        // a second connection that would self-lock against the frame's pending writes on a single-writer store.
        var (removed, streams, plan) = await context.ExecuteAsync(async ctx =>
        {
            var cutoff = timeProvider.GetUtcNow() - tombstoneRetention;
            long removedCount = await ctx.Events
                .Where(e => e.Domain == domain && e.StreamId == SystemEventTypes.SystemStream && e.CreatedAt < cutoff)
                .ExecuteDeleteAsync(ct).ConfigureAwait(false);
            var streamCount = await ctx.Streams.CountAsync(s => s.Domain == domain, ct).ConfigureAwait(false);
            var retentionPlan = new List<(string StreamId, long Boundary)>();

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
                        removedCount += boundary - row.TruncatedBefore; // the contiguous versions being dropped
                        retentionPlan.Add((row.StreamId, boundary));
                    }
                }
            }

            return (removedCount, streamCount, retentionPlan);
        }, ct).ConfigureAwait(false);

        // Force-truncate outside the planning block; autonomously each call opens its own transaction, while
        // under a frame they all join the borrowed one. Each emits the retention truncation's marker so read
        // models learn to forget. A stream deleted between planning and truncating is skipped, so one race
        // does not sink the whole pass.
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
        string streamId,
        Func<EventStoreDbContext, CancellationToken, Task<bool>> operation,
        bool transactional,
        CancellationToken ct)
    {
        if (context.Enlisted is { } enlisted)
        {
            // Enlisted: the frame supplies the transaction, so the transactional flag is moot, nothing commits
            // here, and a conflict propagates to roll the whole frame back rather than retrying in place.
            if (!await operation(enlisted, ct).ConfigureAwait(false))
                return;
            await enlisted.SaveEnlistedChangesAsync(ct).ConfigureAwait(false);
            NotifyChanged(streamId);
            return;
        }

        // Marker writes contend only with other (rarer) maintenance ops on the one $system head, so a
        // smaller budget than the append path's suffices; retries exist to absorb that contention, not
        // sustained load.
        const int maxAttempts = 32;
        for (var attempt = 1; ; attempt++)
        {
            await using var lease = await context.LeaseAsync(ct).ConfigureAwait(false);
            var ctx = lease.Context;
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

                // The maintenance op changed the stream, so signal it — a no-op returned above
                // (operation() == false) without reaching here.
                NotifyChanged(streamId);
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
