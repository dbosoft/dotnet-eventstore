using System.Runtime.CompilerServices;
using System.Text.Json;

namespace Dbosoft.EventStore.InMemory;

/// <summary>
/// A fully in-memory event store that implements every capability. It is the reference
/// implementation the conformance suite runs against and the bar the persistent providers must
/// clear; it is also handy for tests and samples. All writes are serialized, so its global sequence
/// is dense and gap-free, which is why its catch-up reads are exact.
/// </summary>
[PublicAPI]
public sealed class InMemoryEventStore
    : IEventStore, IAllStreamReader, ITimeNavigable, IStreamMaintenance, IMultiStreamAppend, IStreamMetadataStore
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, StreamState> _streams = new(StringComparer.Ordinal);
    private readonly List<Entry> _all = [];
    private readonly Dictionary<string, List<StoredSnapshot>> _snapshots = new(StringComparer.Ordinal);
    private readonly TimeProvider _timeProvider;
    private readonly bool _inBandMarkers;
    private readonly TimeSpan _tombstoneRetention;
    private readonly IChangeNotifier? _changeNotifier;
    private readonly string _notifyDomain;
    private long _sequence;

    /// <summary>Creates the store, optionally with a custom time source, marker policy and change notifier.</summary>
    public InMemoryEventStore(
        TimeProvider? timeProvider = null,
        bool inBandMarkers = false,
        TimeSpan? tombstoneRetention = null,
        IChangeNotifier? changeNotifier = null,
        string? notifyDomain = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _inBandMarkers = inBandMarkers;
        _tombstoneRetention = tombstoneRetention ?? TimeSpan.FromDays(7);
        _changeNotifier = changeNotifier;
        _notifyDomain = notifyDomain ?? "";
        Snapshots = new InMemorySnapshotStore(this);
    }

    // Best-effort "something changed" signal after a write, so a subscription can wake without polling.
    // The in-process notifier completes inline; anything slower is fire-and-forget (a lost signal is caught
    // by the subscription's poll backstop).
    private void FireChanged()
    {
        if (_changeNotifier is null)
            return;
        var pending = _changeNotifier.NotifyAsync(_notifyDomain);
        if (!pending.IsCompleted)
            _ = pending.AsTask();
    }

    /// <summary>A snapshot store backed by the same in-memory data.</summary>
    public ISnapshotStore Snapshots { get; }

    /// <inheritdoc />
    public StoreCapabilities Capabilities =>
        StoreCapabilities.ExactCatchup | StoreCapabilities.SnapshotHistory;

    /// <inheritdoc />
    public Task<AppendResult> AppendToStreamAsync(
        string streamId,
        long expectedVersion,
        IReadOnlyList<EventData> events,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(streamId);
        ArgumentNullException.ThrowIfNull(events);
        ct.ThrowIfCancellationRequested();

        lock (_gate)
        {
            _streams.TryGetValue(streamId, out var state);

            if (state is { HardDeleted: true })
                throw new StreamDeletedException(streamId);

            var batchIds = new HashSet<Guid>();
            foreach (var evt in events)
            {
                if (!batchIds.Add(evt.EventId))
                    throw new InvalidOperationException(
                        $"The append to stream '{streamId}' contains EventId {evt.EventId} more than once.");
            }

            if (state is not null && events.Count > 0)
            {
                // Idempotency is scoped to the whole batch: an exact replay of an already-stored batch is
                // a no-op, a partial overlap is an error, and a fully new batch proceeds. Dedup only holds
                // while the events are retained (truncation drops their ids).
                var present = events.Count(evt => state.EventIds.Contains(evt.EventId));
                if (present == events.Count)
                {
                    var replayed = state.Events.Where(e => batchIds.Contains(e.EventId)).MaxBy(e => e.StreamVersion);
                    return replayed is null ? throw new InvalidOperationException($"Idempotency records for stream '{streamId}' are inconsistent.") :
                        // Return the outcome of the original append operation, not the current head.
                        Task.FromResult(new AppendResult(replayed.StreamVersion, replayed.Position));
                }

                if (present > 0)
                    throw new InvalidOperationException(
                        $"The append to stream '{streamId}' partially overlaps events already stored; an append "
                        + "must be either entirely new or an exact replay of a prior batch.");
            }

            EnsureExpectedVersion(streamId, expectedVersion, state?.Version ?? 0);

            // An empty append writes nothing, so it never materializes a stream or clears a soft-
            // deletion — but the expected-version check above still applies (a checked no-op).
            if (events.Count == 0)
                return Task.FromResult(new AppendResult(state?.Version ?? 0, null));

            // Validation passed — only now materialize the stream, so a rejected append leaves no trace.
            if (state is null)
            {
                state = new StreamState();
                _streams[streamId] = state;
            }

            foreach (var evt in events)
            {
                var entry = new Entry(
                    streamId,
                    state.Version + 1,
                    ++_sequence,
                    evt.EventId,
                    evt.EventType,
                    evt.Data,
                    evt.Metadata,
                    evt.ContentType,
                    _timeProvider.GetUtcNow());

                state.Version = entry.StreamVersion;
                state.IsDeleted = false;
                state.Events.Add(entry);
                state.EventIds.Add(evt.EventId);
                _all.Add(entry);
            }

            FireChanged();
            return Task.FromResult(new AppendResult(state.Version, LastPosition(state)));
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<AppendResult>> AppendToStreamsAsync(
        IReadOnlyList<StreamAppend> appends,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(appends);
        ct.ThrowIfCancellationRequested();

        lock (_gate)
        {
            var seenStreams = new HashSet<string>(StringComparer.Ordinal);
            var plan = new (StreamAppend Append, StreamState? State, AppendResult? Settled)[appends.Count];

            // Phase 1 — validate every entry against one consistent snapshot; mutate nothing, so any
            // failure leaves the whole operation with no effect. The single lock makes it atomic.
            for (var i = 0; i < appends.Count; i++)
            {
                var append = appends[i];
                ArgumentException.ThrowIfNullOrEmpty(append.StreamId);
                ArgumentNullException.ThrowIfNull(append.Events);
                if (!seenStreams.Add(append.StreamId))
                    throw new InvalidOperationException(
                        $"Stream '{append.StreamId}' appears more than once in a multi-stream append.");

                _streams.TryGetValue(append.StreamId, out var state);
                if (state is { HardDeleted: true })
                    throw new StreamDeletedException(append.StreamId);

                var batchIds = new HashSet<Guid>();
                foreach (var evt in append.Events)
                {
                    if (!batchIds.Add(evt.EventId))
                        throw new InvalidOperationException(
                            $"The append to stream '{append.StreamId}' contains EventId {evt.EventId} more than once.");
                }

                AppendResult? settled = null;
                if (state is not null && append.Events.Count > 0)
                {
                    var present = append.Events.Count(evt => state.EventIds.Contains(evt.EventId));
                    if (present == append.Events.Count)
                    {
                        var replayed = state.Events.Where(e => batchIds.Contains(e.EventId)).MaxBy(e => e.StreamVersion)
                                       ?? throw new InvalidOperationException(
                                           $"Idempotency records for stream '{append.StreamId}' are inconsistent.");
                        settled = new AppendResult(replayed.StreamVersion, replayed.Position);
                    }
                    else if (present > 0)
                        throw new InvalidOperationException(
                            $"The append to stream '{append.StreamId}' partially overlaps events already stored; an "
                            + "append must be either entirely new or an exact replay of a prior batch.");
                }

                if (settled is null)
                {
                    EnsureExpectedVersion(append.StreamId, append.ExpectedVersion, state?.Version ?? 0);
                    if (append.Events.Count == 0)
                        settled = new AppendResult(state?.Version ?? 0, null);
                }

                plan[i] = (append, state, settled);
            }

            // Phase 2 — apply. Validation passed, so nothing here can fail.
            var results = new AppendResult[appends.Count];
            for (var i = 0; i < plan.Length; i++)
            {
                var (append, state, settled) = plan[i];
                if (settled is { } done)
                {
                    results[i] = done;
                    continue;
                }

                if (state is null)
                {
                    state = new StreamState();
                    _streams[append.StreamId] = state;
                }

                foreach (var evt in append.Events)
                {
                    var entry = new Entry(
                        append.StreamId, state.Version + 1, ++_sequence, evt.EventId, evt.EventType,
                        evt.Data, evt.Metadata, evt.ContentType, _timeProvider.GetUtcNow());
                    state.Version = entry.StreamVersion;
                    state.IsDeleted = false;
                    state.Events.Add(entry);
                    state.EventIds.Add(evt.EventId);
                    _all.Add(entry);
                }

                results[i] = new AppendResult(state.Version, LastPosition(state));
            }

            FireChanged();
            return Task.FromResult<IReadOnlyList<AppendResult>>(results);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<StoredEvent> ReadStreamAsync(
        string streamId,
        ReadDirection direction = ReadDirection.Forwards,
        long fromVersion = 0,
        long maxCount = long.MaxValue,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        List<StoredEvent> page;
        lock (_gate)
        {
            if (!_streams.TryGetValue(streamId, out var state) || state.HardDeleted || state.IsDeleted)
                yield break;

            IEnumerable<Entry> query = state.Events.Where(e => e.StreamVersion >= state.TruncatedBefore);
            query = direction == ReadDirection.Forwards
                ? query.Where(e => e.StreamVersion >= Math.Max(1, fromVersion)).OrderBy(e => e.StreamVersion)
                : query.Where(e => e.StreamVersion <= (fromVersion <= 0 ? long.MaxValue : fromVersion))
                    .OrderByDescending(e => e.StreamVersion);

            page = query.Take(Clamp(maxCount)).Select(e => e.ToStored()).ToList();
        }

        foreach (var stored in page)
        {
            ct.ThrowIfCancellationRequested();
            yield return stored;
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<StreamInfo?> GetStreamInfoAsync(string streamId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_streams.TryGetValue(streamId, out var state) || state.HardDeleted)
                return Task.FromResult<StreamInfo?>(null);

            return Task.FromResult<StreamInfo?>(
                new StreamInfo(streamId, state.Version, state.TruncatedBefore, state.IsDeleted, state.Metadata?.ToJson()));
        }
    }

    /// <inheritdoc />
    public Task<StreamMetadata?> GetStreamMetadataAsync(string streamId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (_gate)
        {
            return Task.FromResult(
                _streams.TryGetValue(streamId, out var state) && !state.HardDeleted ? state.Metadata : null);
        }
    }

    /// <inheritdoc />
    public Task SetStreamMetadataAsync(string streamId, StreamMetadata metadata, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(streamId);
        ArgumentNullException.ThrowIfNull(metadata);
        ct.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_streams.TryGetValue(streamId, out var state))
            {
                // Metadata may be set before a stream has events — e.g. a retention policy up front.
                state = new StreamState();
                _streams[streamId] = state;
            }

            if (state.HardDeleted)
                throw new StreamDeletedException(streamId);

            state.Metadata = metadata;
            return Task.CompletedTask;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<StoredEvent> ReadAllAsync(
        GlobalPosition from,
        ReadDirection direction = ReadDirection.Forwards,
        long maxCount = long.MaxValue,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        List<StoredEvent> page;
        lock (_gate)
        {
            IEnumerable<Entry> query = direction == ReadDirection.Forwards
                ? _all.Where(e => e.Position > from).OrderBy(e => e.Sequence)
                : _all.Where(e => e.Position < from).OrderByDescending(e => e.Sequence);

            page = query.Take(Clamp(maxCount)).Select(e => e.ToStored()).ToList();
        }

        foreach (var stored in page)
        {
            ct.ThrowIfCancellationRequested();
            yield return stored;
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<GlobalPosition> FindPositionAtAsync(DateTimeOffset timestamp, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (_gate)
        {
            // The checkpoint to read *after*: the last event before the timestamp, so an exclusive
            // read starting here first yields the earliest event at or after it.
            var before = _all.Where(e => e.CreatedAt < timestamp).OrderBy(e => e.Sequence).LastOrDefault();
            return Task.FromResult(before?.Position ?? GlobalPosition.Start);
        }
    }

    /// <inheritdoc />
    public Task TruncateStreamAsync(
        string streamId,
        long beforeVersion,
        bool force = false,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_streams.TryGetValue(streamId, out var state) || state.HardDeleted)
                throw new StreamNotFoundException(streamId);

            // A truncation point past the head means "drop everything"; clamp it so the recorded
            // boundary never runs beyond the end of the stream.
            beforeVersion = Math.Min(beforeVersion, state.Version + 1);

            // A truncation that would remove nothing is a no-op — checked before the snapshot-coverage
            // guard, so repeating it never fails just because the covering snapshot was later pruned.
            if (beforeVersion <= state.TruncatedBefore)
                return Task.CompletedTask;

            if (!force && LatestSnapshotVersion(streamId) < beforeVersion - 1)
                throw new NotSupportedByProviderException(
                    $"Truncating stream '{streamId}' before version {beforeVersion} would leave it "
                    + "impossible to reconstruct: no snapshot at or after that boundary exists. Pass force to override.");

            // Drop the truncated events and their idempotency records: dedup only holds while events
            // are retained, so a re-append of a truncated batch is treated as new (matching a provider
            // whose unique (stream, id) index rows are physically removed by truncation).
            foreach (var removed in state.Events.Where(e => e.StreamVersion < beforeVersion))
                state.EventIds.Remove(removed.EventId);
            state.Events.RemoveAll(e => e.StreamVersion < beforeVersion);
            _all.RemoveAll(e => e.StreamId == streamId && e.StreamVersion < beforeVersion);
            state.TruncatedBefore = beforeVersion;

            EmitMarker(SystemEventTypes.StreamTruncated, new StreamTruncatedMarker(streamId, beforeVersion), streamId);
            return Task.CompletedTask;
        }
    }

    /// <inheritdoc />
    public Task DeleteStreamAsync(string streamId, DeleteMode mode, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_streams.TryGetValue(streamId, out var state) || state.HardDeleted)
                throw new StreamNotFoundException(streamId);

            // Deleting an already-soft-deleted stream is a no-op — don't re-emit a tombstone. A stream
            // recreated after deletion clears IsDeleted on append, so this only skips a true repeat.
            if (mode == DeleteMode.Soft && state.IsDeleted)
                return Task.CompletedTask;

            state.Events.Clear();
            _all.RemoveAll(e => e.StreamId == streamId);
            _snapshots.Remove(streamId);
            state.IsDeleted = true;
            state.TruncatedBefore = state.Version + 1;

            if (mode == DeleteMode.Hard)
                state.HardDeleted = true;
            else
                state.EventIds.Clear();

            // Whole-stream deletion always goes to $system — the stream is gone.
            EmitMarker(SystemEventTypes.StreamDeleted, new StreamDeletedMarker(streamId, mode), inBandStream: null);
            return Task.CompletedTask;
        }
    }

    /// <inheritdoc />
    public Task RedactEventAsync(
        string streamId,
        long version,
        ReadOnlyMemory<byte> newData,
        ReadOnlyMemory<byte>? newMetadata = null,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_streams.TryGetValue(streamId, out var state) || state.HardDeleted)
                throw new StreamNotFoundException(streamId);

            var index = state.Events.FindIndex(e => e.StreamVersion == version);
            if (index < 0)
                throw new StreamNotFoundException(streamId);

            // Overwrite the payload in place: same version, position and id — only the bytes change.
            var redacted = state.Events[index] with { Data = newData, Metadata = newMetadata };
            state.Events[index] = redacted;
            var allIndex = _all.FindIndex(e => e.StreamId == streamId && e.StreamVersion == version);
            if (allIndex >= 0)
                _all[allIndex] = redacted;

            EmitMarker(SystemEventTypes.EventRedacted, new EventRedactedMarker(streamId, version), streamId);
            return Task.CompletedTask;
        }
    }

    /// <inheritdoc />
    public async Task<ScavengeReport> ScavengeAsync(ScavengeOptions? options = null, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var applyRetention = options?.ApplyRetention ?? true;
        List<(string StreamId, long Boundary)> retentionPlan = [];
        long removed;

        // Truncation and deletion already free stream memory eagerly. A scavenge reclaims expired $system
        // tombstones and, with retention enabled, plans per-stream truncations from each stream's policy.
        lock (_gate)
        {
            removed = 0;
            var tombstoneCutoff = _timeProvider.GetUtcNow() - _tombstoneRetention;
            if (_streams.TryGetValue(SystemEventTypes.SystemStream, out var sys))
            {
                removed = sys.Events.RemoveAll(e => e.CreatedAt < tombstoneCutoff);
                _all.RemoveAll(e => e.StreamId == SystemEventTypes.SystemStream && e.CreatedAt < tombstoneCutoff);
            }

            if (applyRetention)
            {
                var now = _timeProvider.GetUtcNow();
                foreach (var (streamId, state) in _streams)
                {
                    if (state.Metadata is not { HasRetention: true } meta || state.IsDeleted || state.HardDeleted)
                        continue;

                    var boundary = RetentionBoundary(meta, state, now);
                    if (boundary <= state.TruncatedBefore)
                        continue;

                    removed += state.Events.Count(e => e.StreamVersion < boundary);
                    retentionPlan.Add((streamId, boundary));
                }
            }
        }

        // Truncate outside the lock: the non-reentrant gate would deadlock, and TruncateStreamAsync also
        // emits the retention truncation's marker so read models learn to forget. A stream deleted between
        // planning and truncating is skipped, so one race does not sink the whole pass.
        foreach (var (streamId, boundary) in retentionPlan)
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

        return new ScavengeReport(CountStreams(), removed);
    }

    // The highest version to retain-truncate before, given a policy: the max of the count boundary
    // (keep the newest MaxCount) and the age boundary (drop events older than MaxAge). Called under the gate.
    private static long RetentionBoundary(StreamMetadata meta, StreamState state, DateTimeOffset now)
    {
        var boundary = state.TruncatedBefore;
        if (meta.MaxCount is { } max && state.Version > max)
            boundary = Math.Max(boundary, state.Version - max + 1);
        if (meta.MaxAge is { } age)
        {
            var cutoff = now - age;
            // Truncate before the first event still within the age window; if none, drop them all.
            long? firstKept = state.Events.Where(e => e.CreatedAt >= cutoff).Select(e => (long?)e.StreamVersion).Min();
            boundary = Math.Max(boundary, firstKept ?? state.Version + 1);
        }

        return boundary;
    }

    private int CountStreams()
    {
        lock (_gate)
            return _streams.Count;
    }

    // Appends a marker event into $all so read models learn to forget or reproject. Goes on the
    // reserved "$system" stream by default, or in-band on the affected stream when the domain opts in
    // and the op is survivable (inBandStream != null). Must be called while holding the gate.
    private void EmitMarker(string eventType, object marker, string? inBandStream)
    {
        var streamId = _inBandMarkers && inBandStream is not null ? inBandStream : SystemEventTypes.SystemStream;
        if (!_streams.TryGetValue(streamId, out var target))
        {
            target = new StreamState();
            _streams[streamId] = target;
        }

        var entry = new Entry(
            streamId,
            target.Version + 1,
            ++_sequence,
            Guid.NewGuid(),
            eventType,
            JsonSerializer.SerializeToUtf8Bytes(marker, marker.GetType()),
            null,
            "application/json",
            _timeProvider.GetUtcNow());

        target.Version = entry.StreamVersion;
        target.Events.Add(entry);
        _all.Add(entry);
    }

    private void EnsureExpectedVersion(string streamId, long expectedVersion, long currentVersion)
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

    private static GlobalPosition? LastPosition(StreamState state) =>
        state.Events.Count == 0 ? null : state.Events[^1].Position;

    private long LatestSnapshotVersion(string streamId) =>
        _snapshots.TryGetValue(streamId, out var list) && list.Count > 0 ? list[^1].StreamVersion : 0;

    private static int Clamp(long maxCount) => maxCount >= int.MaxValue ? int.MaxValue : (int)Math.Max(0, maxCount);

    // --- snapshot operations, invoked by InMemorySnapshotStore under the shared gate ---

    internal void SaveSnapshot(string streamId, long streamVersion, ReadOnlyMemory<byte> payload, string contentType)
    {
        lock (_gate)
        {
            if (!_snapshots.TryGetValue(streamId, out var list))
            {
                list = [];
                _snapshots[streamId] = list;
            }

            list.RemoveAll(s => s.StreamVersion == streamVersion);
            var snapshot = new StoredSnapshot(streamId, streamVersion, payload, contentType, _timeProvider.GetUtcNow());
            var index = list.FindIndex(s => s.StreamVersion > streamVersion);
            if (index < 0)
                list.Add(snapshot);
            else
                list.Insert(index, snapshot);
        }
    }

    internal StoredSnapshot? LoadSnapshot(string streamId, long maxVersion)
    {
        lock (_gate)
        {
            if (!_snapshots.TryGetValue(streamId, out var list))
                return null;

            for (var i = list.Count - 1; i >= 0; i--)
                if (list[i].StreamVersion <= maxVersion)
                    return list[i];

            return null;
        }
    }

    internal void PruneSnapshots(string streamId, int keep)
    {
        lock (_gate)
        {
            if (!_snapshots.TryGetValue(streamId, out var list) || list.Count <= keep)
                return;

            list.RemoveRange(0, list.Count - keep);
        }
    }

    private sealed record Entry(
        string StreamId,
        long StreamVersion,
        long Sequence,
        Guid EventId,
        string EventType,
        ReadOnlyMemory<byte> Data,
        ReadOnlyMemory<byte>? Metadata,
        string ContentType,
        DateTimeOffset CreatedAt)
    {
        public GlobalPosition Position => GlobalPosition.FromSequence(Sequence);

        public StoredEvent ToStored() =>
            new(StreamId, StreamVersion, Position, EventId, EventType, Data, Metadata, ContentType, CreatedAt);
    }

    private sealed class StreamState
    {
        public long Version { get; set; }
        public long TruncatedBefore { get; set; } = 1;
        public bool IsDeleted { get; set; }
        public bool HardDeleted { get; set; }
        public StreamMetadata? Metadata { get; set; }
        public List<Entry> Events { get; } = [];
        public HashSet<Guid> EventIds { get; } = [];
    }
}
