using System.Runtime.CompilerServices;

namespace Dbosoft.EventStore.InMemory;

/// <summary>
/// A fully in-memory event store that implements every capability. It is the reference
/// implementation the conformance suite runs against and the bar the persistent providers must
/// clear; it is also handy for tests and samples. All writes are serialized, so its global sequence
/// is dense and gap-free, which is why it can offer exact subscriptions.
/// </summary>
[PublicAPI]
public sealed class InMemoryEventStore
    : IEventStore, IAllStreamReader, ITimeNavigable, ISubscriptionSource, IStreamMaintenance
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, StreamState> _streams = new(StringComparer.Ordinal);
    private readonly List<Entry> _all = [];
    private readonly Dictionary<string, List<StoredSnapshot>> _snapshots = new(StringComparer.Ordinal);
    private readonly HashSet<InMemorySubscription> _subscriptions = [];
    private readonly TimeProvider _timeProvider;
    private long _sequence;

    /// <summary>Creates the store, optionally with a custom time source for testing.</summary>
    public InMemoryEventStore(TimeProvider? timeProvider = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        Snapshots = new InMemorySnapshotStore(this);
    }

    /// <summary>A snapshot store backed by the same in-memory data.</summary>
    public ISnapshotStore Snapshots { get; }

    /// <inheritdoc />
    public StoreCapabilities Capabilities =>
        StoreCapabilities.AllStreamReads
        | StoreCapabilities.Subscriptions
        | StoreCapabilities.ExactSubscriptions
        | StoreCapabilities.MultiStreamAtomicAppend
        | StoreCapabilities.TimeNavigation
        | StoreCapabilities.Truncation
        | StoreCapabilities.StreamDeletion
        | StoreCapabilities.SnapshotHistory;

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

        AppendResult result;
        List<InMemorySubscription> toNotify;

        lock (_gate)
        {
            if (!_streams.TryGetValue(streamId, out var state))
            {
                state = new StreamState();
                _streams[streamId] = state;
            }

            if (state.HardDeleted)
                throw new StreamDeletedException(streamId);

            var newEvents = events.Where(e => !state.EventIds.Contains(e.EventId)).ToList();
            if (newEvents.Count == 0)
            {
                // Every event has already been appended: an idempotent retry, so this is a no-op
                // regardless of the expected version.
                return Task.FromResult(new AppendResult(state.Version, LastPosition(state)));
            }

            EnsureExpectedVersion(streamId, expectedVersion, state.Version);

            foreach (var evt in newEvents)
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

            result = new AppendResult(state.Version, LastPosition(state));
            toNotify = [.. _subscriptions];
        }

        foreach (var subscription in toNotify)
            subscription.Notify();

        return Task.FromResult(result);
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
                new StreamInfo(streamId, state.Version, state.TruncatedBefore, state.IsDeleted, Metadata: null));
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
    public IAsyncDisposable SubscribeToAll(
        GlobalPosition checkpoint,
        IEventHandler handler,
        SubscriptionOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(handler);

        var subscription = new InMemorySubscription(this, checkpoint, handler, options ?? new SubscriptionOptions());
        lock (_gate)
            _subscriptions.Add(subscription);

        subscription.Start();
        return subscription;
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

            if (!force && LatestSnapshotVersion(streamId) < beforeVersion - 1)
                throw new NotSupportedByProviderException(
                    $"Truncating stream '{streamId}' before version {beforeVersion} would leave it "
                    + "unreconstructable: no snapshot at or after that boundary exists. Pass force to override.");

            if (beforeVersion <= state.TruncatedBefore)
                return Task.CompletedTask;

            state.Events.RemoveAll(e => e.StreamVersion < beforeVersion);
            _all.RemoveAll(e => e.StreamId == streamId && e.StreamVersion < beforeVersion);
            state.TruncatedBefore = beforeVersion;
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

            state.Events.Clear();
            _all.RemoveAll(e => e.StreamId == streamId);
            _snapshots.Remove(streamId);
            state.IsDeleted = true;
            state.TruncatedBefore = state.Version + 1;

            if (mode == DeleteMode.Hard)
                state.HardDeleted = true;
            else
                state.EventIds.Clear();

            return Task.CompletedTask;
        }
    }

    /// <inheritdoc />
    public Task<ScavengeReport> ScavengeAsync(ScavengeOptions? options = null, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        // Truncation and deletion already free memory eagerly here, so a scavenge pass only reports.
        lock (_gate)
        {
            var processed = options?.MaxStreams is { } max ? Math.Min(_streams.Count, max) : _streams.Count;
            return Task.FromResult(new ScavengeReport(processed, 0));
        }
    }

    private void EnsureExpectedVersion(string streamId, long expectedVersion, long currentVersion)
    {
        var satisfied = expectedVersion switch
        {
            ExpectedVersion.Any => true,
            ExpectedVersion.NoStream => currentVersion == 0,
            ExpectedVersion.StreamExists => currentVersion > 0,
            _ => expectedVersion == currentVersion,
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

    internal void RemoveSubscription(InMemorySubscription subscription)
    {
        lock (_gate)
            _subscriptions.Remove(subscription);
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
        public List<Entry> Events { get; } = [];
        public HashSet<Guid> EventIds { get; } = [];
    }
}
