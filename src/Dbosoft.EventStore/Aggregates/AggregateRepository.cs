using System.Diagnostics.CodeAnalysis;
using Dbosoft.EventStore.Serialization;

namespace Dbosoft.EventStore.Aggregates;

/// <summary>
/// Default <see cref="IAggregateRepository{T}"/>. Maps aggregate ids to stream ids as
/// <c>{category}-{id}</c>, maps events through the domain's <see cref="IEventMaterializer"/>, and uses
/// an optional snapshot store governed by a policy.
/// </summary>
/// <typeparam name="T">The aggregate type.</typeparam>
[PublicAPI]
public sealed class AggregateRepository<T> : IAggregateRepository<T> where T : AggregateBase
{
    private readonly IEventStore _store;
    private readonly IEventMaterializer _materializer;
    private readonly IEventSerializer _serializer;
    private readonly ISnapshotStore? _snapshotStore;
    private readonly ISnapshotPolicy _snapshotPolicy;
    private readonly string _category;
    private readonly Func<string, T> _factory;

    /// <summary>Creates the repository for a stream category.</summary>
    /// <param name="store">The backing event store.</param>
    /// <param name="materializer">Maps events between their stored bytes and typed instances.</param>
    /// <param name="serializer">Serializer for snapshot state.</param>
    /// <param name="category">Stream-id prefix for this aggregate type.</param>
    /// <param name="factory">Creates an empty aggregate for a given id.</param>
    /// <param name="snapshotStore">Optional snapshot store enabling snapshot-based rehydration.</param>
    /// <param name="snapshotPolicy">When to write snapshots; ignored without a snapshot store.</param>
    public AggregateRepository(
        IEventStore store,
        IEventMaterializer materializer,
        IEventSerializer serializer,
        string category,
        Func<string, T> factory,
        ISnapshotStore? snapshotStore = null,
        ISnapshotPolicy? snapshotPolicy = null)
    {
        _store = store;
        _materializer = materializer;
        _serializer = serializer;
        _category = category;
        _factory = factory;
        _snapshotStore = snapshotStore;
        _snapshotPolicy = snapshotPolicy ?? NoSnapshotPolicy.Instance;
    }

    private string StreamId(string id) => $"{_category}-{id}";

    /// <inheritdoc />
    public async Task<T?> LoadAsync(string id, CancellationToken ct = default)
    {
        var streamId = StreamId(id);
        var info = await _store.GetStreamInfoAsync(streamId, ct).ConfigureAwait(false);
        var aggregate = _factory(id);
        var restoredVersion = 0L;
        var exists = false;

        if (_snapshotStore is not null && aggregate is ISnapshotable snapshotable)
        {
            // Try the newest snapshot; if it no longer deserializes, fall back through older ones a
            // SnapshotHistory provider still keeps, before giving up on snapshots entirely.
            var upTo = long.MaxValue;
            while (await _snapshotStore.LoadAtOrBeforeAsync(streamId, upTo, ct).ConfigureAwait(false) is { } snapshot)
            {
                if (TryDeserializeSnapshot(snapshotable, snapshot, out var state))
                {
                    snapshotable.RestoreFromSnapshot(state, snapshot.StreamVersion);
                    restoredVersion = snapshot.StreamVersion;
                    exists = true;
                    break;
                }

                if (snapshot.StreamVersion <= 0)
                    break;
                upTo = snapshot.StreamVersion - 1;
            }
        }

        // If the stream is truncated below where we can start folding, the events needed to
        // reconstruct the prefix are gone. A snapshot at or after the boundary is then load-bearing,
        // not optional — fail loudly rather than fold a partial suffix into a silently wrong aggregate.
        var truncatedBefore = info?.TruncatedBefore ?? 1;
        if (truncatedBefore > 1 && restoredVersion < truncatedBefore - 1)
            throw new SnapshotRequiredException(streamId, truncatedBefore);

        var expectedVersion = restoredVersion;
        await foreach (var stored in _store
                           .ReadStreamAsync(streamId, ReadDirection.Forwards, restoredVersion + 1, ct: ct)
                           .ConfigureAwait(false))
        {
            // A system marker must never reach an aggregate's fold — it carries no state and would
            // miscount the version. Aggregate domains keep markers on $system, so a marker on this
            // stream is a misconfiguration; fail loudly rather than silently half-fold.
            if (SystemEventTypes.IsSystem(stored.EventType))
                throw new InvalidOperationException(
                    $"Stream '{streamId}' contains system event '{stored.EventType}', which cannot be folded "
                    + $"into aggregate '{typeof(T).Name}'. Aggregate domains must not emit markers in-band.");

            // The events must be contiguous from where we resumed; a gap means a truncation (possibly
            // racing this load) removed events the snapshot does not cover, so the state is unrecoverable.
            expectedVersion++;
            if (stored.StreamVersion != expectedVersion)
                throw new SnapshotRequiredException(streamId, stored.StreamVersion);

            exists = true;
            aggregate.ReplayEvent(_materializer.Materialize(stored));
        }

        return exists ? aggregate : null;
    }

    /// <inheritdoc />
    public async Task SaveAsync(T aggregate, CancellationToken ct = default)
    {
        var events = aggregate.UncommittedEvents;
        if (events.Count == 0)
            return;

        var streamId = StreamId(aggregate.Id);
        var versionAfter = aggregate.Version;
        var versionBefore = versionAfter - events.Count;

        var data = new EventData[events.Count];
        for (var i = 0; i < events.Count; i++)
            data[i] = _materializer.Dematerialize(events[i].Event, events[i].EventId);

        await _store.AppendToStreamAsync(streamId, versionBefore, data, ct).ConfigureAwait(false);
        aggregate.MarkCommitted();

        if (_snapshotStore is not null
            && aggregate is ISnapshotable snapshotable
            && _snapshotPolicy.ShouldSnapshot(versionBefore, versionAfter))
        {
            var state = _serializer.Serialize(snapshotable.CreateSnapshotState());
            await _snapshotStore.SaveAsync(streamId, versionAfter, state, _serializer.ContentType, ct)
                .ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task SaveAllAsync(IReadOnlyList<T> aggregates, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(aggregates);

        // Only aggregates with pending events take part; one with nothing to commit is skipped, just as
        // SaveAsync returns early for it.
        var pending = aggregates.Where(a => a.UncommittedEvents.Count > 0).ToList();
        if (pending.Count == 0)
            return;

        if (_store is not IMultiStreamAppend multi)
            throw new NotSupportedByProviderException(
                $"The store '{_store.GetType().Name}' cannot append to multiple streams atomically, which "
                + "SaveAllAsync requires. Save the aggregates individually if a non-atomic write is acceptable.");

        var appends = new StreamAppend[pending.Count];
        for (var i = 0; i < pending.Count; i++)
        {
            var aggregate = pending[i];
            var events = aggregate.UncommittedEvents;
            var versionBefore = aggregate.Version - events.Count;
            var data = new EventData[events.Count];
            for (var j = 0; j < events.Count; j++)
                data[j] = _materializer.Dematerialize(events[j].Event, events[j].EventId);
            appends[i] = new StreamAppend(StreamId(aggregate.Id), versionBefore, data);
        }

        await multi.AppendToStreamsAsync(appends, ct).ConfigureAwait(false);

        // The events are durably committed. Mark every aggregate committed first, so a later
        // (best-effort) snapshot failure can never leave an aggregate still holding events that are in
        // fact persisted. Snapshots — disposable derived data — are written afterwards.
        var snapshotWork = new List<(string StreamId, ISnapshotable Snapshotable, long Version)>();
        foreach (var aggregate in pending)
        {
            var versionAfter = aggregate.Version;
            var versionBefore = versionAfter - aggregate.UncommittedEvents.Count;
            aggregate.MarkCommitted();

            if (_snapshotStore is not null
                && aggregate is ISnapshotable snapshotable
                && _snapshotPolicy.ShouldSnapshot(versionBefore, versionAfter))
                snapshotWork.Add((StreamId(aggregate.Id), snapshotable, versionAfter));
        }

        foreach (var (streamId, snapshotable, version) in snapshotWork)
        {
            var state = _serializer.Serialize(snapshotable.CreateSnapshotState());
            await _snapshotStore!.SaveAsync(streamId, version, state, _serializer.ContentType, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Deserializes a snapshot's state, treating any failure as "no usable snapshot". A snapshot is
    /// disposable derived data, so a schema-incompatible or corrupt one must never fail a load — the
    /// caller falls back to replaying the full event stream. Combined with a tolerant (schemaless)
    /// serializer that ignores unknown members and defaults missing ones, additive changes to the
    /// snapshot state just work, and breaking changes degrade gracefully to a full replay.
    /// </summary>
    private bool TryDeserializeSnapshot(
        ISnapshotable snapshotable,
        StoredSnapshot snapshot,
        [MaybeNullWhen(false)] out object state)
    {
        try
        {
            state = _serializer.Deserialize(snapshotable.SnapshotStateType, snapshot.Payload);
            return true;
        }
        catch (Exception) // serializer is pluggable; any deserialization failure means the snapshot
        {                 // is unusable, and replaying from events is always a correct fallback.
            state = null;
            return false;
        }
    }
}
