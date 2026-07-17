using Dbosoft.EventStore.Serialization;

namespace Dbosoft.EventStore.Aggregates;

/// <summary>
/// Default <see cref="IAggregateRepository{T}"/>. Maps aggregate ids to stream ids as
/// <c>{category}-{id}</c>, serializes events through the registry and upcasts on read, and uses an
/// optional snapshot store governed by a policy.
/// </summary>
/// <typeparam name="T">The aggregate type.</typeparam>
public sealed class AggregateRepository<T> : IAggregateRepository<T> where T : AggregateBase
{
    private readonly IEventStore _store;
    private readonly IEventSerializer _serializer;
    private readonly IEventTypeRegistry _registry;
    private readonly UpcasterChain _upcasters;
    private readonly ISnapshotStore? _snapshotStore;
    private readonly ISnapshotPolicy _snapshotPolicy;
    private readonly string _category;
    private readonly Func<string, T> _factory;

    /// <summary>Creates the repository for a stream category.</summary>
    /// <param name="store">The backing event store.</param>
    /// <param name="serializer">Serializer for events and snapshots.</param>
    /// <param name="registry">Registry mapping CLR event types to logical names.</param>
    /// <param name="upcasters">Upcaster chain applied to events on read.</param>
    /// <param name="category">Stream-id prefix for this aggregate type.</param>
    /// <param name="factory">Creates an empty aggregate for a given id.</param>
    /// <param name="snapshotStore">Optional snapshot store enabling snapshot-based rehydration.</param>
    /// <param name="snapshotPolicy">When to write snapshots; ignored without a snapshot store.</param>
    public AggregateRepository(
        IEventStore store,
        IEventSerializer serializer,
        IEventTypeRegistry registry,
        UpcasterChain upcasters,
        string category,
        Func<string, T> factory,
        ISnapshotStore? snapshotStore = null,
        ISnapshotPolicy? snapshotPolicy = null)
    {
        _store = store;
        _serializer = serializer;
        _registry = registry;
        _upcasters = upcasters;
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
        var aggregate = _factory(id);
        var fromVersion = 0L;
        var exists = false;

        if (_snapshotStore is not null && aggregate is ISnapshotable snapshotable)
        {
            var snapshot = await _snapshotStore.LoadLatestAsync(streamId, ct).ConfigureAwait(false);
            if (snapshot is not null)
            {
                var state = _serializer.Deserialize(snapshotable.SnapshotStateType, snapshot.Payload);
                snapshotable.RestoreFromSnapshot(state, snapshot.StreamVersion);
                fromVersion = snapshot.StreamVersion + 1;
                exists = true;
            }
        }

        await foreach (var stored in _store
                           .ReadStreamAsync(streamId, ReadDirection.Forwards, fromVersion, ct: ct)
                           .ConfigureAwait(false))
        {
            exists = true;
            aggregate.ReplayEvent(Deserialize(stored));
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
        {
            var @event = events[i];
            data[i] = new EventData(
                Guid.NewGuid(),
                _registry.GetEventType(@event.GetType()),
                _serializer.Serialize(@event),
                Metadata: null,
                _serializer.ContentType);
        }

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

    private object Deserialize(StoredEvent stored)
    {
        var type = _registry.Resolve(stored.EventType);
        var @event = _serializer.Deserialize(type, stored.Data);
        return _upcasters.Upcast(@event);
    }
}
