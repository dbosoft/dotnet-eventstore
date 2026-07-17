namespace Dbosoft.EventStore.Aggregates;

/// <summary>
/// Base class for event-sourced aggregates. Derived types raise events with <see cref="Raise"/> and
/// fold both raised and replayed events in <see cref="When"/>. The repository drives rehydration via
/// <see cref="ReplayEvent"/> and persistence via <see cref="UncommittedEvents"/>.
/// </summary>
[PublicAPI]
public abstract class AggregateBase
{
    private readonly List<UncommittedEvent> _uncommitted = [];

    /// <summary>Creates the aggregate with its identity.</summary>
    protected AggregateBase(string id) => Id = id;

    /// <summary>The aggregate's identity within its category.</summary>
    public string Id { get; }

    /// <summary>
    /// The number of events applied so far — equivalently, the version of the stream this aggregate
    /// was folded from, including uncommitted events.
    /// </summary>
    public long Version { get; private set; }

    /// <summary>
    /// Events raised since the last load or save, awaiting persistence. Each carries a stable
    /// <see cref="UncommittedEvent.EventId"/> assigned at raise time, so retrying a failed save
    /// re-sends the same ids and the store treats it as an idempotent no-op rather than a duplicate.
    /// </summary>
    public IReadOnlyList<UncommittedEvent> UncommittedEvents => _uncommitted;

    /// <summary>Applies a new event to the aggregate and records it for persistence.</summary>
    protected void Raise(object @event)
    {
        When(@event);
        Version++;
        _uncommitted.Add(new UncommittedEvent(Guid.NewGuid(), @event));
    }

    /// <summary>Applies a historical event during rehydration, advancing the version but not recording it.</summary>
    public void ReplayEvent(object @event)
    {
        When(@event);
        Version++;
    }

    /// <summary>Clears the uncommitted events after they have been persisted.</summary>
    public void MarkCommitted() => _uncommitted.Clear();

    /// <summary>
    /// Sets the version to the one a restored snapshot reflects. For use by
    /// <see cref="ISnapshotable.RestoreFromSnapshot"/> implementations only.
    /// </summary>
    protected void SeedVersion(long version) => Version = version;

    /// <summary>Folds a single event into the aggregate's state. Must not have side effects.</summary>
    protected abstract void When(object @event);
}
