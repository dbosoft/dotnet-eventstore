namespace Dbosoft.EventStore.Aggregates;

/// <summary>Loads and saves an event-sourced aggregate as a stream of events.</summary>
/// <typeparam name="T">The aggregate type.</typeparam>
[PublicAPI]
public interface IAggregateRepository<T> where T : AggregateBase
{
    /// <summary>
    /// Rehydrates the aggregate with the given id, from its latest snapshot plus subsequent events
    /// where available, or <c>null</c> if the stream does not exist.
    /// </summary>
    Task<T?> LoadAsync(string id, CancellationToken ct = default);

    /// <summary>
    /// Persists the aggregate's uncommitted events under optimistic concurrency, then writes a
    /// snapshot if the policy calls for one. Clears the uncommitted events on success.
    /// </summary>
    /// <exception cref="WrongExpectedVersionException">The stream changed since the aggregate was loaded.</exception>
    Task SaveAsync(T aggregate, CancellationToken ct = default);
}
