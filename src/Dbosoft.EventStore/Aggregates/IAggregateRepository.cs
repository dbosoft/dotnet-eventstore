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

    /// <summary>
    /// Persists several aggregates' uncommitted events in one atomic, all-or-nothing operation — either
    /// every aggregate's events commit or none do. Requires the backing store to support multi-stream
    /// atomic append; on a store that does not, this throws rather than fall back to a non-atomic loop.
    /// Each aggregate is checked under its own optimistic-concurrency version. Snapshots are written
    /// after the commit, per the policy, and uncommitted events are cleared on success.
    /// </summary>
    /// <exception cref="NotSupportedByProviderException">The store cannot append to multiple streams atomically.</exception>
    /// <exception cref="WrongExpectedVersionException">A stream changed since its aggregate was loaded.</exception>
    Task SaveAllAsync(IReadOnlyList<T> aggregates, CancellationToken ct = default);
}
