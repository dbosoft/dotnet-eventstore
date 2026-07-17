using System.Collections.Concurrent;
using Dbosoft.EventStore.DependencyInjection;

namespace Dbosoft.EventStore.InMemory;

/// <summary>
/// Backs each domain with its own <see cref="InMemoryEventStore"/>, created on first access. Because
/// the stores are independent instances, domains are fully isolated: separate streams, versions,
/// global sequences and subscriptions.
/// </summary>
[PublicAPI]
public sealed class InMemoryEventStoreProvider : IEventStoreProvider
{
    private readonly ConcurrentDictionary<string, InMemoryEventStore> _stores = new(StringComparer.Ordinal);
    private readonly TimeProvider _timeProvider;

    /// <summary>Creates the provider for the registered domains.</summary>
    public InMemoryEventStoreProvider(IEnumerable<RegisteredDomain> domains, TimeProvider timeProvider)
    {
        _timeProvider = timeProvider;
        Domains = domains.Select(d => d.Name).Distinct(StringComparer.Ordinal).ToArray();
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Domains { get; }

    /// <inheritdoc />
    public IEventStore GetStore(string domain) => Store(domain);

    /// <inheritdoc />
    public ISnapshotStore GetSnapshotStore(string domain) => Store(domain).Snapshots;

    private InMemoryEventStore Store(string domain) =>
        _stores.GetOrAdd(domain, _ => new InMemoryEventStore(_timeProvider));
}
