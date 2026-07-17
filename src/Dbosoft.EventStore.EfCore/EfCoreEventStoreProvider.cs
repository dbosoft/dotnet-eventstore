using System.Collections.Concurrent;
using Dbosoft.EventStore.DependencyInjection;
using Microsoft.EntityFrameworkCore;

namespace Dbosoft.EventStore.EfCore;

/// <summary>
/// Backs each domain with an <see cref="EfCoreEventStore"/> over the shared model. The stores hold no
/// context — they open one per operation from the factory — so the provider is safe as a singleton.
/// </summary>
public sealed class EfCoreEventStoreProvider : IEventStoreProvider
{
    private readonly IDbContextFactory<EventStoreDbContext> _factory;
    private readonly TimeProvider _timeProvider;
    private readonly Dictionary<string, DomainOptions> _options;
    private readonly ConcurrentDictionary<string, EfCoreEventStore> _stores = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, EfCoreSnapshotStore> _snapshots = new(StringComparer.Ordinal);

    /// <summary>Creates the provider for the registered domains.</summary>
    public EfCoreEventStoreProvider(
        IDbContextFactory<EventStoreDbContext> factory,
        IEnumerable<RegisteredDomain> domains,
        TimeProvider timeProvider)
    {
        _factory = factory;
        _timeProvider = timeProvider;
        _options = domains
            .GroupBy(d => d.Name, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First().Options, StringComparer.Ordinal);
        Domains = _options.Keys.ToArray();
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Domains { get; }

    /// <inheritdoc />
    public IEventStore GetStore(string domain) =>
        _stores.GetOrAdd(domain, key =>
        {
            var options = _options.GetValueOrDefault(key) ?? new DomainOptions();
            return new EfCoreEventStore(_factory, key, _timeProvider, options.InBandMarkers, options.TombstoneRetention);
        });

    /// <inheritdoc />
    public ISnapshotStore GetSnapshotStore(string domain) =>
        _snapshots.GetOrAdd(domain, key => new EfCoreSnapshotStore(_factory, key, _timeProvider));
}
