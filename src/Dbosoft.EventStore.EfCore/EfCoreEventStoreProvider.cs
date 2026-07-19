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
    private readonly IChangeNotifier? _changeNotifier;
    private readonly Dictionary<string, DomainOptions> _options;
    private readonly ConcurrentDictionary<string, EfCoreEventStore> _stores = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, EfCoreSnapshotStore> _snapshots = new(StringComparer.Ordinal);

    /// <summary>Creates the provider for the registered domains, optionally signalling changes to subscriptions.</summary>
    public EfCoreEventStoreProvider(
        IDbContextFactory<EventStoreDbContext> factory,
        IEnumerable<RegisteredDomain> domains,
        TimeProvider timeProvider,
        IChangeNotifier? changeNotifier = null)
    {
        _factory = factory;
        _timeProvider = timeProvider;
        _changeNotifier = changeNotifier;
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
            if (!_options.TryGetValue(key, out var options))
                throw new ArgumentException($"Domain '{key}' is not registered.", nameof(domain));
            return new EfCoreEventStore(_factory, key, _timeProvider, options.InBandMarkers, options.TombstoneRetention, _changeNotifier);
        });

    /// <inheritdoc />
    public ISnapshotStore GetSnapshotStore(string domain)
    {
        if (!_options.ContainsKey(domain))
            throw new ArgumentException($"Domain '{domain}' is not registered.", nameof(domain));
        return _snapshots.GetOrAdd(domain, key => new EfCoreSnapshotStore(_factory, key, _timeProvider));
    }
}
