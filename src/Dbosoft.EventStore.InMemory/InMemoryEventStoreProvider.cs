using System.Collections.Concurrent;
using Dbosoft.EventStore.DependencyInjection;

namespace Dbosoft.EventStore.InMemory;

/// <summary>
/// Backs each domain with its own <see cref="InMemoryEventStore"/>, created on first access. Because
/// the stores are independent instances, domains are fully isolated: separate streams, versions and
/// global sequences.
/// </summary>
[PublicAPI]
public sealed class InMemoryEventStoreProvider : IEventStoreProvider
{
    private readonly ConcurrentDictionary<string, InMemoryEventStore> _stores = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DomainOptions> _options;
    private readonly TimeProvider _timeProvider;

    /// <summary>Creates the provider for the registered domains.</summary>
    public InMemoryEventStoreProvider(IEnumerable<RegisteredDomain> domains, TimeProvider timeProvider)
    {
        _timeProvider = timeProvider;
        _options = domains
            .GroupBy(d => d.Name, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First().Options, StringComparer.Ordinal);
        Domains = _options.Keys.ToArray();
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Domains { get; }

    /// <inheritdoc />
    public IEventStore GetStore(string domain) => Store(domain);

    /// <inheritdoc />
    public ISnapshotStore GetSnapshotStore(string domain) => Store(domain).Snapshots;

    private InMemoryEventStore Store(string domain) =>
        _stores.GetOrAdd(domain, key =>
        {
            if (!_options.TryGetValue(key, out var options))
                throw new ArgumentException($"Domain '{key}' is not registered.", nameof(domain));
            return new InMemoryEventStore(_timeProvider, options.InBandMarkers, options.TombstoneRetention);
        });
}
