using System.Reflection;
using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace Dbosoft.EventStore.DependencyInjection;

/// <summary>
/// Configures one domain: the event types stored in it, their upcasters, and the aggregate
/// repositories over its streams. Event types and upcasters are scoped to the domain.
/// </summary>
[PublicAPI]
public sealed class DomainBuilder
{
    private readonly List<Func<IServiceProvider, IEventUpcaster>> _upcasters = [];
    private readonly EventTypeRegistry _registry = new();

    internal DomainBuilder(string name, IServiceCollection services)
    {
        Name = name;
        Services = services;
    }

    /// <summary>The domain's name.</summary>
    public string Name { get; }

    /// <summary>The underlying service collection.</summary>
    public IServiceCollection Services { get; }

    /// <summary>Registers a single event type under a logical name (defaulting to the type's name).</summary>
    public DomainBuilder AddEventType<TEvent>(string? eventType = null)
    {
        _registry.Register(typeof(TEvent), eventType ?? typeof(TEvent).Name);
        return this;
    }

    /// <summary>
    /// Registers every concrete type implementing <typeparamref name="TMarker"/> as an event type,
    /// each under its type name. Scans <paramref name="assembly"/>, or the marker's own assembly when
    /// none is given — the bulk, contract-based way to register a domain's events.
    /// </summary>
    public DomainBuilder AddEventTypesImplementing<TMarker>(Assembly? assembly = null)
    {
        var marker = typeof(TMarker);
        foreach (var type in (assembly ?? marker.Assembly).GetTypes())
        {
            if (type is { IsAbstract: false, IsInterface: false, IsGenericTypeDefinition: false }
                && marker.IsAssignableFrom(type))
            {
                _registry.Register(type, type.Name);
            }
        }

        return this;
    }

    /// <summary>Registers an upcaster (resolved from the container) applied to this domain's events.</summary>
    public DomainBuilder AddUpcaster<TUpcaster>() where TUpcaster : class, IEventUpcaster
    {
        _upcasters.Add(sp => ActivatorUtilities.CreateInstance<TUpcaster>(sp));
        return this;
    }

    /// <summary>Registers an upcaster instance applied to this domain's events.</summary>
    public DomainBuilder AddUpcaster(IEventUpcaster upcaster)
    {
        _upcasters.Add(_ => upcaster);
        return this;
    }

    /// <summary>
    /// Registers an <see cref="IAggregateRepository{T}"/> for an aggregate type stored under the given
    /// stream category within this domain. Pass a <paramref name="snapshotPolicy"/> to enable
    /// snapshotting (requires a snapshot store from the provider).
    /// </summary>
    public DomainBuilder AddAggregateRepository<T>(
        string category,
        Func<string, T> factory,
        ISnapshotPolicy? snapshotPolicy = null)
        where T : AggregateBase
    {
        var domain = Name;
        var registry = _registry;
        var upcasterFactories = _upcasters;

        Services.AddScoped<IAggregateRepository<T>>(sp =>
        {
            var provider = sp.GetRequiredService<IEventStoreProvider>();
            var upcasters = new UpcasterChain(upcasterFactories.Select(create => create(sp)));
            var snapshots = snapshotPolicy is not null ? provider.GetSnapshotStore(domain) : null;

            return new AggregateRepository<T>(
                provider.GetStore(domain),
                sp.GetRequiredService<IEventSerializer>(),
                registry,
                upcasters,
                category,
                factory,
                snapshots,
                snapshotPolicy);
        });

        return this;
    }
}
