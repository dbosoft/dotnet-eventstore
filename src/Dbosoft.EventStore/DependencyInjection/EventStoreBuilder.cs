using System.Text.Json;
using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace Dbosoft.EventStore.DependencyInjection;

/// <summary>
/// Fluent configuration for the event store, returned by <c>AddEventStore</c>. Providers extend this
/// with <c>UseXxx</c> methods that register an <see cref="IEventStore"/> (and optional interfaces).
/// </summary>
[PublicAPI]
public sealed class EventStoreBuilder
{
    private readonly EventTypeRegistry _registry;

    internal EventStoreBuilder(IServiceCollection services, EventTypeRegistry registry)
    {
        Services = services;
        _registry = registry;
    }

    /// <summary>The underlying service collection, for providers and advanced wiring.</summary>
    public IServiceCollection Services { get; }

    /// <summary>Registers an event type under a logical name (defaulting to the type's name).</summary>
    public EventStoreBuilder AddEventType<TEvent>(string? eventType = null)
    {
        _registry.Register(typeof(TEvent), eventType ?? typeof(TEvent).Name);
        return this;
    }

    /// <summary>
    /// Replaces the serializer used for events and snapshots with a custom instance. The default is
    /// <see cref="SystemTextJsonEventSerializer"/>; plug in your own to change the wire format or
    /// mapping rules.
    /// </summary>
    public EventStoreBuilder UseSerializer(IEventSerializer serializer)
    {
        Services.AddSingleton(serializer);
        return this;
    }

    /// <summary>Replaces the serializer with one resolved from the container.</summary>
    public EventStoreBuilder UseSerializer<TSerializer>() where TSerializer : class, IEventSerializer
    {
        Services.AddSingleton<IEventSerializer, TSerializer>();
        return this;
    }

    /// <summary>
    /// Keeps the System.Text.Json serializer but adjusts its options — naming policy, converters,
    /// unmapped-member handling — so fields can be mapped differently without writing a serializer.
    /// </summary>
    public EventStoreBuilder UseSystemTextJson(Action<JsonSerializerOptions> configure)
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        configure(options);
        Services.AddSingleton<IEventSerializer>(new SystemTextJsonEventSerializer(options));
        return this;
    }

    /// <summary>Registers an upcaster applied to events on read.</summary>
    public EventStoreBuilder AddUpcaster<TUpcaster>() where TUpcaster : class, IEventUpcaster
    {
        Services.AddSingleton<IEventUpcaster, TUpcaster>();
        return this;
    }

    /// <summary>Registers an upcaster instance applied to events on read.</summary>
    public EventStoreBuilder AddUpcaster(IEventUpcaster upcaster)
    {
        Services.AddSingleton(upcaster);
        return this;
    }

    /// <summary>
    /// Registers an <see cref="IAggregateRepository{T}"/> for an aggregate type stored under the given
    /// stream category. Pass a <paramref name="snapshotPolicy"/> to enable snapshotting (requires an
    /// <see cref="ISnapshotStore"/> from the provider).
    /// </summary>
    public EventStoreBuilder AddAggregateRepository<T>(
        string category,
        Func<string, T> factory,
        ISnapshotPolicy? snapshotPolicy = null)
        where T : AggregateBase
    {
        Services.AddScoped<IAggregateRepository<T>>(sp => new AggregateRepository<T>(
            sp.GetRequiredService<IEventStore>(),
            sp.GetRequiredService<IEventSerializer>(),
            sp.GetRequiredService<IEventTypeRegistry>(),
            sp.GetRequiredService<UpcasterChain>(),
            category,
            factory,
            sp.GetService<ISnapshotStore>(),
            snapshotPolicy));
        return this;
    }
}
