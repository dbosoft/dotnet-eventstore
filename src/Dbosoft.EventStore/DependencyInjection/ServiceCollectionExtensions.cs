using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Dbosoft.EventStore.DependencyInjection;

/// <summary>Entry point for registering the event store with dependency injection.</summary>
[PublicAPI]
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the serializer, event type registry and upcaster chain, and returns a builder for
    /// choosing a provider, registering event types and adding aggregate repositories.
    /// </summary>
    public static EventStoreBuilder AddEventStore(this IServiceCollection services, Action<EventStoreBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var registry = new EventTypeRegistry();
        services.TryAddSingleton<IEventTypeRegistry>(registry);
        services.TryAddSingleton<IEventSerializer>(new SystemTextJsonEventSerializer());
        services.TryAddSingleton(sp => new UpcasterChain(sp.GetServices<IEventUpcaster>()));

        var builder = new EventStoreBuilder(services, registry);
        configure(builder);
        return builder;
    }
}
