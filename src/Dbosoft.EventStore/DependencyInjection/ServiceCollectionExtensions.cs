using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Dbosoft.EventStore.DependencyInjection;

/// <summary>Entry point for registering the event store with dependency injection.</summary>
[PublicAPI]
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the serializer and returns a builder for choosing a provider, declaring domains and
    /// their event types, and adding aggregate repositories. Each declared domain is also exposed as a
    /// keyed <see cref="IEventStore"/> (keyed by domain name) for direct injection.
    /// </summary>
    public static EventStoreBuilder AddEventStore(this IServiceCollection services, Action<EventStoreBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new EventStoreBuilder(services);
        configure(builder);

        // Defaults registered after configure, so a caller's UseSerializer takes precedence.
        services.TryAddSingleton<IEventSerializer>(new SystemTextJsonEventSerializer());

        // Validate and publish domains once, after all configuration — so a domain's final options and
        // its full set of event types are seen, whichever methods contributed them.
        foreach (var domain in builder.DomainBuilders)
        {
            domain.Validate();
            services.AddSingleton(new RegisteredDomain(domain.Name, domain.Options));
            services.AddKeyedSingleton<IEventStore>(domain.Name, (sp, key) =>
                sp.GetRequiredService<IEventStoreProvider>().GetStore((string)key!));
        }

        return builder;
    }
}
