using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rebus.Bus;

namespace Dbosoft.EventStore.Rebus;

/// <summary>Registers a <see cref="RebusEventRelay"/> as a hosted service.</summary>
[PublicAPI]
public static class RebusEventRelayServiceCollectionExtensions
{
    /// <summary>
    /// Runs a relay for a domain as a background service: it forwards the domain's <c>$all</c> onto the
    /// registered Rebus <c>IBus</c>. Register the event store (with the domain) and Rebus separately; this
    /// only wires the relay between them.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="domain">The domain whose <c>$all</c> stream is forwarded.</param>
    /// <param name="checkpointName">Checkpoint name; defaults to <c>{domain}:rebus</c>.</param>
    /// <param name="options">Optional relay tuning.</param>
    public static IServiceCollection AddRebusEventRelay(
        this IServiceCollection services,
        string domain,
        string? checkpointName = null,
        RebusRelayOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrEmpty(domain);

        var name = checkpointName ?? $"{domain}:rebus";
        services.AddHostedService(sp => new RebusEventRelayService(sp, domain, name, options));
        return services;
    }

    // Constructs the relay from the registered services (all singletons) and runs it until the host stops.
    private sealed class RebusEventRelayService : BackgroundService
    {
        private readonly RebusEventRelay _relay;

        public RebusEventRelayService(IServiceProvider services, string domain, string checkpointName, RebusRelayOptions? options)
        {
            if (services.GetRequiredKeyedService<IEventStore>(domain) is not IAllStreamReader reader)
                throw new NotSupportedByProviderException(
                    $"The store for domain '{domain}' does not support $all reads, which the Rebus relay requires.");

            _relay = new RebusEventRelay(
                reader,
                services.GetRequiredKeyedService<IEventMaterializer>(domain),
                services.GetRequiredService<ICheckpointStore>(),
                services.GetRequiredService<IBus>(),
                checkpointName,
                options);
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken) => _relay.RunAsync(stoppingToken);
    }
}
