using Microsoft.Extensions.DependencyInjection;
using Rebus.Config;
using Rebus.Transport.InMem;

namespace AuctionHouse.Hosting;

/// <summary>Configures a module's own Rebus bus over the shared in-memory network.</summary>
public static class ModuleBus
{
    extension(IServiceCollection services)
    {
        /// <summary>A full bus with an input queue (receives messages), on the shared in-memory network.</summary>
        public IServiceCollection AddModuleBus(InMemNetwork network, string inputQueue) =>
            services.AddRebus(configure => configure
                .Transport(t => t.UseInMemoryTransport(network, inputQueue))
                .Logging(l => l.None()));

        /// <summary>A send-only bus (no input queue) for a module that only publishes/sends, on the shared network.</summary>
        public IServiceCollection AddOneWayModuleBus(InMemNetwork network) =>
            services.AddRebus(configure => configure
                .Transport(t => t.UseInMemoryTransportAsOneWayClient(network))
                .Logging(l => l.None()));
    }
}
