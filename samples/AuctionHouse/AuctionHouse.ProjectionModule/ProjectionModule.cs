using AuctionHouse.Contracts;
using AuctionHouse.Hosting;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore.Sqlite;
using Dbosoft.EventStore.Rebus;
using Microsoft.Extensions.DependencyInjection;
using Rebus.Transport.InMem;

namespace AuctionHouse.ProjectionModule;

/// <summary>
/// The read-side Hosuto module. Its own container holds a <b>read-only</b> view of the shared event store and
/// its own Rebus bus. It never writes: it tails the store's ordered <c>$all</c> stream and folds it into the
/// shared read model. Because it is a separate module (separate container), it shares no in-process notifier
/// with the writer — the writer's change ping reaches it only over the bus, via the Rebus notify bridge.
/// </summary>
public class AuctionProjectionModule
{
    public void ConfigureServices(IServiceProvider sharedSp, IServiceCollection services)
    {
        var network = sharedSp.GetRequiredService<InMemNetwork>();
        var connectionString = sharedSp.GetRequiredService<AuctionConnectionString>().Value;

        // The same store, opened read-only here: only the event types are registered (no aggregate), and the
        // Rebus notify bridge supplies the IChangeNotifier the subscription waits on.
        services.AddEventStore(es => es
            .UseSqlite(connectionString)
            .UseRebusNotifications()
            .AddDomain(AuctionDomain.Name, domain => domain
                .AddEventTypesImplementing<IAuctionEvent>()));

        // This module's own bus (input queue "auction-projection"); UseRebusNotifications already registered
        // the EventStoreChanged handler that releases the subscription's waiter when a ping arrives.
        services.AddModuleBus(network, QueueNames.Projection);

        // The shared read model this module folds into, and the runner that does the folding.
        services.AddSingleton(sharedSp.GetRequiredService<ILotReadStore>());
        services.AddHostedService<ProjectionRunner>();
    }
}
