using AuctionHouse.Contracts;
using AuctionHouse.Hosting;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore.Sqlite;
using Dbosoft.EventStore.Rebus;
using Microsoft.Extensions.DependencyInjection;
using Rebus.Handlers;
using Rebus.Transport.InMem;

namespace AuctionHouse.WriterModule;

/// <summary>
/// The write-side Hosuto module. It owns its own DI container: the event store (opened against the shared
/// SQLite database), its own Rebus bus on the shared in-memory network, and the command handlers. Appending
/// an event makes the store publish a best-effort change ping over the bus via the Rebus notify bridge —
/// that is the only thing this module tells the projection module; it never ships the events themselves.
/// </summary>
public class AuctionWriterModule
{
    public void ConfigureServices(IServiceProvider sharedSp, IServiceCollection services)
    {
        var network = sharedSp.GetRequiredService<InMemNetwork>();
        var connectionString = sharedSp.GetRequiredService<AuctionConnectionString>().Value;

        // The write-side event store, plus the Rebus change-notification bridge: on append the store
        // publishes an EventStoreChanged ping instead of shipping events onto the bus.
        services.AddEventStore(es => es
            .UseSqlite(connectionString)
            .UseRebusNotifications()
            .AddDomain(AuctionDomain.Name, domain => domain
                .AddEventTypesImplementing<IAuctionEvent>()
                .AddAggregateRepository(Lot.Category, id => new Lot(id))));

        // This module's own bus (input queue "auction-writer") over the shared network, and its handlers.
        // Rebus resolves IHandleMessages<T> from the container, so plain DI registration is all it needs.
        services.AddModuleBus(network, QueueNames.Writer);
        services.AddScoped<AuctionCommandHandlers>();
        services.AddTransient<IHandleMessages<ListLotCommand>>(sp => sp.GetRequiredService<AuctionCommandHandlers>());
        services.AddTransient<IHandleMessages<PlaceBidCommand>>(sp => sp.GetRequiredService<AuctionCommandHandlers>());
        services.AddTransient<IHandleMessages<CloseLotCommand>>(sp => sp.GetRequiredService<AuctionCommandHandlers>());

        // Drives the demo: sends a few list/bid commands so the pipeline lights up on its own.
        services.AddHostedService<DemoSeeder>();
    }
}
