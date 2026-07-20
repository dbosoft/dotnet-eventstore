using AuctionHouse.Contracts;
using AuctionHouse.Hosting;
using AuctionHouse.ProjectionModule;
using AuctionHouse.UIModule;
using AuctionHouse.WriterModule;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore;
using Dbosoft.EventStore.EfCore.Sqlite;
using Dbosoft.Hosuto.Modules.Hosting;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// One shared in-memory SQLite database, kept alive for the whole process, is the event store both modules
// open. Creating the schema once up front means either module can start first without racing on it.
var connectionString = $"DataSource=file:auction-{Guid.NewGuid():N}?mode=memory&cache=shared";
using var keepAlive = new SqliteConnection(connectionString);
keepAlive.Open();
await CreateSchemaAsync(connectionString);

var builder = ModulesHost.CreateDefaultBuilder(args);
builder.UseEnvironment(Environments.Development);

// The shared (outer) container holds only cross-cutting resources: the in-memory bus network, the connection
// string, and the read model. Everything else stays private to each module's own container.
var shared = new ServiceCollection();
shared.AddAuctionSharedInfra(connectionString);
builder.UseServiceCollection(shared);

builder.HostModule<AuctionWriterModule>();
builder.HostModule<AuctionProjectionModule>();
builder.HostModule<AuctionUIModule>();

// Required once because a web module (the Blazor UI) is hosted. Hosuto serves the module's static web assets
// (including the framework's blazor.server.js) from its development manifest when the environment is Development.
const string url = "http://localhost:5005";
builder.UseAspNetCoreWithDefaults((_, webBuilder) => webBuilder.UseUrls(url));

var host = builder.Build();

// Also print the read model to the console whenever the projection updates it — the same shared read model
// the Blazor grid renders, so the whole chain (append -> change ping over the in-mem bus -> projection wakes
// -> fold -> read model) is visible in both places.
var readStore = host.Services.GetRequiredService<ILotReadStore>();
readStore.Changed += () => PrintBoard(readStore);

Console.WriteLine($"Auction house running — open {url} for the live Blazor grid. Three DI-separated modules,");
Console.WriteLine("one process: writer appends, projection folds $all woken by the change ping over the in-mem bus.");
Console.WriteLine("(Ctrl+C to stop.)");
await host.RunAsync();
return;

static async Task CreateSchemaAsync(string connectionString)
{
    // A throwaway one-off provider just to create the schema before the modules start; disposed immediately.
#pragma warning disable ASP0000 // building a provider here is intentional and not the application container
    var services = new ServiceCollection();
    services.AddEventStore(es => es.UseSqlite(connectionString).AddDomain(AuctionDomain.Name, _ => { }));
    await using var sp = services.BuildServiceProvider();
#pragma warning restore ASP0000
    await using var ctx = await sp.GetRequiredService<IDbContextFactory<EventStoreDbContext>>().CreateDbContextAsync();
    await ctx.Database.EnsureCreatedAsync();
}

static void PrintBoard(ILotReadStore store)
{
    Console.WriteLine();
    Console.WriteLine("=== Auction board ===");
    foreach (var lot in store.All())
    {
        var status = lot.Closed ? $"CLOSED (winner: {lot.Winner ?? "none"})" : "open";
        Console.WriteLine(
            $"  Lot {lot.LotId,-4} {lot.Title,-16} {lot.CurrentPrice,8:0.00}  bids {lot.BidCount}  high {lot.HighBidder ?? "-",-6} {status}");
    }
}
