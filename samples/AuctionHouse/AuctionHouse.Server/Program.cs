using AuctionHouse.Hosting;
using AuctionHouse.UIModule;
using AuctionHouse.WriterModule;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore;
using Dbosoft.EventStore.EfCore.Sqlite;
using Dbosoft.Hosuto.Modules.Hosting;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

// One shared in-memory SQLite database, kept alive for the whole process, is the event store both modules
// open (each against its own store instance). Creating the schema once up front means either module can start
// first without racing on it.
var connectionString = $"DataSource=file:auction-{Guid.NewGuid():N}?mode=memory&cache=shared";
await using var keepAlive = new SqliteConnection(connectionString);
keepAlive.Open();
await CreateSchemaAsync(connectionString);

var builder = ModulesHost.CreateDefaultBuilder(args);
builder.UseEnvironment(Environments.Development);

// The shared (outer) container holds only cross-cutting infrastructure: the in-memory bus network and the
// connection string each module opens its own store against. Everything else — read model, bus, handlers —
// stays private to each module's own container.
var shared = new ServiceCollection();
shared.AddAuctionSharedInfra(connectionString);
builder.UseServiceCollection(shared);

builder.HostModule<AuctionWriterModule>();
builder.HostModule<AuctionUIModule>();

// Required once because a web module (the UI) is hosted; pick the URL to serve it on.
const string url = "http://localhost:5005";
builder.UseAspNetCoreWithDefaults((_, webBuilder) => webBuilder.UseUrls(url));

Console.WriteLine($"Auction house running — open {url} for the live grid. Two DI-separated modules, one");
Console.WriteLine("process: the writer appends; the UI module's own projection folds $all, woken by the change");
Console.WriteLine("ping the writer's store publishes over the in-memory bus. (Ctrl+C to stop.)");

await builder.Build().RunAsync();
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
