using System.Text;
using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore;
using Dbosoft.EventStore.EfCore.Sqlite;
using Dbosoft.EventStore.Sample.IssueTracker;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// A realistic host setup backed by an on-disk SQLite database, so state survives between runs. One
// "tracking" aggregate domain holds Issue streams (markers go to $system, keeping the aggregate
// streams clean). The EF Core provider creates a fresh context per operation from a factory — never a
// global context — and persists snapshots and the consumer checkpoint. Run it twice with `dotnet run`.

// UTF-8 without a BOM: the box-drawing and arrow glyphs need UTF-8, but Encoding.UTF8 would emit a
// byte-order mark that shows up as a stray character at the start of the output.
Console.OutputEncoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);

var builder = Host.CreateApplicationBuilder(args);

// The sample narrates itself to the console via Report; keep the framework's own logging out of the way.
builder.Logging.SetMinimumLevel(LogLevel.Warning);

builder.Services.AddEventStore(eventStore => eventStore
    .UseSqlite($"Data Source={SampleWorker.DatabaseFile}")
    .AddDomain(SampleWorker.Tracking, tracking => tracking
        .AddEventTypesImplementing<IIssueEvent>()
        .AddAggregateRepository(Issue.Category, id => new Issue(id), new EveryNEventsSnapshotPolicy(SampleWorker.SnapshotInterval))));

builder.Services.AddHostedService<SampleWorker>();

var host = builder.Build();

// Ensure the schema exists before the worker runs. A real app would use EF Core migrations instead.
await using (var scope = host.Services.CreateAsyncScope())
{
    var factory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<EventStoreDbContext>>();
    await using var context = await factory.CreateDbContextAsync();
    await context.Database.EnsureCreatedAsync();
}

await host.RunAsync();
