using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore;
using Dbosoft.EventStore.EfCore.Sqlite;
using Dbosoft.EventStore.Sample.IssueTracker;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// A realistic host setup backed by an on-disk SQLite database, so state survives between runs. One
// "tracking" aggregate domain holds Issue streams (markers go to $system, keeping the aggregate
// streams clean). The EF Core provider creates a fresh context per operation from a factory — never a
// global context — and persists snapshots and the consumer checkpoint. Run it twice with `dotnet run`.

const string databaseFile = "issues.db";

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddEventStore(eventStore => eventStore
    .UseSqlite($"Data Source={databaseFile}")
    .AddDomain(SampleWorker.Tracking, tracking => tracking
        .AddEventTypesImplementing<IIssueEvent>()
        .AddAggregateRepository(Issue.Category, id => new Issue(id), new EveryNEventsSnapshotPolicy(10))));

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
