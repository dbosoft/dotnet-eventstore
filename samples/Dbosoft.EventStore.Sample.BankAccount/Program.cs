using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Sample.BankAccount;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// A realistic host setup: register the event store, then let a hosted service drive the scenario.
// Two domains — a "banking" aggregate domain (markers in $system) and an "access-log" domain that
// emits its markers in-band because its streams are read, not folded. A demo bus relays each
// domain's $all to read models. Run it with `dotnet run`.

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddEventStore(eventStore => eventStore
    .UseInMemory()
    .AddDomain(SampleWorker.Banking, banking => banking
        .AddEventTypesImplementing<IBankingEvent>()
        .AddAggregateRepository("account", id => new BankAccount(id), new EveryNEventsSnapshotPolicy(5)))
    .AddDomain(SampleWorker.AccessLog, accessLog => accessLog
        .EmitMarkersInBand()
        .AddEventTypesImplementing<IAccessLogEvent>()));

builder.Services.AddHostedService<SampleWorker>();

await builder.Build().RunAsync();
