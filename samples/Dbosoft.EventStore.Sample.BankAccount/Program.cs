using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Sample.BankAccount;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// Register the event store with the generic host, then let a hosted service
// drive the scenario. The banking events are grouped into their own domain and registered in bulk
// through their marker interface. Run it with `dotnet run`.

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddEventStore(eventStore => eventStore
    .UseInMemory()
    .AddDomain(BankAccountWorker.Domain, banking => banking
        .AddEventTypesImplementing<IBankingEvent>()
        .AddAggregateRepository("account", id => new BankAccount(id), new EveryNEventsSnapshotPolicy(5))));

builder.Services.AddHostedService<BankAccountWorker>();

await builder.Build().RunAsync();
