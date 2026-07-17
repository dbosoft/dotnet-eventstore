using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Sample.BankAccount;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// A realistic host setup: register the event store with the generic host, then let a hosted service
// drive the scenario. Everything the store needs — provider, event types, aggregate repository — is
// configured in one fluent chain. Run it with `dotnet run`.

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddEventStore(eventStore => eventStore
    .UseInMemory()
    .AddEventType<AccountOpened>()
    .AddEventType<Deposited>()
    .AddEventType<Withdrawn>()
    .AddAggregateRepository("account", id => new BankAccount(id), new EveryNEventsSnapshotPolicy(5)));

builder.Services.AddHostedService<BankAccountWorker>();

await builder.Build().RunAsync();
