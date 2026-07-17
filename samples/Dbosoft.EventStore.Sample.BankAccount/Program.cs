using Dbosoft.EventStore;
using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Sample.BankAccount;
using Microsoft.Extensions.DependencyInjection;

// This sample exercises the whole public API surface against the in-memory provider:
// registration, appends under optimistic concurrency, snapshot-based rehydration, an $all
// subscription, and time navigation. Run it with `dotnet run`.

var services = new ServiceCollection();

// 1. Pick a provider, then register event types and an aggregate repository. A snapshot is
//    written every 5 events, so a busy account rehydrates from a snapshot plus the tail.
services.AddInMemoryEventStore();
services.AddEventStore(builder => builder
    .AddEventType<AccountOpened>()
    .AddEventType<Deposited>()
    .AddEventType<Withdrawn>()
    .AddAggregateRepository("account", id => new BankAccount(id), new EveryNEventsSnapshotPolicy(5)));

await using var provider = services.BuildServiceProvider();
var repository = provider.GetRequiredService<IAggregateRepository<BankAccount>>();

// 2. A live subscription over the global sequence — the basis for read models and projections.
var subscription = provider.GetRequiredService<ISubscriptionSource>()
    .SubscribeToAll(GlobalPosition.Start, new ConsoleProjection());

// 3. Open an account and record some activity, each Save persisting under optimistic concurrency.
var account = BankAccount.Open("0001", "Ada Lovelace");
account.Deposit(1000m);
account.Withdraw(250m);
await repository.SaveAsync(account);

for (var i = 0; i < 4; i++)
{
    var reloaded = await repository.LoadAsync("0001");
    reloaded!.Deposit(100m);
    await repository.SaveAsync(reloaded);
}

// 4. Rehydrate and show the folded state. With 7 events this load crossed the snapshot threshold.
var current = await repository.LoadAsync("0001");
Console.WriteLine($"\nAccount {current!.Id} owned by {current.Owner}");
Console.WriteLine($"Balance: {current.Balance:C} at version {current.Version}");

// 5. Optimistic concurrency in action: two callers load the same version, both write, one loses.
var first = await repository.LoadAsync("0001");
var second = await repository.LoadAsync("0001");
first!.Deposit(10m);
second!.Deposit(20m);
await repository.SaveAsync(first);
try
{
    await repository.SaveAsync(second);
    Console.WriteLine("Unexpected: the stale write succeeded.");
}
catch (WrongExpectedVersionException ex)
{
    Console.WriteLine(
        $"\nConcurrency conflict as expected: expected v{ex.ExpectedVersion}, stream was at v{ex.ActualVersion}.");
}

// 6. Time navigation: find a checkpoint and replay the global stream from the beginning.
var navigable = (ITimeNavigable)provider.GetRequiredService<IEventStore>();
var reader = (IAllStreamReader)provider.GetRequiredService<IEventStore>();
var fromStart = await navigable.FindPositionAtAsync(DateTimeOffset.MinValue);
var count = 0;
await foreach (var _ in reader.ReadAllAsync(fromStart))
    count++;
Console.WriteLine($"\nGlobal stream holds {count} events in total.");

// Give the async subscription a moment to drain before shutting down.
await Task.Delay(200);
await subscription.DisposeAsync();

/// <summary>A trivial projection that prints each event as the subscription delivers it.</summary>
internal sealed class ConsoleProjection : IEventHandler
{
    public Task HandleAsync(StoredEvent @event, CancellationToken ct = default)
    {
        Console.WriteLine($"  [{@event.Position}] {@event.StreamId} v{@event.StreamVersion}: {@event.EventType}");
        return Task.CompletedTask;
    }
}
