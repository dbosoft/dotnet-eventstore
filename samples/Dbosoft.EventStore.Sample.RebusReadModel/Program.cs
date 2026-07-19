using System.Globalization;
using Dbosoft.EventStore;
using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Rebus;
using Dbosoft.EventStore.Sample.RebusReadModel;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Transport.InMem;

// This sample shows the two ways a read model can consume events, side by side:
//   • the DIRECT typed event off the bus — a live notification, fine with out-of-order, at-least-once delivery;
//   • the same message as a SIGNAL, with the ordered content read from the STORE — exact and rebuildable.
// It runs a self-contained in-memory Rebus bus, so there is no external broker to set up.

const string domain = "banking";

var services = new ServiceCollection();
services.AddEventStore(es => es
    .UseInMemory()
    .AddDomain(domain, d => d
        .AddEventTypesImplementing<IAccountEvent>()
        .AddAggregateRepository(Account.Category, id => new Account(id))));
await using var provider = services.BuildServiceProvider();

var store = provider.GetRequiredKeyedService<IEventStore>(domain);
var materializer = provider.GetRequiredKeyedService<IEventMaterializer>(domain);
var checkpoints = provider.GetRequiredService<ICheckpointStore>();

// The read model and the ordered dispatcher that keeps it correct from the store.
var readModel = new AccountBalances();
var dispatcher = new OrderedEventDispatcher(store, materializer,
    (@event, meta, _) =>
    {
        readModel.Apply(@event, meta);
        return Task.CompletedTask;
    });

var activityLog = new List<string>();
var handled = new int[1];

// A self-contained in-memory Rebus bus subscribed to the account events.
var activator = new BuiltinHandlerActivator();
activator.Register(() => new AccountBusHandler(dispatcher, activityLog, handled));
using var bus = Configure.With(activator)
    .Logging(l => l.None()) // keep the bus's own logging out of the sample's output
    .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "read-model"))
    .Start();
await bus.Subscribe<AccountOpened>();
await bus.Subscribe<MoneyDeposited>();
await bus.Subscribe<MoneyWithdrawn>();

// === 1. Produce events through the aggregate, into the store ===
Section("1. Produce events (aggregate → store)");
await using (var scope = provider.CreateAsyncScope())
{
    var repo = scope.ServiceProvider.GetRequiredService<IAggregateRepository<Account>>();

    var alice = Account.Open("alice", "Alice");
    alice.Deposit(100m);
    alice.Withdraw(30m);
    alice.Deposit(50m);
    await repo.SaveAsync(alice);

    var bob = Account.Open("bob", "Bob");
    bob.Deposit(200m);
    await repo.SaveAsync(bob);

    // The aggregate folds its own state as it raises events — the read model below reaches the same numbers.
    Console.WriteLine($"{alice.Owner}'s account (account-{alice.Id}): opened, +100, -30, +50  → balance {alice.Balance}");
    Console.WriteLine($"{bob.Owner}'s account (account-{bob.Id}): opened, +200            → balance {bob.Balance}");
}

// === 2. Live via Rebus: the relay forwards $all onto the bus; both consumers react ===
Section("2. Live via Rebus (relay $all → bus → consumers)");
var relay = new RebusEventRelay((IAllStreamReader)store, materializer, checkpoints, bus, "banking->rebus");
var published = await relay.PumpAsync();
await WaitUntilAsync(() => Volatile.Read(ref handled[0]) >= published);

Console.WriteLine("(a) Direct off the bus — a live activity feed; order and duplicates do not matter here:");
lock (activityLog)
    activityLog.ForEach(entry => Console.WriteLine($"    {entry}"));
Console.WriteLine("(b) Read model, applied from the store in order — this must be exact:");
PrintBalances(readModel);

// === 3. Why the store is the authority: the bus delivers scrambled and twice ===
Section("3. Same events, but the bus misbehaves (scrambled + duplicated)");
var scrambledModel = new AccountBalances();
var scrambledDispatcher = new OrderedEventDispatcher(store, materializer,
    (@event, meta, _) =>
    {
        scrambledModel.Apply(@event, meta);
        return Task.CompletedTask;
    });

// account-alice has four versions; pretend the bus delivered their signals out of order and some twice.
long[] scrambled = [3, 1, 4, 2, 1, 3];
Console.WriteLine($"signals for account-alice arrive as: {string.Join(", ", scrambled.Select(v => "v" + v))}");
foreach (var version in scrambled)
    await scrambledDispatcher.DispatchAsync(new EventStoreMetadata("account-alice", version, GlobalPosition.Start, Guid.Empty, ""));

Console.WriteLine("Read model is still correct — the dispatcher fills gaps from the store and skips duplicates:");
PrintBalances(scrambledModel);

// === 4. Rebuild the read model from the store ===
Section("4. Rebuild the read model from the store ($all catch-up)");
readModel.Clear();
await foreach (var stored in ((IAllStreamReader)store).ReadAllAsync(GlobalPosition.Start))
{
    if (SystemEventTypes.IsSystem(stored.EventType) || !materializer.TryMaterialize(stored, out var @event))
        continue;
    readModel.Apply(@event,
        new EventStoreMetadata(stored.StreamId, stored.StreamVersion, stored.Position, stored.EventId, stored.EventType));
}
Console.WriteLine("Rebuilt from zero by replaying the store — identical to the live read model:");
PrintBalances(readModel);

return;

static void Section(string title)
{
    Console.WriteLine();
    Console.WriteLine($"=== {title} ===");
}

static void PrintBalances(AccountBalances model)
{
    foreach (var (account, balance) in model.Snapshot())
        Console.WriteLine($"    {account}: {balance.ToString("0.00", CultureInfo.InvariantCulture)}");
}

static async Task WaitUntilAsync(Func<bool> condition)
{
    for (var attempt = 0; attempt < 250 && !condition(); attempt++)
        await Task.Delay(20);
}
