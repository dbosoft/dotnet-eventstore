using Dbosoft.EventStore;
using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Sample.Projection;
using Dbosoft.EventStore.Serialization;
using Dbosoft.EventStore.Subscriptions;
using Microsoft.Extensions.DependencyInjection;

// How a read model actually consumes events: it tails the STORE's ordered $all catch-up subscription,
// applies each delta in order, and keeps whatever it wants — here a full statement with history. No bus:
// the store is the ordered log. Rebuilding is the same fold from the start.

const string domain = "banking";

var services = new ServiceCollection();
services.AddEventStore(es => es
    .UseInMemory()
    .AddDomain(domain, d => d
        .AddEventTypesImplementing<IAccountEvent>()
        .AddAggregateRepository(Account.Category, id => new Account(id))));
await using var provider = services.BuildServiceProvider();

// The projection half is wired read-only: a keyed $all reader, a materializer, and a checkpoint store —
// no write handle. That is the DI separation a later process split relies on. (Writes below go through the
// aggregate repository, which is a different component.)
var reader = provider.GetRequiredKeyedService<IAllStreamReader>(domain);
var materializer = provider.GetRequiredKeyedService<IEventMaterializer>(domain);
var checkpoints = provider.GetRequiredService<ICheckpointStore>();

var statements = new AccountStatements();

// The projection: materialize each stored event and fold it into the read model, in order.
Task Project(StoredEvent stored, CancellationToken ct)
{
    if (!SystemEventTypes.IsSystem(stored.EventType) && materializer.TryMaterialize(stored, out var @event))
        statements.Apply(stored.StreamId, @event);
    return Task.CompletedTask;
}

var subscription = EventStoreSubscription.ForReader(reader, checkpoints, "statements", Project);

// === 1. Produce events through the aggregate, into the store ===
Section("1. Produce events (aggregate → store)");
var alice = await SaveAsync(provider, account =>
{
    account.Deposit(100m);
    account.Withdraw(30m);
}, "alice", "Alice");
var bob = await SaveAsync(provider, account => account.Deposit(200m), "bob", "Bob");
Console.WriteLine($"aggregate folded its own state: {alice.Owner} → {alice.Balance}, {bob.Owner} → {bob.Balance}");

// === 2. Build the read model by tailing the store's ordered $all ===
Section("2. Read model from the store's ordered catch-up ($all) — no bus");
await subscription.PumpAsync();
PrintStatements();

// === 3. More events live; the subscription resumes from its checkpoint ===
Section("3. More events; the subscription resumes from its checkpoint");
await SaveAsync(provider, account => account.Deposit(50m), "alice");
await subscription.PumpAsync();
PrintStatements();

// === 4. Rebuild from zero — the same fold, from the start ===
Section("4. Rebuild the read model from the store (fresh checkpoint, from the start)");
statements.Clear();
var rebuild = EventStoreSubscription.ForReader(reader, new InMemoryCheckpointStore(), "rebuild", Project);
await rebuild.PumpAsync();
PrintStatements();
Console.WriteLine("Identical — and it kept the full history, which a latest-state feed could not.");

return;

async Task<Account> SaveAsync(IServiceProvider serviceProvider, Action<Account> act, string id, string? owner = null)
{
    await using var scope = serviceProvider.CreateAsyncScope();
    var repo = scope.ServiceProvider.GetRequiredService<IAggregateRepository<Account>>();
    var account = owner is null
        ? await repo.LoadAsync(id) ?? throw new InvalidOperationException($"'{id}' not found")
        : Account.Open(id, owner);
    act(account);
    await repo.SaveAsync(account);
    return account;
}

void PrintStatements()
{
    foreach (var (account, lines) in statements.Statements())
    {
        Console.WriteLine($"  {account}");
        foreach (var line in lines)
            Console.WriteLine($"      {line}");
    }
}

static void Section(string title)
{
    Console.WriteLine();
    Console.WriteLine($"=== {title} ===");
}
