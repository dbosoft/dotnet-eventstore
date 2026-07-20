# Aggregates & snapshots

The store works with raw events, but most applications fold a stream into an **aggregate**. The optional
aggregate layer (in the core package) gives you a base class and a repository, so you never touch positions or
expected versions by hand.

## Writing an aggregate

Derive from `AggregateBase`, raise events from your methods, and fold them into state in `When`. An aggregate
decides what may happen and records it as events; it never touches the store.

```csharp
public sealed class Account(string id) : AggregateBase(id)
{
    public const string Category = "account";       // stream id is "account-{id}"

    public string Owner { get; private set; } = "";
    public decimal Balance { get; private set; }

    public static Account Open(string id, string owner)
    {
        var account = new Account(id);
        account.Raise(new AccountOpened(owner));
        return account;
    }

    public void Deposit(decimal amount) => Raise(new MoneyDeposited(amount));

    public void Withdraw(decimal amount)
    {
        if (amount > Balance)
            throw new InvalidOperationException($"'{Id}' cannot withdraw {amount}; balance is {Balance}.");
        Raise(new MoneyWithdrawn(amount));
    }

    protected override void When(object @event)
    {
        switch (@event)
        {
            case AccountOpened e:    Owner = e.Owner; break;
            case MoneyDeposited e:   Balance += e.Amount; break;
            case MoneyWithdrawn e:   Balance -= e.Amount; break;
        }
    }
}
```

`Raise` appends the event to the aggregate's list of uncommitted changes and calls `When` so the in-memory
state stays current. `When` runs both for freshly raised events and for events replayed on load, so keep it
free of side effects.

## The repository

Register a repository per aggregate type, giving its stream category and a factory:

```csharp
services.AddEventStore(es => es
    .UseSqlite(connectionString)
    .AddDomain("banking", d => d
        .AddEventTypesImplementing<IAccountEvent>()
        .AddAggregateRepository(Account.Category, id => new Account(id))));
```

Then inject `IAggregateRepository<Account>`:

```csharp
public interface IAggregateRepository<T> where T : AggregateBase
{
    Task<T?> LoadAsync(string id, CancellationToken ct = default);
    Task<T?> LoadAsOfAsync(string id, DateTimeOffset asOf, CancellationToken ct = default);
    Task SaveAsync(T aggregate, CancellationToken ct = default);
    Task SaveAllAsync(IReadOnlyList<T> aggregates, CancellationToken ct = default);
}
```

- **`LoadAsync`** reads the stream (via the latest snapshot when snapshotting is on), folds it and returns the
  aggregate, or `null` if the stream does not exist.
- **`SaveAsync`** appends the aggregate's uncommitted events under optimistic concurrency (the version it was
  loaded at), then clears them. A concurrent change throws `WrongExpectedVersionException`.
- **`LoadAsOfAsync`** rebuilds the aggregate as it stood at a past time, using the provider's time navigation
  (`ITimeNavigable`). Requires a provider that supports it.
- **`SaveAllAsync`** persists several aggregates **atomically** — all-or-nothing — using the provider's
  `IMultiStreamAppend`. On a provider that lacks it (Azure Tables), it throws `NotSupportedByProviderException`
  rather than silently degrading to a non-atomic loop.

## Snapshots

A long stream is expensive to replay from its first event. A **snapshot** is a memoized fold of the stream —
never the source of truth, always rebuildable by replaying — so a load can start from the latest snapshot plus
the events after it.

Make an aggregate snapshottable by implementing `ISnapshotable`:

```csharp
public sealed class Account(string id) : AggregateBase(id), ISnapshotable
{
    private sealed record State(string Owner, decimal Balance);

    public Type SnapshotStateType => typeof(State);
    public object CreateSnapshotState() => new State(Owner, Balance);

    public void RestoreFromSnapshot(object state, long version)
    {
        var s = (State)state;
        Owner = s.Owner;
        Balance = s.Balance;
        // The repository then replays only the events after `version`.
    }
    // … methods and When as before …
}
```

Turn snapshotting on by passing a policy when registering the repository:

```csharp
.AddAggregateRepository(Account.Category, id => new Account(id), new EveryNEventsSnapshotPolicy(100))
```

- `EveryNEventsSnapshotPolicy(n)` snapshots whenever the version crosses a multiple of `n`.
- `NoSnapshotPolicy.Instance` never snapshots (the default when no policy is given).
- Implement `ISnapshotPolicy.ShouldSnapshot(versionBefore, versionAfter)` for your own rule.

Snapshotting needs a provider snapshot store (all providers have one). Snapshots are keyed by
`(streamId, streamVersion)`, so on a provider that declares `StoreCapabilities.SnapshotHistory` more than one
is kept and the load can fall back to an older snapshot if the newest fails to deserialize.

### Schema evolution of snapshot state

Snapshot state is not upcasted (events are — see [Serialization & upcasting](serialization.md)). Instead:

- **Tolerant deserialization** — the serializer ignores unknown members and defaults missing ones, so additive
  changes to the state just work.
- **Best-effort load** — if a snapshot still fails to deserialize, the repository discards it and rebuilds from
  the event stream. A snapshot can never break a load.

The one exception is **after truncation** (see [Maintenance](maintenance.md)): once the events below a snapshot
have been dropped, that snapshot *becomes* the source of truth for the removed prefix. If it is then unusable,
`LoadAsync` fails loudly with `SnapshotRequiredException` rather than folding a silently wrong aggregate.
