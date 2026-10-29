# Reading & subscriptions

## Reading a stream

`ReadStreamAsync` streams one stream's events, forward or backward, with paging:

```csharp
await foreach (var e in store.ReadStreamAsync("account-42"))
    Console.WriteLine($"{e.StreamVersion}: {e.EventType}");

// backward from the end, most recent 10
await foreach (var e in store.ReadStreamAsync("account-42", ReadDirection.Backwards, maxCount: 10))
    …
```

A missing stream yields nothing. `fromVersion` is 1-based and inclusive (`0` means from the start).

## The global `$all` sequence

On a provider that supports it, `IAllStreamReader.ReadAllAsync` reads every event of a domain in global order,
resuming from a `GlobalPosition`:

```csharp
if (store is IAllStreamReader all)
{
    await foreach (var e in all.ReadAllAsync(GlobalPosition.Start))
        Forward(e);
}
```

This is the primitive a read-model rebuild or a bus relay tails. Positions are exclusive: pass the position of
the last event you processed to resume after it.

### Time navigation

`ITimeNavigable.FindPositionAtAsync` resolves the `GlobalPosition` for a wall-clock time, so a fresh consumer
can replay `$all` from a point in time. It is correct on all providers but is currently an unindexed scan, so
treat it as an occasional admin operation, not a hot path.

```csharp
var from = await ((ITimeNavigable)store).FindPositionAtAsync(DateTimeOffset.UtcNow.AddDays(-1));
```

## Catch-up subscriptions

The store does **not** deliver events — there is no live push, no handler contract (that is the message bus's
job). What it offers is a resumable **catch-up read** of `$all`, wrapped by `EventStoreSubscription`: it reads
forward from a saved checkpoint, invokes a handler per event, and advances the checkpoint once the handler
succeeds — the shape a read-model rebuilder or a bus relay needs.

```csharp
var subscription = new EventStoreSubscription(
    store, checkpoints, "read-model",
    async (e, ct) => await projection.ApplyAsync(e, ct));

await subscription.RunAsync(cancellationToken);   // pumps and checkpoints until cancelled
```

- **`RunAsync`** loops: it pumps everything available, then waits (see [waking on writes](read-models-and-notifications.md))
  before pumping again. It does **not** retry on failure — an exception from a read or a handler ends the run
  and propagates, so the host owns restart/backoff.
- **`PumpAsync`** does one pass and returns how many events were dispatched — call it yourself if you want to
  drive the loop.
- **`StartingAtAsync(…, DateTimeOffset from, …)`** creates a subscription whose first run starts at a point in
  time (via `ITimeNavigable`); once it has checkpointed it resumes from the checkpoint.

Delivery is **at-least-once**: a crash between the handler and the checkpoint re-delivers the in-flight events,
so handlers must be idempotent. Exactly-once *effects* come from persisting the checkpoint transactionally with
the read model.

### Checkpoints

A subscription remembers how far it read through `ICheckpointStore`, keyed by the subscription name. Every
provider ships one (EF Core and Azure Tables persist it; in-memory keeps it in memory):

```csharp
public interface ICheckpointStore
{
    Task<GlobalPosition?> GetAsync(string name, CancellationToken ct = default);
    Task SetAsync(string name, GlobalPosition position, CancellationToken ct = default);
}

var checkpoints = serviceProvider.GetRequiredService<ICheckpointStore>();
```

To rebuild a read model from scratch, run a subscription with a **fresh** checkpoint name (or a fresh
in-memory checkpoint store) so it replays `$all` from the start.

### Read-only wiring

A subscription only needs to read, so you can wire a projection component without a write handle. Each domain
is also exposed as a keyed read-only `IAllStreamReader`; build the subscription over it with `ForReader`:

```csharp
var reader = serviceProvider.GetRequiredKeyedService<IAllStreamReader>("banking");
var subscription = EventStoreSubscription.ForReader(reader, checkpoints, "read-model", Handle);
```

## Completeness and `ExactCatchup`

Whether a catch-up read is **complete** depends on the provider, and is the `ExactCatchup` capability.

On the in-memory store, appends are serialized under one lock, so its `$all` is dense and gap-free — it
declares `ExactCatchup`, and the subscription simply forwards each event and advances the checkpoint.

On EF Core, an event's `$all` position is its **commit sequence**, taken as the last step before its
transaction commits, from one counter row whose lock the transaction keeps until the commit. The next
transaction takes its numbers only after this one is visible, so sequence order is commit order and a forward
read never passes an event that commits later — however long the transaction stays open, which matters when an
append is [enlisted](providers.md#transactional-enlistment) in a caller's transaction that goes on to do other
work. EF Core declares `ExactCatchup`. The lock is held only from that last step to the commit; a rollback rolls
the counter back with it.

On Azure Tables, a global position is assigned before the commit that makes an event visible, so a naive
forward read could skip an event that commits *below* a position it has already passed. It does **not** declare
`ExactCatchup`. The subscription closes the gap with a **safety window**: it does not deliver or checkpoint past
an event newer than `SafetyWindow`, so a not-yet-committed lower sibling is never skipped — completeness holds
provided no append stays uncommitted longer than the window (the price is that much delivery latency).

A time-started subscription (`StartingAtAsync`) finds its start by when events were *appended*, while `$all` on
EF Core is ordered by when they *committed*: an event appended before the start time but committed after a later
one can stand behind the start and be passed over. Start by position where every event counts.

On every store, the checkpoint advances at the end of a pump, so after a handler throws the next pump reads
the events before it again; de-duplication by `EventId` over a bounded window keeps them from reaching the
handler twice in the same process.

Tune it through `SubscriptionOptions`:

```csharp
new SubscriptionOptions
{
    PollInterval       = TimeSpan.FromSeconds(1),    // how often to poll when nothing is new (and the wake backstop)
    SafetyWindow       = TimeSpan.FromMilliseconds(500), // hold events newer than this on a non-exact store
    DeduplicationWindow = 1000,                       // EventIds remembered to drop re-reads
    StartFrom          = GlobalPosition.Start,        // where a fresh subscription begins
    TimeProvider       = TimeProvider.System,
    // ChangeNotifier / NotifyKey — see the read-models guide
}
```

On a store without `ExactCatchup`, a smaller safety window keeps latency low but leaves less margin for a slow
commit; size it for your worst-case commit latency under load. An exact store ignores it. If your read model is a per-stream fold rather than a globally-ordered projection,
you often do not need the ordered catch-up at all — see
[Read models & change notifications](read-models-and-notifications.md).
