# Read models & change notifications

A read model is a projection of the event log kept in whatever shape a query needs. This guide covers how to
keep one current: the change signal, and the two ways to consume it.

## The change signal

`IChangeNotifier` is a best-effort **"a stream changed"** ping, so a reader can react on a write instead of
waiting out a poll interval — without a message bus. It carries only the id of the changed stream, and makes no
delivery or ordering guarantee: the store stays the source of truth, and re-reading (or polling) is the
backstop, so a missed ping only costs latency, never a change.

```csharp
public interface IChangeNotifier
{
    ValueTask NotifyAsync(string domain, string streamId, CancellationToken ct = default);
    IChangeWatcher Watch(string domain);
}

public interface IChangeWatcher : IDisposable
{
    ValueTask<string> WaitAsync(CancellationToken ct = default);   // the next changed stream id
}
```

A watcher **coalesces** repeated changes to the same stream but never drops a distinct changed stream between
reads, so a consumer sees every changed stream at least once.

The store fires the signal for you on every write — you do not call `NotifyAsync` yourself. Register a
notifier and it flows into the store:

```csharp
services.AddEventStore(es => es
    .UseSqlite(connectionString)
    .UseInProcessNotifications()                    // a writer and reader in the same process
    .AddDomain("auction", d => d.AddEventTypesImplementing<IAuctionEvent>()));
```

`UseInProcessNotifications()` covers a writer and reader that share the process. For a reader in a **different**
process, use the Rebus bridge (below).

## Two ways to consume it

### 1. Wake an ordered `$all` subscription

If your read model needs global order across streams (or you are relaying to a bus), keep the ordered catch-up
from [Reading & subscriptions](reading-and-subscriptions.md) and just hand it the notifier so it wakes on a
write instead of only polling:

```csharp
var options = new SubscriptionOptions
{
    ChangeNotifier = serviceProvider.GetRequiredService<IChangeNotifier>(),
    NotifyKey      = "auction",                     // the domain to watch
    PollInterval   = TimeSpan.FromSeconds(2),       // backstop for a missed ping
};
var subscription = EventStoreSubscription.ForReader(reader, checkpoints, "read-model", Handle, options);
await subscription.RunAsync(ct);
```

The subscription treats any signal as a wake and re-reads `$all` from its checkpoint — it ignores *which*
stream changed.

### 2. Notify + re-derive per stream (the simple, self-healing way)

Most read models are a **fold per stream** — one row per aggregate, each a function of that aggregate's events.
For those you do not need a global position, a checkpoint or a safety window. Watch the notifier, and when a
stream changes, re-read just that stream and re-fold it:

```csharp
using var watcher = notifier.Watch("auction");
while (!ct.IsCancellationRequested)
{
    var streamId = await watcher.WaitAsync(ct);          // "lot-42"
    var view = await Rederive(streamId, ct);             // read the stream, fold it
    readModel.Upsert(view);
}

async Task<LotView?> Rederive(string streamId, CancellationToken ct)
{
    LotView? view = null;
    await foreach (var stored in reader.ReadStreamAsync(streamId, ct: ct))
        if (materializer.TryMaterialize(stored, out var e))
            view = Fold(view, e);
    return view;
}
```

Because re-reading a stream is a pure function of committed state, this is **idempotent and self-healing**: a
duplicate or lost ping only affects latency, never correctness — re-deriving always converges to the current
state. There is no per-event cursor to keep in step. Pair it with a periodic full sweep (re-derive everything)
as a paranoia backstop for a lost ping.

The reader here is the keyed read-only per-stream surface, so the read side has no write handle:

```csharp
var reader       = serviceProvider.GetRequiredKeyedService<IEventReader>("auction");     // read one stream
var materializer = serviceProvider.GetRequiredKeyedService<IEventMaterializer>("auction");
var notifier     = serviceProvider.GetRequiredService<IChangeNotifier>();
```

**Which to use?** Use the ordered subscription (1) for exactly-once ordered *outbound* work — relaying events to
a bus in order — and for the rare read model whose correctness depends on cross-stream order. Use notify +
re-derive (2) for the common per-stream fold: it is simpler, needs no completeness machinery, and self-heals.

## Cross-process: the Rebus bridge

`Dbosoft.EventStore.Rebus` ships an `IChangeNotifier` that bridges the signal across processes over
[Rebus](https://github.com/rebus-org/Rebus). On a write it publishes a best-effort `EventStoreChanged` ping
(carrying the domain and stream id); a handler on the receiving side delivers it to that process's watchers. It
is a wake signal over the bus, deliberately **not** event delivery — the store stays the ordered log.

```csharp
// writer and reader processes both:
services.AddEventStore(es => es
    .UseSqlite(sharedConnectionString)
    .UseRebusNotifications()
    .AddDomain("auction", d => d.AddEventTypesImplementing<IAuctionEvent>()));

// the reader process, once at startup, subscribes to the ping:
await bus.SubscribeToEventStoreChangesAsync();
```

Your app owns the Rebus configuration (transport, and a container-resolved handler activator). The bridge is
one of many possible — a Redis pub/sub or a Postgres `LISTEN/NOTIFY` bridge would work the same way; it is
provided so a Rebus app need not build it. For a runnable example see the
[Auction House sample](bus-and-modules.md#the-auction-house-sample).

## Rebuilding

A read model is a pure fold, so it can be rebuilt from zero. For the subscription form, run it with a fresh
checkpoint so it replays `$all` from the start. For the re-derive form, clear the read model and re-derive
every stream (read `$all` once to discover the stream ids, then re-fold each). Either way the store is the
source of truth and the read model is disposable derived data.
