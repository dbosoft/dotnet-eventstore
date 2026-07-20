# Bus integration & modular hosting

## The store ships no bus adapter, on purpose

A message bus does not preserve order and delivers at-least-once, so it is the **wrong source for a folded
read model** — ordered projections read the store's catch-up, because the store is the ordered log. The bus is
for what it is good at: commands and integration events between services. And *which* events you expose, in
what shape, is application policy the store cannot decide. So the store gives you the primitives and your app
wires its own bus.

### Outbox — the store as the single publisher

Publish reliably by making a catch-up subscription the only publisher: append to the store, and a subscription
forwards each new event to the bus. No dual-write (append + publish as two steps that a crash can split); live
and replay use one path.

```csharp
var outbox = EventStoreSubscription.ForReader(reader, checkpoints, "outbox", async (e, ct) =>
{
    if (materializer.TryMaterialize(e, out var @event))
        await bus.Publish(@event!);        // your bus, your choice of which events to expose
});
await outbox.RunAsync(ct);
```

### Inbound — a normal append

The inbound side is not a special primitive. A bus handler maps the message to an event and appends it:

```csharp
var data = materializer.Dematerialize(incomingMessage);
await store.AppendToStreamAsync(streamId, expectedVersion, [data]);
```

`StoreToBusRelay` (in the InMemory package) is a reference of the outbox shape with no real-bus dependency, for
tests and samples.

### The one thing that does cross the boundary: the change signal

The store does publish one thing over a bus if you opt in — a best-effort **change notification** (a stream
changed), so an out-of-process reader wakes on a write. That is a wake signal, not event delivery. See
[Read models & change notifications](read-models-and-notifications.md) and the `Dbosoft.EventStore.Rebus`
bridge.

## Modular hosting with Hosuto

The store's DI is designed for the per-component isolation that [Hosuto](https://github.com/dbosoft/Hosuto)
modules give you: each module has its own container, and the read-only keyed surfaces (`IEventReader`,
`IAllStreamReader`) let a read-side module be wired without a write handle. A writer module appends; a reader
module keeps its own read model current from the store; the only thing crossing between them is the change
ping over an in-memory bus. Because the modules are DI-separated, the same code splits into separate processes
by swapping the transport — nothing about the store changes.

### The Auction House sample

[`samples/AuctionHouse`](../samples/AuctionHouse) is a runnable walkthrough of exactly that: a live auction on
Hosuto modules over one shared store and an in-memory Rebus network.

- A **writer module** handles bid commands and appends events.
- A **UI module** opens a read-only view of the store, keeps its own read model current by re-deriving each
  changed lot (notify + re-derive), and serves a live page over Server-Sent Events.
- The store's change ping crosses the module boundary over the bus to wake the UI's projection.

It shows the whole chain end to end and is structured so a module could split into its own process by changing
only the transport and connection string.

Other samples under [`samples/`](../samples) show the smaller pieces (a bank account, an issue tracker, a
projection rebuild).

## Testing

`Dbosoft.EventStore.Testing` is a **public** package: an abstract `EventStoreConformanceTests` suite each
provider inherits via a fixture. It is capability-gated — a provider that does not implement, say, `ITruncatable`
skips those tests instead of faking them. It is public so a third-party provider can validate its
implementation against the same contract. The suite is built together with the abstractions: it *is* the
contract.

## Package layout

The contract stack is layered `Primitives → Abstractions → Core`, all under the base namespace
`Dbosoft.EventStore`.

| Package | Contents |
| --- | --- |
| `Dbosoft.EventStore.Primitives` | value types, records, enums, exceptions, capabilities |
| `Dbosoft.EventStore.Abstractions` | interfaces |
| `Dbosoft.EventStore` | serializer, type registry, upcasting, materializer, aggregate repository, subscription, DI |
| `Dbosoft.EventStore.InMemory` | in-memory reference provider (+ `StoreToBusRelay`) |
| `Dbosoft.EventStore.EfCore` | EF Core provider (shared) |
| `Dbosoft.EventStore.EfCore.Sqlite` / `.SqlServer` / `.PostgreSql` | EF Core dialect + `UseXxx` registration |
| `Dbosoft.EventStore.AzureTables` | Azure Table Storage provider |
| `Dbosoft.EventStore.Rebus` | cross-process change-notification bridge over Rebus (not a bus adapter) |
| `Dbosoft.EventStore.Testing` | the public conformance suite |

A provider package references Core so it can add a `UseXxx(this EventStoreBuilder)` DI extension. A third-party
provider that ships no DI sugar could reference Abstractions alone.
