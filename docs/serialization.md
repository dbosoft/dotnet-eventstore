# Serialization & upcasting

The store's core knows only `ReadOnlyMemory<byte>` plus a `ContentType`. Turning bytes into typed events and
back is a separate layer, so if you only want a byte log you pay nothing for the rest.

## The serializer

`IEventSerializer` turns event and snapshot objects into bytes and back:

```csharp
public interface IEventSerializer
{
    ReadOnlyMemory<byte> Serialize(object value);
    object Deserialize(Type type, ReadOnlyMemory<byte> data);
}
```

The default is **System.Text.Json**. JSON is the default on purpose: the event log is the source of truth and
lives for years, so a readable, portable text format is a feature — inspecting rows, auditing, ops tooling.

Tune the JSON rules (naming policy, converters, unmapped-member handling) without writing a serializer:

```csharp
services.AddEventStore(es => es
    .UseSqlite(connectionString)
    .UseSystemTextJson(o =>
    {
        o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
        o.Converters.Add(new JsonStringEnumConverter());
    })
    .AddDomain("banking", d => d.AddEventTypesImplementing<IBankingEvent>()));
```

For speed and AOT/trim-safety without leaving JSON, pass a source-generated `JsonSerializerContext`:

```csharp
.UseSystemTextJson(MyJsonContext.Default)
```

To use a different format entirely (MessagePack, Protobuf, …), implement `IEventSerializer` and register it:

```csharp
.UseSerializer(new MyBinaryEventSerializer())
```

Serialization is a builder-level choice, so a hot domain could use a binary format while the rest stay JSON.

## Event types

The serializer needs to know which CLR type an `EventType` string maps to. Register a domain's event types on
its builder:

```csharp
.AddDomain("banking", d => d
    .AddEventType<MoneyDeposited>()                 // one type, under its type name
    .AddEventType<MoneyWithdrawn>("withdrawn")      // …or under a custom logical name
    .AddEventTypesImplementing<IBankingEvent>())    // …or every concrete type implementing a marker
```

`AddEventTypesImplementing<TMarker>()` scans the marker's assembly (or a given one) and registers every
concrete implementing type under its type name — the bulk, contract-based way to register a domain's events.

## Upcasting — evolving events

An event, once stored, is immutable. When the shape of an event needs to change, add an **upcaster** that
rewrites the old shape into the new one on the way out, so folds and handlers only ever see the current
version:

```csharp
public sealed class MoneyDepositedV1ToV2 : IEventUpcaster
{
    public bool CanUpcast(object @event) => @event is MoneyDepositedV1;
    public object Upcast(object @event)
    {
        var v1 = (MoneyDepositedV1)@event;
        return new MoneyDeposited(v1.Amount, Currency: "EUR");   // add the new field with a default
    }
}
```

Register upcasters per domain; they are applied in a chain until none matches, so you can stack V1→V2→V3:

```csharp
.AddDomain("banking", d => d
    .AddEventTypesImplementing<IBankingEvent>()
    .AddUpcaster<MoneyDepositedV1ToV2>()            // resolved from the container
    .AddUpcaster(new SomeOtherUpcaster()))          // …or an instance
```

Upcasting applies to **events**. Snapshot state is evolved differently (tolerant deserialization + best-effort
rebuild) — see [Aggregates & snapshots](aggregates-and-snapshots.md#schema-evolution-of-snapshot-state).

## The materializer

`IEventMaterializer` is the read-side/bridge primitive that maps a domain's events between stored bytes and
typed objects, in both directions. It is resolved from DI keyed by domain, and applies the serializer, the type
registry and the upcaster chain in one place — so read models, relays and the aggregate repository all share
one pipeline.

```csharp
public interface IEventMaterializer
{
    object Materialize(StoredEvent stored);                   // deserialize + upcast
    bool TryMaterialize(StoredEvent stored, out object? @event); // false for an unregistered type
    EventData Dematerialize(object @event, Guid? eventId = null, ReadOnlyMemory<byte>? metadata = null);
}

var materializer = serviceProvider.GetRequiredKeyedService<IEventMaterializer>("banking");

// stored bytes -> typed event
if (materializer.TryMaterialize(stored, out var @event))
    Apply(@event!);

// typed event -> EventData ready to append
var data = materializer.Dematerialize(new MoneyDeposited(100m, "EUR"));
```

`Materialize` also recognizes the reserved `$`-prefixed [maintenance markers](maintenance.md) and reads them as
JSON regardless of the domain's serializer, so a consumer handles them uniformly.
