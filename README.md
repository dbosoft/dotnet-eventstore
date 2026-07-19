# dotnet-eventstore

![.NET](https://img.shields.io/badge/.NET-10.0-512BD4)
![License](https://img.shields.io/badge/license-MIT-blue)

A storage-agnostic event sourcing library for .NET. Append events to a stream under optimistic
concurrency, read them back, fold them into aggregates, and catch up over a global `$all` stream — on
in-memory, EF Core (SQLite / SQL Server / PostgreSQL), or Azure Table Storage, all behind one API.

## Providers

| Package | Store |
| --- | --- |
| `Dbosoft.EventStore.InMemory` | In-memory (tests, samples, single process) |
| `Dbosoft.EventStore.EfCore.Sqlite` | SQLite |
| `Dbosoft.EventStore.EfCore.SqlServer` | SQL Server |
| `Dbosoft.EventStore.EfCore.PostgreSql` | PostgreSQL |
| `Dbosoft.EventStore.AzureTables` | Azure Table Storage |

Every provider passes the same conformance suite. Optional features — global reads, time navigation,
truncation, deletion, redaction, multi-stream atomic append, stream metadata — are separate interfaces a
provider implements only when its store can support them, so you always know what a store actually does.

## Install

Add the provider package for your store; it brings the core in transitively.

```sh
dotnet add package Dbosoft.EventStore.EfCore.Sqlite
```

## Usage

### 1. Define events and an aggregate

Events are plain records. An aggregate decides what may happen (its methods), raises events, and folds
them into state in `When`. It never touches the store.

```csharp
public interface IIssueEvent;
public record IssueOpened(string Title, string Reporter) : IIssueEvent;
public record IssueAssigned(string Assignee) : IIssueEvent;
public record IssueResolved(string Resolution) : IIssueEvent;

public sealed class Issue(string id) : AggregateBase(id)
{
    public const string Category = "issue";
    public string Status { get; private set; } = "open";

    public static Issue Report(string id, string title, string reporter)
    {
        var issue = new Issue(id);
        issue.Raise(new IssueOpened(title, reporter));
        return issue;
    }

    public void AssignTo(string assignee) => Raise(new IssueAssigned(assignee));
    public void Resolve(string resolution) => Raise(new IssueResolved(resolution));

    protected override void When(object @event)
    {
        switch (@event)
        {
            case IssueAssigned: Status = "assigned"; break;
            case IssueResolved: Status = "resolved"; break;
        }
    }
}
```

### 2. Register

Pick a provider, declare a domain and its event types, and add an aggregate repository.

```csharp
services.AddEventStore(es => es
    .UseSqlite("Data Source=issues.db")
    .AddDomain("tracking", d => d
        .AddEventTypesImplementing<IIssueEvent>()
        .AddAggregateRepository(Issue.Category, id => new Issue(id))));
```

Swap `UseSqlite` for `UseInMemory`, `UseSqlServer`, `UseNpgsql`, or `UseAzureTables`.

### 3. Load and save

Inject `IAggregateRepository<Issue>`.

```csharp
var issue = Issue.Report("42", "Login is broken", "alice");
issue.AssignTo("bob");
await repo.SaveAsync(issue);              // appends under optimistic concurrency

var loaded = await repo.LoadAsync("42");
loaded.Resolve("fixed in 1.2.3");
await repo.SaveAsync(loaded);            // throws WrongExpectedVersionException on a concurrent change
```

### Working with streams directly

Skip aggregates and use the store — each domain is also a keyed `IEventStore`.

```csharp
var store = serviceProvider.GetRequiredKeyedService<IEventStore>("tracking");

await store.AppendToStreamAsync("issue-42", ExpectedVersion.NoStream,
    [new EventData(Guid.NewGuid(), "IssueOpened", payloadBytes)]);

await foreach (var e in store.ReadStreamAsync("issue-42"))
    Console.WriteLine($"{e.StreamVersion}: {e.EventType}");
```

### Catch up over `$all`

Forward or project every event across a domain, resuming from a durable checkpoint. Delivery is
at-least-once, so handlers should be idempotent.

```csharp
var subscription = new EventStoreSubscription(
    store, checkpoints, "read-model",
    async (e, ct) => await projection.ApplyAsync(e, ct));

await subscription.RunAsync(cancellationToken); // pumps and checkpoints until cancelled
```

A read model is a pure fold of that ordered stream, so it can keep whatever it needs — including history —
and be rebuilt from zero by replaying `$all` from the start.
[`samples/Dbosoft.EventStore.Sample.Projection`](samples/Dbosoft.EventStore.Sample.Projection) is a runnable
walkthrough: a subscription-fed account statement, a live resume from its checkpoint, and a rebuild.

### A message bus?

The store is deliberately bus-agnostic. Ordered projections read the store's catch-up above — a message bus
(Rebus, NServiceBus, MassTransit) does **not** preserve order and delivers at-least-once, so it is the wrong
source for a folded read model. Use a bus for what it is good at — commands and integration events between
services — and wire it in your app: an outbox is the same `EventStoreSubscription` with your own
`bus.Publish(...)` for the events you choose to expose; an inbound handler is `materializer.Dematerialize(...)`
then `AppendToStreamAsync(...)`. The library ships no bus adapter on purpose.

## More

- **Snapshots** — implement `ISnapshotable` on an aggregate and pass a snapshot policy to
  `AddAggregateRepository`, so a long stream doesn't replay from its first event.
- **Point in time** — `repo.LoadAsOfAsync(id, timestamp)` rebuilds an aggregate as it stood at a past time.
- **Atomic multi-stream save** — `repo.SaveAllAsync(aggregates)` commits several aggregates all-or-nothing
  (on a store that supports it; otherwise it throws rather than write non-atomically).
- **Retention** — set `StreamMetadata` (max count / max age) and run `ScavengeAsync` to truncate a stream
  to its policy.

Runnable samples are in [`samples/`](samples). The storage layouts, the capability model, and the
trade-offs behind each provider are written up in [`docs/design.md`](docs/design.md).

## License

MIT © dbosoft GmbH
