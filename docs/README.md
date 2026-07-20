# Dbosoft.EventStore — documentation

A storage-agnostic event-sourcing library for .NET. Append events to a stream under optimistic
concurrency, read them back, fold them into aggregates, catch up over a global `$all` stream, and keep
read models current — on in-memory, EF Core (SQLite / SQL Server / PostgreSQL), or Azure Table Storage,
all behind one API.

It is a **store, not a framework**: there is no message bus, no projection host and no command dispatcher
in the box. The library gives you the durable write side, the read primitives (catch-up reads, a per-stream
reader, a change signal) and the building blocks (serialization, upcasting, an aggregate repository), and
leaves delivery and orchestration to your application.

Targets `net10.0`.

## Install

Add the provider package for your store; it brings the core in transitively.

```sh
dotnet add package Dbosoft.EventStore.EfCore.Sqlite   # or .SqlServer / .PostgreSql / .AzureTables / .InMemory
```

## Quick start

Define events and an aggregate, register a provider and a domain, then load and save.

```csharp
// 1. Events are plain records; an aggregate raises them and folds them into state.
public interface IIssueEvent;
public record IssueOpened(string Title, string Reporter) : IIssueEvent;
public record IssueResolved(string Resolution) : IIssueEvent;

public sealed class Issue(string id) : AggregateBase(id)
{
    public const string Category = "issue";
    public string Status { get; private set; } = "open";

    public static Issue Open(string id, string title, string reporter)
    {
        var issue = new Issue(id);
        issue.Raise(new IssueOpened(title, reporter));
        return issue;
    }

    public void Resolve(string resolution) => Raise(new IssueResolved(resolution));

    protected override void When(object @event)
    {
        switch (@event)
        {
            case IssueResolved: Status = "resolved"; break;
        }
    }
}
```

```csharp
// 2. Register the provider, a domain, its event types and an aggregate repository.
services.AddEventStore(es => es
    .UseSqlite("Data Source=issues.db")
    .AddDomain("tracking", d => d
        .AddEventTypesImplementing<IIssueEvent>()
        .AddAggregateRepository(Issue.Category, id => new Issue(id))));
```

```csharp
// 3. Inject IAggregateRepository<Issue> and use it.
var issue = Issue.Open("42", "Login is broken", "alice");
await repo.SaveAsync(issue);                 // appends under optimistic concurrency

var loaded = await repo.LoadAsync("42");
loaded!.Resolve("fixed in 1.2.3");
await repo.SaveAsync(loaded);                // throws WrongExpectedVersionException on a concurrent change
```

Swap `UseSqlite` for `UseInMemory`, `UseSqlServer`, `UseNpgsql`, or `UseAzureTables` — nothing else changes.

## Topics

| Guide | What it covers |
| --- | --- |
| [Concepts](concepts.md) | Streams, versions, optimistic concurrency, domains, `GlobalPosition`, the value types. |
| [Providers & capabilities](providers.md) | The provider matrix, how to register each, and the two-axis capability model. |
| [Aggregates & snapshots](aggregates-and-snapshots.md) | `AggregateBase`, the repository, snapshots, point-in-time loads. |
| [Serialization & upcasting](serialization.md) | The serializer, JSON defaults, schema evolution, the materializer. |
| [Reading & subscriptions](reading-and-subscriptions.md) | Reading streams, the `$all` catch-up, `EventStoreSubscription`, checkpoints, completeness. |
| [Read models & change notifications](read-models-and-notifications.md) | Keeping a read model current — the change signal, re-derive, read-only wiring, rebuild. |
| [Maintenance & retention](maintenance.md) | Truncation, deletion, redaction, scavenge, retention policies. |
| [Bus integration & modular hosting](bus-and-modules.md) | Why there is no bus adapter, the outbox shape, the Rebus change bridge, Hosuto modules. |
