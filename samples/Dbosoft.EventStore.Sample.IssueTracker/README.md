# Issue Tracker sample (EF Core / SQLite)

An event-sourced issue tracker on the EF Core provider, backed by an **on-disk SQLite file**
(`issues.db`). Where the BankAccount sample shows the in-memory story, this one shows what real,
durable storage adds.

## Run it

```
dotnet run
```

Then **run it again** — and again. Each run opens a couple of new issues with randomly varied content
and touches an existing one, reading the database the previous runs wrote, so the board grows and looks
different every time.

## What it demonstrates

- **Durability** — the board projection rebuilds from `$all` starting at `GlobalPosition.Start`. Every
  run still lists the issues earlier runs created, because they were persisted to SQLite.
- **Durable checkpoint** — `ActivityNotifier` remembers how far it read in the EF Core
  `ICheckpointStore`. The first run notifies about every event; each later run resumes from the saved
  position and reports only what was added since.
- **Persisted snapshots** — a chatty issue crosses the ten-event snapshot interval, so a later run
  loads it from a snapshot plus its tail rather than replaying from the first event.
- **Redaction (PII erasure)** — a comment leaks an email; `RedactEventAsync` overwrites the payload in
  place (same version, position and id). A fresh read never sees the address, and the `$event-redacted`
  marker lets already-caught-up consumers forget the value they cached.
- **Race-free append** — optimistic concurrency is enforced by a real unique index on
  `(domain, stream, version)`, not an in-process lock.

The context is created per operation from an `IDbContextFactory` — never held globally — so the store
is safe to share. To start over, delete `issues.db`.
