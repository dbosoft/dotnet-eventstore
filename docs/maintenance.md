# Maintenance & retention

Deletion in this store means **actual deletion** — data is physically removed, not crypto-shredded. These are
optional operations, available on a provider that implements the matching interface (all shipped providers do).
Cast the store to `IStreamMaintenance` (the composite) or to the individual role interface.

```csharp
var maintenance = (IStreamMaintenance)store;   // ITruncatable + IStreamDeletable + IRedactable + IScavengeable
```

## Truncation

Drop the events below a version — the head of the stream is kept, the prefix is removed:

```csharp
await maintenance.TruncateStreamAsync("account-42", beforeVersion: 100);
```

Because a truncated prefix can only be reconstructed from a snapshot, truncation **refuses** unless a covering
snapshot exists at or after the boundary — pass `force: true` to override. This protects the write side of the
"snapshot becomes the source of truth after truncation" rule; the read side is protected by `LoadAsync`
throwing `SnapshotRequiredException` (see [Aggregates & snapshots](aggregates-and-snapshots.md)).

## Deletion

Delete a whole stream, soft or hard:

```csharp
await maintenance.DeleteStreamAsync("account-42", DeleteMode.Soft);   // events removed, stream can be recreated
await maintenance.DeleteStreamAsync("account-42", DeleteMode.Hard);   // tombstoned; the id cannot be reused
```

`Soft` removes the events but leaves the stream re-creatable (a later append starts it fresh). `Hard` tombstones
the stream so the id can never be reused.

## Redaction

Overwrite a single event's payload in place — same version, position and id, only the bytes change. For a GDPR
"erase this field" that must keep the stream's shape intact:

```csharp
await maintenance.RedactEventAsync("account-42", version: 7, newData: scrubbedBytes);
```

## Retention policies

Set a per-stream `StreamMetadata` retention policy through `IStreamMetadataStore`; the scavenger enforces it:

```csharp
var metadata = (IStreamMetadataStore)store;
await metadata.SetStreamMetadataAsync("access-log-2026", new StreamMetadata
{
    MaxCount = 10_000,                 // keep the newest 10k events
    MaxAge   = TimeSpan.FromDays(30),  // …and drop events older than 30 days
});
```

```csharp
public sealed record StreamMetadata
{
    public long? MaxCount { get; init; }
    public TimeSpan? MaxAge { get; init; }
    public IReadOnlyDictionary<string, string>? Custom { get; init; }   // your own key/values on the head
    public bool HasRetention => MaxCount is not null || MaxAge is not null;
}
```

## Scavenge

`ScavengeAsync` reclaims expired tombstones and, with retention enabled (the default), force-truncates each
stream to its policy. Run it on a schedule:

```csharp
var report = await maintenance.ScavengeAsync();                          // apply retention + reclaim tombstones
var report = await maintenance.ScavengeAsync(new ScavengeOptions { ApplyRetention = false }); // tombstones only

// report.StreamsProcessed, report.EventsRemoved
```

## Markers and tombstones

A destructive operation records a **marker** event so read models learn to forget or reproject —
`StreamTruncatedMarker`, `StreamDeletedMarker`, `EventRedactedMarker`, materialized as JSON regardless of the
domain's serializer (see [the materializer](serialization.md#the-materializer)). Where the marker lands is a
per-domain choice:

- **`$system` (default).** Markers go to the reserved `$system` stream, which shows up in the domain's `$all`.
  This keeps aggregate streams pure — a marker in a folded stream would take a version slot and break optimistic
  concurrency. The aggregate repository also skips `$`-typed events when folding, as a safety net.
- **In-band, opt-in for non-folded (log-like) domains.** With `EmitMarkersInBand()` on the domain, a *survivable*
  marker (truncate, redact) is written on the affected stream itself, so a per-stream reader sees the deletion in
  the stream's own flow. Whole-stream **deletion always uses `$system`** — the stream is gone, there is nowhere
  else to put it.

```csharp
.AddDomain("access-log", d => d
    .AddEventTypesImplementing<IAccessEvent>()
    .EmitMarkersInBand())
```

The `$system` stream is itself append-only, so under frequent retention it would grow without bound. Markers
are therefore **transient tombstones**: the scavenger removes them after `TombstoneRetention` (default 7 days,
`TimeProvider`-driven). The contract, plainly: a consumer must catch up within that window or it misses the
deletion notification. Tune it per domain:

```csharp
.AddDomain("access-log", d => d
    .AddEventTypesImplementing<IAccessEvent>()
    .WithTombstoneRetention(TimeSpan.FromDays(1)))
```
