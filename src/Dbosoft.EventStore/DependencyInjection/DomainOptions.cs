namespace Dbosoft.EventStore.DependencyInjection;

/// <summary>Per-domain policy for how destructive operations surface and how long tombstones live.</summary>
[PublicAPI]
public sealed record DomainOptions
{
    /// <summary>
    /// When <c>true</c>, a survivable destructive op (truncate, redact) writes its marker **in-band**
    /// on the affected stream instead of the reserved <c>$system</c> stream. Only safe for domains
    /// whose streams are *read*, not folded as aggregates — an in-band marker would take a version slot
    /// and break an aggregate's optimistic concurrency. Whole-stream deletion always uses <c>$system</c>
    /// (the stream is gone). Default <c>false</c>.
    /// </summary>
    public bool InBandMarkers { get; init; }

    /// <summary>
    /// How long <c>$system</c> tombstones are kept before a scavenge may remove them. Consumers must
    /// catch up within this window or they miss the deletion notification. Default 7 days.
    /// </summary>
    public TimeSpan TombstoneRetention { get; init; } = TimeSpan.FromDays(7);
}
