namespace Dbosoft.EventStore;

/// <summary>Controls a scavenge pass that reclaims space from truncated and deleted streams.</summary>
[PublicAPI]
public sealed record ScavengeOptions
{
    /// <summary>
    /// Reserved for a future retention engine that will apply per-stream policies (max count, max age)
    /// during a scavenge. No provider acts on it yet; today a scavenge only reclaims expired
    /// <c>$system</c> tombstones.
    /// </summary>
    public bool ApplyRetention { get; init; } = true;

    /// <summary>Upper bound on streams processed in this pass; <c>null</c> for no limit.</summary>
    public int? MaxStreams { get; init; }
}

/// <summary>What a scavenge pass reclaimed.</summary>
/// <param name="StreamsProcessed">How many streams were examined.</param>
/// <param name="EventsRemoved">How many events were physically removed.</param>
[PublicAPI]
public sealed record ScavengeReport(int StreamsProcessed, long EventsRemoved);
