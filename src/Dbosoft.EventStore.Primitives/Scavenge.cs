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

    /// <summary>
    /// Reserved: an upper bound on streams processed in one pass, for chunking a large scavenge. No
    /// provider bounds its work by this yet; today a scavenge only reclaims expired <c>$system</c>
    /// tombstones, which is cheap enough not to need chunking.
    /// </summary>
    public int? MaxStreams { get; init; }
}

/// <summary>What a scavenge pass reclaimed.</summary>
/// <param name="StreamsProcessed">How many streams were examined.</param>
/// <param name="EventsRemoved">How many events were physically removed.</param>
[PublicAPI]
public sealed record ScavengeReport(int StreamsProcessed, long EventsRemoved);
