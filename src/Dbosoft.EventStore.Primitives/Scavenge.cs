namespace Dbosoft.EventStore;

/// <summary>Controls a scavenge pass that reclaims space from truncated and deleted streams.</summary>
[PublicAPI]
public sealed record ScavengeOptions
{
    /// <summary>
    /// Whether the scavenge applies each stream's retention policy (max count, max age from
    /// <see cref="StreamMetadata"/>), force-truncating streams past their limits. On by default; set false
    /// to reclaim only expired <c>$system</c> tombstones and leave stream events untouched. A provider
    /// without <c>IStreamMetadataStore</c> has no policies to apply, so this has no effect there.
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
