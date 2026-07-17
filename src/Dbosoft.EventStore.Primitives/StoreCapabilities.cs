namespace Dbosoft.EventStore;

/// <summary>
/// The optional features a provider supports beyond the core append/read contract. A caller reads
/// the store's declared capabilities and only uses an optional interface that appears among them;
/// the conformance suite tests only what is declared.
/// </summary>
[Flags]
public enum StoreCapabilities
{
    /// <summary>The core append/read contract only; no optional features.</summary>
    None = 0,

    /// <summary>Events across all streams can be read in a single ordered sequence.</summary>
    AllStreamReads = 1 << 0,

    /// <summary>Subscriptions can follow that sequence from a checkpoint.</summary>
    Subscriptions = 1 << 1,

    /// <summary>
    /// A subscription never silently skips an event: once it reports a position, every event
    /// ordered before that position has been delivered. Without this flag a subscription is
    /// complete only eventually, and handlers must tolerate redelivery and reordering within the
    /// store's safety window.
    /// </summary>
    ExactSubscriptions = 1 << 2,

    /// <summary>Events for several streams can be appended in one atomic operation.</summary>
    MultiStreamAtomicAppend = 1 << 3,

    /// <summary>A position can be located from a wall-clock timestamp.</summary>
    TimeNavigation = 1 << 4,

    /// <summary>Streams carry readable and writable metadata.</summary>
    StreamMetadata = 1 << 5,

    /// <summary>Events before a version can be permanently removed from a stream.</summary>
    Truncation = 1 << 6,

    /// <summary>Whole streams can be deleted.</summary>
    StreamDeletion = 1 << 7,

    /// <summary>More than the latest snapshot per stream is retained and readable.</summary>
    SnapshotHistory = 1 << 8,
}
