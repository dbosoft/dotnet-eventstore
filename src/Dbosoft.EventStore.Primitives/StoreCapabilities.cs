namespace Dbosoft.EventStore;

/// <summary>
/// The optional features a provider supports beyond the core append/read contract. A caller reads
/// the store's declared capabilities and only uses an optional interface that appears among them;
/// the conformance suite tests only what is declared.
/// </summary>
[Flags]
[PublicAPI]
public enum StoreCapabilities
{
    /// <summary>The core append/read contract only; no optional features.</summary>
    None = 0,

    /// <summary>
    /// Events across all streams in a domain can be read in a single ordered sequence — the basis
    /// for replay, read-model rebuilds and forwarding to a message bus.
    /// </summary>
    AllStreamReads = 1 << 0,

    /// <summary>
    /// A catch-up read over that sequence is exact: reading forward from a position never silently
    /// skips an event that will commit below the point already returned. Without this flag a
    /// catch-up read is complete only up to a bounded staleness window, so readers must tolerate
    /// re-reading and de-duplicate by <see cref="StoredEvent.EventId"/>.
    /// </summary>
    ExactCatchup = 1 << 2,

    /// <summary>
    /// A read of the global sequence sees an event as soon as its append returns — the global read is
    /// synchronous with the write. Without this flag the global read is served from an eventually-built
    /// index, so a just-appended event may not appear until the index catches up (a consumer that needs
    /// it now re-reads); per-stream reads are unaffected. Implies neither more nor less than
    /// <see cref="ExactCatchup"/> — one is about completeness under out-of-order commit, this about the
    /// visibility delay of the global read.
    /// </summary>
    SynchronousAllReads = 1 << 10,

    /// <summary>
    /// Reserved: events for several streams appended in one atomic operation. No append API for this
    /// exists yet, so no provider declares it; the flag is kept for when a multi-stream append is added.
    /// </summary>
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

    /// <summary>A single event's payload can be overwritten in place (PII erasure / redaction).</summary>
    Redaction = 1 << 9
}
