namespace Dbosoft.EventStore;

/// <summary>
/// Behavioural guarantees a provider makes about operations it already offers — the answers the type
/// system cannot give. Whether an <i>operation</i> is available is a structural fact (the store
/// implements the matching interface: <c>IAllStreamReader</c>, <c>ITimeNavigable</c>, <c>ITruncatable</c>,
/// and so on); these flags describe how an available operation <i>behaves</i>, so a consumer can pick a
/// faster path, a costlier-but-correct path, or fail fast.
/// </summary>
[Flags]
[PublicAPI]
public enum StoreCapabilities
{
    /// <summary>No behavioural guarantees beyond the core contract.</summary>
    None = 0,

    /// <summary>
    /// A catch-up read of the global sequence is exact: reading forward from a position never skips an
    /// event that commits below the point already returned. A provider declares it only when a position is
    /// not assigned before the commit that makes the event visible (the in-memory store, whose append is
    /// atomic under one lock). Without this flag, positions are handed out before commit, so an event can
    /// become visible <i>below</i> a position a reader has already passed — a naive forward-only reader
    /// would miss it. A consumer therefore needs a bounded safety window (deliver only events older than
    /// the window, so an in-flight lower sibling is not skipped) to be complete on such a store; the core
    /// subscription applies one, at the price of that much staleness, and de-duplicating by
    /// <see cref="StoredEvent.EventId"/> makes the window's overlapping re-reads idempotent.
    /// </summary>
    ExactCatchup = 1 << 0,

    /// <summary>More than the latest snapshot per stream is retained and readable.</summary>
    SnapshotHistory = 1 << 1
}
