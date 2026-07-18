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
    /// become visible <i>below</i> a position a reader has already passed — a forward-only reader misses it
    /// permanently. De-duplicating by <see cref="StoredEvent.EventId"/> only removes duplicates from a
    /// re-read; it does not recover a miss. A consumer needing completeness on such a store must re-read
    /// from behind its checkpoint (and dedupe the overlap); nothing in the store does that automatically.
    /// </summary>
    ExactCatchup = 1 << 0,

    /// <summary>More than the latest snapshot per stream is retained and readable.</summary>
    SnapshotHistory = 1 << 1
}
