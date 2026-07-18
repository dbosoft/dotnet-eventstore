namespace Dbosoft.EventStore;

/// <summary>
/// Behavioural guarantees a provider makes about operations it already offers — the answers the type
/// system cannot give. Whether an <i>operation</i> is available is a structural fact (the store
/// implements the matching interface: <c>IAllStreamReader</c>, <c>ITimeNavigable</c>, <c>ITruncatable</c>,
/// and so on); these flags describe how an available operation <i>behaves</i>, so a consumer can pick a
/// faster path, a costlier-but-correct path, or fail fast. The conformance suite gates its behavioural
/// assertions on them.
/// </summary>
[Flags]
[PublicAPI]
public enum StoreCapabilities
{
    /// <summary>No behavioural guarantees beyond the core contract.</summary>
    None = 0,

    /// <summary>
    /// A catch-up read of the global sequence is exact: reading forward from a position never silently
    /// skips an event that will commit below the point already returned. Without this flag a catch-up
    /// read is complete only up to a bounded staleness window, so a subscription must tolerate re-reading
    /// and de-duplicate by <see cref="StoredEvent.EventId"/>.
    /// </summary>
    ExactCatchup = 1 << 0,

    /// <summary>More than the latest snapshot per stream is retained and readable.</summary>
    SnapshotHistory = 1 << 1
}
