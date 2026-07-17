namespace Dbosoft.EventStore;

/// <summary>
/// Destructive operations that shrink the log: truncation, deletion and scavenging. Optional and
/// deliberately separate from the append-only core; gated by <see cref="StoreCapabilities.Truncation"/>
/// and <see cref="StoreCapabilities.StreamDeletion"/>.
/// </summary>
public interface IStreamMaintenance
{
    /// <summary>
    /// Physically removes events with version below <paramref name="beforeVersion"/>. Version numbers
    /// are not renumbered — the versions below the boundary simply cease to exist. Refused unless a
    /// snapshot at or after <c>beforeVersion - 1</c> exists (so the state stays reconstructable),
    /// unless <paramref name="force"/> is set.
    /// </summary>
    /// <exception cref="NotSupportedByProviderException">
    /// No qualifying snapshot exists and <paramref name="force"/> is <c>false</c>.
    /// </exception>
    Task TruncateStreamAsync(
        string streamId,
        long beforeVersion,
        bool force = false,
        CancellationToken ct = default);

    /// <summary>Deletes a whole stream, softly or hard (see <see cref="DeleteMode"/>).</summary>
    Task DeleteStreamAsync(string streamId, DeleteMode mode, CancellationToken ct = default);

    /// <summary>Reclaims space from truncated and deleted streams and applies retention policies.</summary>
    Task<ScavengeReport> ScavengeAsync(ScavengeOptions? options = null, CancellationToken ct = default);
}
