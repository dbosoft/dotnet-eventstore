namespace Dbosoft.EventStore;

/// <summary>
/// Destructive operations: truncation, deletion, scavenging and redaction. Optional and deliberately
/// separate from the append-only core; gated by <see cref="StoreCapabilities.Truncation"/>,
/// <see cref="StoreCapabilities.StreamDeletion"/> and <see cref="StoreCapabilities.Redaction"/>.
/// Each destructive operation emits a system marker event into <c>$all</c> (see
/// <see cref="SystemEventTypes"/>) so read models over the bus learn to forget or reproject.
/// </summary>
[PublicAPI]
public interface IStreamMaintenance
{
    /// <summary>
    /// Physically removes events with version below <paramref name="beforeVersion"/>. Version numbers
    /// are not renumbered — the versions below the boundary simply cease to exist. Refused unless a
    /// snapshot at or after <c>beforeVersion - 1</c> exists (so the state can still be reconstructed),
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

    /// <summary>
    /// Overwrites the payload of a single event in place — its version, position and id are kept, only
    /// <see cref="StoredEvent.Data"/> (and metadata) change. This is PII erasure by redaction, not a
    /// deletion: the event's slot survives, so the stream shape is preserved. A
    /// <see cref="SystemEventTypes.EventRedacted"/> marker is emitted so read models reproject.
    /// </summary>
    /// <exception cref="StreamNotFoundException">The stream or the version does not exist.</exception>
    Task RedactEventAsync(
        string streamId,
        long version,
        ReadOnlyMemory<byte> newData,
        ReadOnlyMemory<byte>? newMetadata = null,
        CancellationToken ct = default);
}
