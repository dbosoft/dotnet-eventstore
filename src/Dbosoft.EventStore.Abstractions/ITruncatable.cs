namespace Dbosoft.EventStore;

/// <summary>
/// A store whose streams can be truncated: events below a version are physically removed. Optional —
/// a provider supports truncation iff it implements this interface. Emits a
/// <see cref="SystemEventTypes.StreamTruncated"/> marker so read models learn to forget.
/// </summary>
[PublicAPI]
public interface ITruncatable
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
}
