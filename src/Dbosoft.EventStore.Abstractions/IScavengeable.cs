namespace Dbosoft.EventStore;

/// <summary>
/// A store that can reclaim space left behind by truncation and deletion. Optional — a provider supports
/// scavenging iff it implements this interface.
/// </summary>
[PublicAPI]
public interface IScavengeable
{
    /// <summary>Reclaims space from truncated and deleted streams and applies retention policies.</summary>
    Task<ScavengeReport> ScavengeAsync(ScavengeOptions? options = null, CancellationToken ct = default);
}
