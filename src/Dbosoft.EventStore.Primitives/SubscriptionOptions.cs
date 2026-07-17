namespace Dbosoft.EventStore;

/// <summary>
/// Tuning for a catch-up subscription. Which knobs a provider honours depends on how it bounds its
/// watermark: an exact provider (see <see cref="StoreCapabilities.ExactSubscriptions"/>) uses
/// <see cref="GapTimeout"/> to decide when a missing sequence number is abandoned rather than
/// awaited; a bounded-staleness provider uses <see cref="SafetyWindow"/> to stay far enough behind
/// the write front that in-flight writes cannot be skipped. The provider documents which it reads.
/// </summary>
[PublicAPI]
public sealed record SubscriptionOptions
{
    /// <summary>
    /// How long a gap in a dense sequence is awaited before it is treated as an aborted write and
    /// skipped. Honoured by exact providers.
    /// </summary>
    public TimeSpan GapTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How far behind the write front the subscription stays. Honoured by bounded-staleness
    /// providers to avoid skipping writes that have not yet become visible.
    /// </summary>
    public TimeSpan SafetyWindow { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>How many events to fetch per catch-up page.</summary>
    public int PageSize { get; init; } = 500;
}
