namespace Dbosoft.EventStore.Rebus;

/// <summary>Tunes a <see cref="RebusEventRelay"/>.</summary>
[PublicAPI]
public sealed record RebusRelayOptions
{
    /// <summary>How long <see cref="RebusEventRelay.RunAsync"/> waits after an empty pump before polling again.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Whether to forward <c>$system</c> markers (stream truncated/deleted, event redacted) so read models
    /// can prune. On by default; set false to forward only domain events.
    /// </summary>
    public bool PublishSystemMarkers { get; init; } = true;

    /// <summary>Clock used for the poll delay; defaults to the system clock. Injected for testing.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}
