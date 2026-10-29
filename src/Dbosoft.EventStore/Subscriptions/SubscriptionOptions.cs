namespace Dbosoft.EventStore.Subscriptions;

/// <summary>Tunes an <see cref="EventStoreSubscription"/>.</summary>
[PublicAPI]
public sealed record SubscriptionOptions
{
    /// <summary>Where to begin when no checkpoint has been saved yet. Defaults to the start of the log.</summary>
    public GlobalPosition StartFrom { get; init; } = GlobalPosition.Start;

    /// <summary>
    /// How many recently delivered <see cref="StoredEvent.EventId"/>s to remember for de-duplication. A store
    /// without <see cref="StoreCapabilities.ExactCatchup"/> re-reads across its safety window; any store re-reads
    /// the events before a handler that threw. A re-read must not span more events than this or a duplicate could
    /// slip through; the default covers a generous overlap.
    /// </summary>
    public int DeduplicationWindow { get; init; } = 4096;

    /// <summary>How long <see cref="EventStoreSubscription.RunAsync"/> waits after an empty pump before polling again.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Optional change signal so <see cref="EventStoreSubscription.RunAsync"/> wakes on a write instead of
    /// waiting out <see cref="PollInterval"/>. When set together with <see cref="NotifyKey"/>, an empty pump
    /// waits for the signal <i>or</i> the poll interval, whichever comes first — so the poll stays the
    /// backstop. Best-effort: a missed signal only costs latency.
    /// </summary>
    public IChangeNotifier? ChangeNotifier { get; init; }

    /// <summary>The domain (or key) to wait on via <see cref="ChangeNotifier"/>; required for it to take effect.</summary>
    public string? NotifyKey { get; init; }

    /// <summary>
    /// On a store <b>without</b> <see cref="StoreCapabilities.ExactCatchup"/>, the subscription does not
    /// deliver (or checkpoint past) an event recorded more recently than this — an event whose position was
    /// assigned but whose commit may not yet be visible could have a lower-positioned sibling still in
    /// flight, and delivering past it would skip that sibling permanently. The window trades bounded
    /// staleness for completeness, and holds provided no append stays uncommitted longer than it. Ignored
    /// on an exact-catch-up store, which needs no window. Must be non-negative.
    /// </summary>
    public TimeSpan SafetyWindow { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Clock used for the poll delay; defaults to the system clock. Injected for testing.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}
