namespace Dbosoft.EventStore.Subscriptions;

/// <summary>Tunes an <see cref="EventStoreSubscription"/>.</summary>
[PublicAPI]
public sealed record SubscriptionOptions
{
    /// <summary>Where to begin when no checkpoint has been saved yet. Defaults to the start of the log.</summary>
    public GlobalPosition StartFrom { get; init; } = GlobalPosition.Start;

    /// <summary>
    /// How many recently delivered <see cref="StoredEvent.EventId"/>s to remember for de-duplication on a
    /// store without <see cref="StoreCapabilities.ExactCatchup"/>. A re-read must not span more events than
    /// this or a duplicate could slip through; the default covers a generous overlap. Ignored on an
    /// exact-catch-up store, which never re-surfaces an event.
    /// </summary>
    public int DeduplicationWindow { get; init; } = 4096;

    /// <summary>How long <see cref="EventStoreSubscription.RunAsync"/> waits after an empty pump before polling again.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(1);

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
