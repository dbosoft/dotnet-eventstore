namespace Dbosoft.EventStore;

/// <summary>
/// Handles events delivered by a subscription. Delivery is in-order per the provider's guarantee;
/// without <see cref="StoreCapabilities.ExactSubscriptions"/> a handler must tolerate redelivery and
/// reordering within the store's safety window, so keep handling idempotent.
/// </summary>
[PublicAPI]
public interface IEventHandler
{
    /// <summary>Processes one event. The subscription advances its checkpoint once this completes.</summary>
    Task HandleAsync(StoredEvent @event, CancellationToken ct = default);
}

/// <summary>
/// Starts catch-up subscriptions over the global sequence. Optional; gated by
/// <see cref="StoreCapabilities.Subscriptions"/>.
/// </summary>
[PublicAPI]
public interface ISubscriptionSource
{
    /// <summary>
    /// Subscribes to all events after <paramref name="checkpoint"/>, first catching up historically
    /// and then following live writes. Dispose the returned handle to stop. The handler is invoked
    /// serially in global order.
    /// </summary>
    IAsyncDisposable SubscribeToAll(
        GlobalPosition checkpoint,
        IEventHandler handler,
        SubscriptionOptions? options = null);
}
