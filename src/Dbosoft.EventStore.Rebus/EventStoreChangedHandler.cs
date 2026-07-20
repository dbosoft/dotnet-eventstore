using Rebus.Handlers;

namespace Dbosoft.EventStore.Rebus;

/// <summary>
/// Receives an <see cref="EventStoreChanged"/> ping from another process and releases this process's local
/// waiters, so an out-of-process subscription wakes on the remote write. <c>UseRebusNotifications</c>
/// registers it; a reader process must also subscribe to <see cref="EventStoreChanged"/> (see
/// <c>SubscribeToEventStoreChangesAsync</c>) for the ping to be delivered.
/// </summary>
[PublicAPI]
public sealed class EventStoreChangedHandler(RebusChangeNotifier notifier) : IHandleMessages<EventStoreChanged>
{
    /// <inheritdoc />
    public Task Handle(EventStoreChanged message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return notifier.ReleaseLocalAsync(message.Domain, message.StreamId).AsTask();
    }
}
