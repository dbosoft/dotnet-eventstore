using Rebus.Bus;

namespace Dbosoft.EventStore.Rebus;

/// <summary>
/// An <see cref="IChangeNotifier"/> that bridges the change signal across processes over Rebus, so a
/// subscription in one process wakes on a write that happened in another — without turning the store into a
/// bus. On a write it publishes a best-effort <see cref="EventStoreChanged"/> ping and releases any waiter in
/// the same process; an incoming ping (delivered to <see cref="EventStoreChangedHandler"/>) releases local
/// waiters. It is only one of many possible bridges — Redis, a database <c>LISTEN/NOTIFY</c>, or anything
/// else works the same way — provided so a Rebus-based app need not rebuild it.
/// </summary>
[PublicAPI]
public sealed class RebusChangeNotifier(IBus bus) : IChangeNotifier
{
    // The local waiter registry. WaitForChangeAsync parks on it; both a local write and an incoming ping
    // release it. Reusing the in-process notifier keeps the parking/release logic in one place.
    private readonly InProcessChangeNotifier _local = new();

    /// <summary>
    /// Signals a write: delivers to any watcher in this process immediately (so a same-process reader need
    /// not wait for the round-trip), then publishes an <see cref="EventStoreChanged"/> ping for other
    /// processes. The publish is best-effort — a transport failure only means remote readers fall back to poll.
    /// </summary>
    public async ValueTask NotifyAsync(string domain, string streamId, CancellationToken ct = default)
    {
        // Local first, so a same-process reader wakes even when the transport is down.
        await _local.NotifyAsync(domain, streamId, ct).ConfigureAwait(false);
        try
        {
            await bus.Publish(new EventStoreChanged(domain, streamId)).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Best-effort: a failed publish only means cross-process readers fall back to their poll tick.
            // Swallowing keeps a transport blip from surfacing as an unobserved fault on the write path.
        }
    }

    /// <inheritdoc />
    public IChangeWatcher Watch(string domain) => _local.Watch(domain);

    // Delivers a ping that arrived from another process to local watchers — WITHOUT re-publishing, which
    // would loop the notification back onto the bus. Called by EventStoreChangedHandler.
    internal ValueTask ReleaseLocalAsync(string domain, string streamId) => _local.NotifyAsync(domain, streamId);
}
