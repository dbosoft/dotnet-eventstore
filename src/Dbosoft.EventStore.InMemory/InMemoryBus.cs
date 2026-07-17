namespace Dbosoft.EventStore.InMemory;

/// <summary>
/// A stored event delivered onto the demo bus: the original <see cref="StoredEvent"/> for context
/// plus its materialized, typed payload (a domain event or a system marker).
/// </summary>
/// <param name="Source">The stored event, for stream id, version and position.</param>
/// <param name="Payload">The typed payload produced by the materializer.</param>
public sealed record BusMessage(StoredEvent Source, object Payload);

/// <summary>
/// A minimal in-process pub/sub bus — the reference for the store↔bus seam, not a real bus. We
/// presuppose a real bus (Rebus); this exists only to prove the relay/inbound bridge end to end,
/// as <see cref="InMemoryEventStore"/> is the reference provider.
/// </summary>
[PublicAPI]
public sealed class InMemoryBus
{
    private readonly List<Func<BusMessage, CancellationToken, Task>> _handlers = [];
    private readonly Lock _gate = new();

    /// <summary>Subscribes a handler; dispose the result to unsubscribe.</summary>
    public IDisposable Subscribe(Func<BusMessage, CancellationToken, Task> handler)
    {
        lock (_gate)
            _handlers.Add(handler);
        return new Subscription(this, handler);
    }

    /// <summary>Delivers a message to every current subscriber, in registration order.</summary>
    public async Task PublishAsync(BusMessage message, CancellationToken ct = default)
    {
        Func<BusMessage, CancellationToken, Task>[] handlers;
        lock (_gate)
            handlers = [.. _handlers];

        foreach (var handler in handlers)
            await handler(message, ct).ConfigureAwait(false);
    }

    private void Unsubscribe(Func<BusMessage, CancellationToken, Task> handler)
    {
        lock (_gate)
            _handlers.Remove(handler);
    }

    private sealed class Subscription(InMemoryBus bus, Func<BusMessage, CancellationToken, Task> handler) : IDisposable
    {
        public void Dispose() => bus.Unsubscribe(handler);
    }
}
