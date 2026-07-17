using Microsoft.Extensions.Logging;

namespace Dbosoft.EventStore.Sample.BankAccount;

/// <summary>
/// A trivial read-side projection: it logs each event as the $all subscription delivers it. A real
/// projection would fold these into a query model.
/// </summary>
internal sealed class ConsoleProjection(ILogger<ConsoleProjection> logger) : IEventHandler
{
    public Task HandleAsync(StoredEvent @event, CancellationToken ct = default)
    {
        logger.LogInformation(
            "projection: [{Position}] {StreamId} v{Version} {EventType}",
            @event.Position, @event.StreamId, @event.StreamVersion, @event.EventType);
        return Task.CompletedTask;
    }
}
