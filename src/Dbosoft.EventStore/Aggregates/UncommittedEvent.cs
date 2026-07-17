namespace Dbosoft.EventStore.Aggregates;

/// <summary>
/// An event raised by an aggregate but not yet persisted, paired with the stable identity assigned
/// when it was raised. The id survives save retries, which is what makes the append idempotent.
/// </summary>
/// <param name="EventId">The stable identity assigned at raise time.</param>
/// <param name="Event">The event instance.</param>
[PublicAPI]
public sealed record UncommittedEvent(Guid EventId, object Event);
