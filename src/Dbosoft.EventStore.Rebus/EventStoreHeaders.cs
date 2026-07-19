namespace Dbosoft.EventStore.Rebus;

/// <summary>
/// The Rebus message-header keys the relay stamps on every forwarded event. They carry the ordering
/// identity Rebus itself does not preserve — <b>Rebus delivers messages out of order and at-least-once</b>,
/// so a consumer must use these, not arrival order, to sequence and de-duplicate. See
/// <see cref="EventStoreMetadata"/> for reading them and <see cref="StreamOrderingTracker"/> for acting on them.
/// </summary>
[PublicAPI]
public static class EventStoreHeaders
{
    /// <summary>The stream the event belongs to.</summary>
    public const string StreamId = "dbosoft-es-stream-id";

    /// <summary>The event's 1-based version within its stream — the per-stream order key.</summary>
    public const string StreamVersion = "dbosoft-es-stream-version";

    /// <summary>The event's opaque position in the store's global ordering.</summary>
    public const string GlobalPosition = "dbosoft-es-position";

    /// <summary>The stable event id, for de-duplicating redeliveries.</summary>
    public const string EventId = "dbosoft-es-event-id";

    /// <summary>The event's logical type name.</summary>
    public const string EventType = "dbosoft-es-event-type";
}
