using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace Dbosoft.EventStore.Rebus;

/// <summary>
/// The event-store identity the relay attaches to a Rebus message (see <see cref="EventStoreHeaders"/>).
/// A handler reads it from the incoming message headers to sequence and de-duplicate, since Rebus does
/// not preserve order and may redeliver.
/// </summary>
/// <param name="StreamId">The stream the event belongs to.</param>
/// <param name="StreamVersion">The event's 1-based version within its stream.</param>
/// <param name="Position">The event's position in the store's global ordering.</param>
/// <param name="EventId">The stable event id.</param>
/// <param name="EventType">The event's logical type name.</param>
[PublicAPI]
public sealed record EventStoreMetadata(
    string StreamId,
    long StreamVersion,
    GlobalPosition Position,
    Guid EventId,
    string EventType)
{
    /// <summary>
    /// Reads the metadata from a Rebus message's headers (e.g. <c>MessageContext.Current.Headers</c>).
    /// Returns <c>false</c> if the message was not stamped by the relay (the stream-id header is absent).
    /// </summary>
    public static bool TryRead(IReadOnlyDictionary<string, string> headers, [NotNullWhen(true)] out EventStoreMetadata? metadata)
    {
        ArgumentNullException.ThrowIfNull(headers);
        metadata = null;
        if (!headers.TryGetValue(EventStoreHeaders.StreamId, out var streamId))
            return false;

        var version = headers.TryGetValue(EventStoreHeaders.StreamVersion, out var v)
            ? long.Parse(v, CultureInfo.InvariantCulture)
            : 0;
        var position = new GlobalPosition(headers.GetValueOrDefault(EventStoreHeaders.GlobalPosition, ""));
        var eventId = headers.TryGetValue(EventStoreHeaders.EventId, out var id) ? Guid.ParseExact(id, "N") : Guid.Empty;
        var eventType = headers.GetValueOrDefault(EventStoreHeaders.EventType, "");

        metadata = new EventStoreMetadata(streamId, version, position, eventId, eventType);
        return true;
    }
}
