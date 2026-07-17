using System.Diagnostics.CodeAnalysis;

namespace Dbosoft.EventStore.Serialization;

/// <summary>
/// Maps a domain's events between the store's byte form and typed CLR objects, both directions:
/// a stored event to its current-version instance (resolve type, deserialize, upcast), and a typed
/// event to an <see cref="EventData"/> for appending. Resolved from DI keyed by domain name, it is
/// the primitive a read model or a message-bus bridge uses — the store never delivers; it hands out
/// this so consumers can turn its feed into typed events.
/// </summary>
[PublicAPI]
public interface IEventMaterializer
{
    /// <summary>
    /// Turns a stored event into its current-version typed instance (deserialize then upcast).
    /// </summary>
    /// <exception cref="InvalidOperationException">The event type is not registered in the domain.</exception>
    object Materialize(StoredEvent stored);

    /// <summary>
    /// Like <see cref="Materialize"/> but returns <c>false</c> for an event type the domain does not
    /// know, instead of throwing — for consumers that filter to a subset of a domain's events.
    /// </summary>
    bool TryMaterialize(StoredEvent stored, [MaybeNullWhen(false)] out object @event);

    /// <summary>Turns a typed event into an <see cref="EventData"/> ready to append.</summary>
    EventData Dematerialize(object @event, Guid? eventId = null, ReadOnlyMemory<byte>? metadata = null);
}
