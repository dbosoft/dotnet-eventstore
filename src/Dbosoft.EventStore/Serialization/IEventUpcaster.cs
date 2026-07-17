namespace Dbosoft.EventStore.Serialization;

/// <summary>
/// Transforms a deserialized event from an older shape to a newer one on read, so aggregates only
/// ever see the current version. Upcasters are applied in a chain until none matches (see
/// <see cref="UpcasterChain"/>).
/// </summary>
[PublicAPI]
public interface IEventUpcaster
{
    /// <summary>Whether this upcaster applies to the given event instance.</summary>
    bool CanUpcast(object @event);

    /// <summary>Returns the upgraded event. Only called when <see cref="CanUpcast"/> is <c>true</c>.</summary>
    object Upcast(object @event);
}
