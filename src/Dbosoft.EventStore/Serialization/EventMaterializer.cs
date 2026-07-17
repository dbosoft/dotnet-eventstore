using System.Diagnostics.CodeAnalysis;

namespace Dbosoft.EventStore.Serialization;

/// <summary>Default <see cref="IEventMaterializer"/> over a domain's type registry, serializer and upcasters.</summary>
[PublicAPI]
public sealed class EventMaterializer : IEventMaterializer
{
    private readonly IEventTypeRegistry _registry;
    private readonly IEventSerializer _serializer;
    private readonly UpcasterChain _upcasters;

    /// <summary>Creates the materializer for a domain.</summary>
    public EventMaterializer(IEventTypeRegistry registry, IEventSerializer serializer, UpcasterChain upcasters)
    {
        _registry = registry;
        _serializer = serializer;
        _upcasters = upcasters;
    }

    /// <inheritdoc />
    public object Materialize(StoredEvent stored) =>
        _upcasters.Upcast(_serializer.Deserialize(_registry.Resolve(stored.EventType), stored.Data));

    /// <inheritdoc />
    public bool TryMaterialize(StoredEvent stored, [MaybeNullWhen(false)] out object @event)
    {
        if (!_registry.TryResolve(stored.EventType, out var type) || type is null)
        {
            @event = null;
            return false;
        }

        @event = _upcasters.Upcast(_serializer.Deserialize(type, stored.Data));
        return true;
    }

    /// <inheritdoc />
    public EventData Dematerialize(object @event, Guid? eventId = null, ReadOnlyMemory<byte>? metadata = null) =>
        new(
            eventId ?? Guid.NewGuid(),
            _registry.GetEventType(@event.GetType()),
            _serializer.Serialize(@event),
            metadata,
            _serializer.ContentType);
}
