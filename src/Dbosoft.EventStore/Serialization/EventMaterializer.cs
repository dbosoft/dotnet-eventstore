using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Dbosoft.EventStore.Serialization;

/// <summary>Default <see cref="IEventMaterializer"/> over a domain's type registry, serializer and upcasters.</summary>
[PublicAPI]
public sealed class EventMaterializer : IEventMaterializer
{
    // System markers are written by the store itself in a fixed JSON format, so they are read the same
    // way regardless of the domain's serializer.
    private static readonly Dictionary<string, Type> SystemTypes = new(StringComparer.Ordinal)
    {
        [SystemEventTypes.StreamTruncated] = typeof(StreamTruncatedMarker),
        [SystemEventTypes.StreamDeleted] = typeof(StreamDeletedMarker),
        [SystemEventTypes.EventRedacted] = typeof(EventRedactedMarker),
    };

    private static readonly JsonSerializerOptions SystemJson = new(JsonSerializerDefaults.Web);

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
    public object Materialize(StoredEvent stored)
    {
        if (SystemTypes.TryGetValue(stored.EventType, out var systemType))
            return DeserializeSystem(stored, systemType);

        return _upcasters.Upcast(_serializer.Deserialize(_registry.Resolve(stored.EventType), stored.Data));
    }

    /// <inheritdoc />
    public bool TryMaterialize(StoredEvent stored, [MaybeNullWhen(false)] out object @event)
    {
        if (SystemTypes.TryGetValue(stored.EventType, out var systemType))
        {
            @event = DeserializeSystem(stored, systemType);
            return true;
        }

        if (!_registry.TryResolve(stored.EventType, out var type) || type is null)
        {
            @event = null;
            return false;
        }

        @event = _upcasters.Upcast(_serializer.Deserialize(type, stored.Data));
        return true;
    }

    private static object DeserializeSystem(StoredEvent stored, Type systemType) =>
        JsonSerializer.Deserialize(stored.Data.Span, systemType, SystemJson)
        ?? throw new InvalidOperationException($"System marker '{stored.EventType}' deserialized to null.");

    /// <inheritdoc />
    public EventData Dematerialize(object @event, Guid? eventId = null, ReadOnlyMemory<byte>? metadata = null) =>
        new(
            eventId ?? Guid.NewGuid(),
            _registry.GetEventType(@event.GetType()),
            _serializer.Serialize(@event),
            metadata,
            _serializer.ContentType);
}
