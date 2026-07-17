using System.Collections.Concurrent;

namespace Dbosoft.EventStore.Serialization;

/// <summary>Default in-memory <see cref="IEventTypeRegistry"/>, safe for concurrent reads.</summary>
[PublicAPI]
public sealed class EventTypeRegistry : IEventTypeRegistry
{
    private readonly ConcurrentDictionary<Type, string> _byType = new();
    private readonly ConcurrentDictionary<string, Type> _byName = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public void Register(Type clrType, string eventType)
    {
        ArgumentNullException.ThrowIfNull(clrType);
        ArgumentException.ThrowIfNullOrEmpty(eventType);

        if (_byName.TryGetValue(eventType, out var existing) && existing != clrType)
            throw new InvalidOperationException(
                $"Event type name '{eventType}' is already registered to '{existing}'.");

        _byType[clrType] = eventType;
        _byName[eventType] = clrType;
    }

    /// <inheritdoc />
    public string GetEventType(Type clrType) =>
        _byType.TryGetValue(clrType, out var name)
            ? name
            : throw new InvalidOperationException($"CLR type '{clrType}' is not registered as an event type.");

    /// <inheritdoc />
    public Type Resolve(string eventType) =>
        _byName.TryGetValue(eventType, out var type)
            ? type
            : throw new InvalidOperationException($"Event type name '{eventType}' is not registered.");

    /// <inheritdoc />
    public bool TryResolve(string eventType, out Type? clrType)
    {
        var found = _byName.TryGetValue(eventType, out var type);
        clrType = type;
        return found;
    }
}
