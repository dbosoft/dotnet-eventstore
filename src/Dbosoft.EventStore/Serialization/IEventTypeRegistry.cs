namespace Dbosoft.EventStore.Serialization;

/// <summary>
/// Maps between CLR event types and the stable logical type names written to the store. The logical
/// name decouples the persisted stream from type renames and refactorings.
/// </summary>
[PublicAPI]
public interface IEventTypeRegistry
{
    /// <summary>Registers a CLR type under a logical name (defaulting to the type's name).</summary>
    void Register(Type clrType, string eventType);

    /// <summary>Returns the logical name for a CLR type.</summary>
    /// <exception cref="InvalidOperationException">The type is not registered.</exception>
    string GetEventType(Type clrType);

    /// <summary>Resolves a logical name to its CLR type.</summary>
    /// <exception cref="InvalidOperationException">The name is not registered.</exception>
    Type Resolve(string eventType);

    /// <summary>Resolves a logical name to its CLR type, returning <c>false</c> if unknown.</summary>
    bool TryResolve(string eventType, out Type? clrType);
}
