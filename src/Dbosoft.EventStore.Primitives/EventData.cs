namespace Dbosoft.EventStore;

/// <summary>
/// One event to append. The store treats <see cref="Data"/> and <see cref="Metadata"/> as opaque
/// bytes; interpreting them is the serialization layer's job.
/// </summary>
/// <param name="EventId">
/// A stable identity for this event, used for idempotent appends and de-duplication on catch-up.
/// Reusing an id that already exists in the target stream is a no-op rather than a duplicate.
/// </param>
/// <param name="EventType">The logical type name a serializer maps to a CLR type.</param>
/// <param name="Data">The serialized event payload.</param>
/// <param name="Metadata">
/// Optional serialized metadata — the home for searchable attributes: correlation/causation ids,
/// tenant, and categories that time-and-attribute retention filters on. Structured (e.g. JSON) so a
/// provider can query it; there is deliberately no separate typed attributes field.
/// </param>
/// <param name="ContentType">The media type of <see cref="Data"/> and <see cref="Metadata"/>.</param>
[PublicAPI]
public sealed record EventData(
    Guid EventId,
    string EventType,
    ReadOnlyMemory<byte> Data,
    ReadOnlyMemory<byte>? Metadata = null,
    string ContentType = "application/json");
