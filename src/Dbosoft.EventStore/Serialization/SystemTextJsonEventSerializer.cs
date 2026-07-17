using System.Text.Json;

namespace Dbosoft.EventStore.Serialization;

/// <summary>An <see cref="IEventSerializer"/> backed by <c>System.Text.Json</c>.</summary>
[PublicAPI]
public sealed class SystemTextJsonEventSerializer : IEventSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <summary>Creates the serializer, optionally with custom JSON options.</summary>
    public SystemTextJsonEventSerializer(JsonSerializerOptions? options = null) =>
        _options = options ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);

    /// <inheritdoc />
    public string ContentType => "application/json";

    /// <inheritdoc />
    public ReadOnlyMemory<byte> Serialize(object value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return JsonSerializer.SerializeToUtf8Bytes(value, value.GetType(), _options);
    }

    /// <inheritdoc />
    public object Deserialize(Type type, ReadOnlyMemory<byte> data) =>
        JsonSerializer.Deserialize(data.Span, type, _options)
        ?? throw new InvalidOperationException($"Deserializing to '{type}' yielded null.");
}
