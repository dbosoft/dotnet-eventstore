namespace Dbosoft.EventStore.Serialization;

/// <summary>Turns event and snapshot objects into bytes and back, independent of the store.</summary>
[PublicAPI]
public interface IEventSerializer
{
    /// <summary>The media type this serializer produces, e.g. <c>application/json</c>.</summary>
    string ContentType { get; }

    /// <summary>Serializes an object to bytes.</summary>
    ReadOnlyMemory<byte> Serialize(object value);

    /// <summary>Deserializes bytes to the given target type.</summary>
    object Deserialize(Type type, ReadOnlyMemory<byte> data);
}
