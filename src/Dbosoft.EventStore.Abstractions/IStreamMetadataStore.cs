namespace Dbosoft.EventStore;

/// <summary>
/// A store whose streams carry readable and writable metadata — a retention policy and custom
/// key/values (see <see cref="StreamMetadata"/>). Optional — a provider supports it iff it implements
/// this interface. Setting a retention policy takes effect on the next scavenge that has retention
/// enabled (see <c>IScavengeable</c>); the metadata itself is stored immediately.
/// </summary>
/// <remarks>
/// Metadata is stored on the stream head. Setting it before the stream has any events materializes the
/// head, so <c>GetStreamInfoAsync</c> then returns a non-null <see cref="StreamInfo"/> at version 0 for a
/// stream that has no events yet — callers must not treat "stream info is non-null" as "the stream has
/// events". On a key/value provider metadata is stored inline on the head entity, so a very large custom
/// dictionary can exceed the backend's per-entity limit; keep custom metadata small.
/// </remarks>
[PublicAPI]
public interface IStreamMetadataStore
{
    /// <summary>Returns a stream's metadata, or <c>null</c> if none has been set (or the stream is deleted).</summary>
    Task<StreamMetadata?> GetStreamMetadataAsync(string streamId, CancellationToken ct = default);

    /// <summary>Sets (replacing) a stream's metadata. Metadata may be set before the stream has any events.</summary>
    Task SetStreamMetadataAsync(string streamId, StreamMetadata metadata, CancellationToken ct = default);
}
