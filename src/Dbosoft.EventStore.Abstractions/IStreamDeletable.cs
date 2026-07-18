namespace Dbosoft.EventStore;

/// <summary>
/// A store whose streams can be deleted, softly or hard. Optional — a provider supports deletion iff it
/// implements this interface. Emits a <see cref="SystemEventTypes.StreamDeleted"/> marker so read models
/// learn to forget.
/// </summary>
[PublicAPI]
public interface IStreamDeletable
{
    /// <summary>Deletes a whole stream, softly or hard (see <see cref="DeleteMode"/>).</summary>
    Task DeleteStreamAsync(string streamId, DeleteMode mode, CancellationToken ct = default);
}
