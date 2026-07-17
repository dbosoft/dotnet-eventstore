namespace Dbosoft.EventStore;

/// <summary>
/// Durably remembers how far a named consumer has read the global sequence. A read-model rebuilder
/// or a bus relay stores its <see cref="GlobalPosition"/> here and resumes from it — the store never
/// tracks consumer progress itself, since delivery is not its job.
/// </summary>
[PublicAPI]
public interface ICheckpointStore
{
    /// <summary>Returns the saved position for a checkpoint name, or <c>null</c> if none is saved.</summary>
    Task<GlobalPosition?> GetAsync(string name, CancellationToken ct = default);

    /// <summary>Saves the position for a checkpoint name.</summary>
    Task SetAsync(string name, GlobalPosition position, CancellationToken ct = default);
}
