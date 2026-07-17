namespace Dbosoft.EventStore.Aggregates;

/// <summary>
/// Implemented by aggregates that can summarize their state into a snapshot, so the repository can
/// rehydrate from the latest snapshot plus the events after it instead of the whole stream.
/// </summary>
public interface ISnapshotable
{
    /// <summary>The CLR type of the snapshot state, used to deserialize a stored snapshot.</summary>
    Type SnapshotStateType { get; }

    /// <summary>Produces the current state as a serializable snapshot object.</summary>
    object CreateSnapshotState();

    /// <summary>
    /// Restores state from a snapshot taken at <paramref name="version"/>. The repository then
    /// replays only the events after that version.
    /// </summary>
    void RestoreFromSnapshot(object state, long version);
}
