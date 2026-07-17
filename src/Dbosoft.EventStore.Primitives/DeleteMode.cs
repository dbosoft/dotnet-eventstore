namespace Dbosoft.EventStore;

/// <summary>How a stream is deleted.</summary>
[PublicAPI]
public enum DeleteMode
{
    /// <summary>
    /// Tombstone the stream: its events are removed but the id stays usable — a later append recreates
    /// the stream and versioning resumes where it left off rather than resetting, so the recreation is
    /// an ordinary append (<see cref="ExpectedVersion.Any"/> or the current version), not a
    /// <see cref="ExpectedVersion.NoStream"/> one.
    /// </summary>
    Soft = 0,

    /// <summary>
    /// Remove the stream and burn its id permanently; a later append to the same id is rejected.
    /// </summary>
    Hard = 1
}
