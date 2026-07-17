namespace Dbosoft.EventStore;

/// <summary>How a stream is deleted.</summary>
[PublicAPI]
public enum DeleteMode
{
    /// <summary>
    /// Tombstone the stream: its events are removed but the id may be reused by a later append.
    /// </summary>
    Soft = 0,

    /// <summary>
    /// Remove the stream and burn its id permanently; a later append to the same id is rejected.
    /// </summary>
    Hard = 1,
}
