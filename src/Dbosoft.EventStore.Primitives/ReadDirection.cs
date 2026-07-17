namespace Dbosoft.EventStore;

/// <summary>The direction in which a stream or the global sequence is read.</summary>
[PublicAPI]
public enum ReadDirection
{
    /// <summary>From lower versions/positions to higher ones.</summary>
    Forwards = 0,

    /// <summary>From higher versions/positions to lower ones.</summary>
    Backwards = 1,
}
