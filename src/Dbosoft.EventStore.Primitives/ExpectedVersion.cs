namespace Dbosoft.EventStore;

/// <summary>
/// Sentinel values for the <c>expectedVersion</c> argument of an append. A non-negative value
/// means the stream must currently be at exactly that version; the constants below relax or
/// strengthen that check.
/// </summary>
[PublicAPI]
public static class ExpectedVersion
{
    /// <summary>Append regardless of the stream's current version.</summary>
    public const long Any = -2;

    /// <summary>The stream must not exist yet.</summary>
    public const long NoStream = -1;

    /// <summary>The stream must already exist, at any version.</summary>
    public const long StreamExists = -4;

    /// <summary>The version of a stream that has no events yet.</summary>
    public const long EmptyStream = 0;
}
