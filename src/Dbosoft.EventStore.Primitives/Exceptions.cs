namespace Dbosoft.EventStore;

/// <summary>Base type for all event store failures.</summary>
[PublicAPI]
public abstract class EventStoreException : Exception
{
    /// <summary>Initializes the base exception with a message and optional inner exception.</summary>
    protected EventStoreException(string message, Exception? innerException = null)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Thrown when an append operation's <c>expectedVersion</c> does not match the stream's actual version, i.e.
/// on an optimistic-concurrency conflict. <see cref="ActualVersion"/> lets a caller decide whether
/// to retry or reload.
/// </summary>
[PublicAPI]
public sealed class WrongExpectedVersionException : EventStoreException
{
    /// <summary>Creates the exception describing the version mismatch.</summary>
    public WrongExpectedVersionException(string streamId, long expectedVersion, long actualVersion)
        : base($"Append to stream '{streamId}' expected version {expectedVersion} but the stream is at {actualVersion}.")
    {
        StreamId = streamId;
        ExpectedVersion = expectedVersion;
        ActualVersion = actualVersion;
    }

    /// <summary>The stream the append operation targeted.</summary>
    public string StreamId { get; }

    /// <summary>The version the caller expected.</summary>
    public long ExpectedVersion { get; }

    /// <summary>The version the stream was actually at.</summary>
    public long ActualVersion { get; }
}

/// <summary>Thrown when a stream is required to exist but does not.</summary>
[PublicAPI]
public sealed class StreamNotFoundException : EventStoreException
{
    /// <summary>Creates the exception for the given stream.</summary>
    public StreamNotFoundException(string streamId)
        : base($"Stream '{streamId}' was not found.")
    {
        StreamId = streamId;
    }

    /// <summary>The stream that was not found.</summary>
    public string StreamId { get; }
}

/// <summary>
/// Thrown when a stream has been truncated below the point from which it could be folded and no
/// usable snapshot covers the gap, so the aggregate cannot be reconstructed. Rebuilding from a
/// partial suffix would produce silently wrong state, so the load fails instead.
/// </summary>
[PublicAPI]
public sealed class SnapshotRequiredException : EventStoreException
{
    /// <summary>Creates the exception for a stream truncated before the given version.</summary>
    public SnapshotRequiredException(string streamId, long truncatedBefore)
        : base($"Stream '{streamId}' is truncated before version {truncatedBefore} and no usable snapshot "
               + $"at or after version {truncatedBefore - 1} exists; it cannot be reconstructed.")
    {
        StreamId = streamId;
        TruncatedBefore = truncatedBefore;
    }

    /// <summary>The stream that cannot be reconstructed.</summary>
    public string StreamId { get; }

    /// <summary>The truncation boundary: events below this version are gone.</summary>
    public long TruncatedBefore { get; }
}

/// <summary>Thrown when a stream has been deleted and can no longer be read or appended to.</summary>
[PublicAPI]
public sealed class StreamDeletedException : EventStoreException
{
    /// <summary>Creates the exception for the given stream.</summary>
    public StreamDeletedException(string streamId)
        : base($"Stream '{streamId}' has been deleted.")
    {
        StreamId = streamId;
    }

    /// <summary>The stream that was deleted.</summary>
    public string StreamId { get; }
}

/// <summary>
/// Thrown when a requested operation is not supported by the provider, or is refused for a
/// data-integrity reason — for example an optional interface the store does not implement, or a
/// truncation that would leave the stream impossible to reconstruct and was not forced.
/// </summary>
[PublicAPI]
public sealed class NotSupportedByProviderException : EventStoreException
{
    /// <summary>Creates the exception with an explanatory message.</summary>
    public NotSupportedByProviderException(string message)
        : base(message)
    {
    }
}
