namespace Dbosoft.EventStore;

/// <summary>The current state of a stream, independent of its events.</summary>
/// <param name="StreamId">The stream's identifier.</param>
/// <param name="Version">The version of the most recent event, or <see cref="ExpectedVersion.EmptyStream"/>.</param>
/// <param name="TruncatedBefore">
/// The lowest version still retained. Events below it have been truncated and can no longer be read;
/// <c>1</c> means nothing has been truncated.
/// </param>
/// <param name="IsDeleted">Whether the stream has been (soft-)deleted.</param>
/// <param name="Metadata">Provider-defined stream metadata, or <c>null</c> when unset or unsupported.</param>
[PublicAPI]
public sealed record StreamInfo(
    string StreamId,
    long Version,
    long TruncatedBefore,
    bool IsDeleted,
    string? Metadata);
