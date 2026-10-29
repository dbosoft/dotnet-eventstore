// EF Core materializes and updates these rows (Position is assigned after insert, Version and flags
// change on truncate/delete/redact), so the properties must stay settable — init-only breaks the
// framework here.
// ReSharper disable PropertyCanBeMadeInitOnly.Global

namespace Dbosoft.EventStore.EfCore;

/// <summary>The head row of a stream: its current version and lifecycle flags. One per (domain, stream).</summary>
internal sealed class StreamRow
{
    public required string Domain { get; set; }
    public required string StreamId { get; set; }
    public long Version { get; set; }
    public long TruncatedBefore { get; set; } = 1;
    public bool IsDeleted { get; set; }
    public bool HardDeleted { get; set; }

    /// <summary>Serialized <see cref="StreamMetadata"/> (JSON), or <c>null</c> when none is set.</summary>
    public string? Metadata { get; set; }
}

/// <summary>
/// A stored event. <see cref="Position"/> is the database-assigned row id, taken when the row is inserted;
/// <see cref="CommitSequence"/> is its place in <c>$all</c>, taken just before its transaction commits.
/// </summary>
internal sealed class EventRow
{
    public long Position { get; set; }

    /// <summary>
    /// The event's global position, or <c>null</c> while its transaction has not been prepared for commit. A
    /// transaction can stay open long after its insert (an append enlisted in a caller's message), so the row id
    /// does not follow commit order; this sequence is taken under one counter lock held until commit, so it
    /// does, and a reader that has seen sequence n has seen every committed event before it.
    /// </summary>
    public long? CommitSequence { get; set; }
    public required string Domain { get; set; }
    public required string StreamId { get; set; }
    public long StreamVersion { get; set; }
    public Guid EventId { get; set; }
    public required string EventType { get; set; }
    public required byte[] Data { get; set; }
    public byte[]? Metadata { get; set; }
    public required string ContentType { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}

/// <summary>A snapshot row. Keyed by (domain, stream, version) so history is possible.</summary>
internal sealed class SnapshotRow
{
    public required string Domain { get; set; }
    public required string StreamId { get; set; }
    public long StreamVersion { get; set; }
    public required byte[] Payload { get; set; }
    public required string ContentType { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}

/// <summary>The counter <see cref="EventRow.CommitSequence"/> is taken from. One row.</summary>
internal sealed class SequenceRow
{
    public int Id { get; set; }

    /// <summary>The last sequence handed out.</summary>
    public long Last { get; set; }
}

/// <summary>A durable consumer checkpoint.</summary>
internal sealed class CheckpointRow
{
    public required string Name { get; set; }
    public required string Token { get; set; }
}
