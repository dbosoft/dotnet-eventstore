using System.Globalization;
using Azure.Data.Tables;

namespace Dbosoft.EventStore.AzureTables;

/// <summary>
/// Keying and (de)serialization within one domain's tables. A partition is one stream; a HEAD entity
/// (row key <see cref="HeadRowKey"/>) holds the stream's version and lifecycle flags, and each event
/// is a row keyed by its zero-padded version, so append (HEAD check-and-bump plus the event rows) is
/// one atomic entity-group transaction in that partition. Payload fields are held inline when small
/// and spilled to a blob when large (see <see cref="PayloadStore"/>), so no size ceiling applies.
/// </summary>
internal static class TableSchema
{
    public const string HeadRowKey = "HEAD";
    public const string SystemStream = SystemEventTypes.SystemStream;

    private const char EventRowPrefix = 'E';

    // --- keys ---

    public static string EventRowKey(long version) => EventRowPrefix + version.ToString("D19", CultureInfo.InvariantCulture);

    // The row keys of all event rows in a partition sort strictly below the HEAD row ('E' < 'H'), so a
    // range query for events never has to read (or skip) the head.
    public static string EventRowKeyLowerBound => EventRowPrefix + "0000000000000000000";
    public static string EventRowKeyUpperBound => EventRowPrefix + "9999999999999999999";

    // --- blob names for spilled payloads (one container per domain) ---

    // Event payload blobs are keyed by the event's own id, not its version: an id is unique and stable
    // across append retries, so two writers racing for the same version never collide on a blob name
    // (which would corrupt the winner's data), and a retried append reuses its blob rather than leaking.
    public static string DataBlobName(string streamId, Guid eventId) => $"e/{streamId}/{eventId:N}/d";
    public static string MetaBlobName(string streamId, Guid eventId) => $"e/{streamId}/{eventId:N}/m";
    public static string SnapshotBlobName(string streamId, long version) => $"s/{streamId}/{version}";

    // --- head entity ---

    public static TableEntity NewHead(string streamId) =>
        new(streamId, HeadRowKey)
        {
            ["Version"] = 0L,
            ["TruncatedBefore"] = 1L,
            ["IsDeleted"] = false,
            ["HardDeleted"] = false,
            // Marker watermarks. A destructive op and its $system marker are in different partitions, so
            // they cannot commit together; these record how far a marker has been confirmed-emitted, so a
            // retry after a lost marker re-emits it (markers apply idempotently, so at-least-once is safe).
            ["MarkedBefore"] = 1L,
            ["DeleteMarked"] = false
        };

    public static long HeadVersion(TableEntity head) => head.GetInt64("Version") ?? 0;

    public static long HeadTruncatedBefore(TableEntity head) => head.GetInt64("TruncatedBefore") ?? 1;

    public static bool HeadIsDeleted(TableEntity head) => head.GetBoolean("IsDeleted") ?? false;

    public static bool HeadHardDeleted(TableEntity head) => head.GetBoolean("HardDeleted") ?? false;

    public static long HeadMarkedBefore(TableEntity head) => head.GetInt64("MarkedBefore") ?? 1;

    public static bool HeadDeleteMarked(TableEntity head) => head.GetBoolean("DeleteMarked") ?? false;

    // --- event entity ---

    public static TableEntity EventEntity(
        string streamId,
        long version,
        GlobalPosition position,
        EventData evt,
        DateTimeOffset createdAt,
        PayloadSlot data,
        PayloadSlot metadata)
    {
        var entity = new TableEntity(streamId, EventRowKey(version))
        {
            ["StreamId"] = streamId,
            ["StreamVersion"] = version,
            ["EventId"] = evt.EventId.ToString("N"),
            ["EventType"] = evt.EventType,
            ["ContentType"] = evt.ContentType,
            ["CreatedAt"] = createdAt,
            ["Position"] = position.Token
        };
        WriteSlot(entity, "Data", data);
        WriteSlot(entity, "Meta", metadata);
        return entity;
    }

    public static StoredEvent ToStored(TableEntity entity, ReadOnlyMemory<byte> data, ReadOnlyMemory<byte>? metadata) =>
        new(
            Required(entity, "StreamId"),
            entity.GetInt64("StreamVersion") ?? 0,
            new GlobalPosition(Required(entity, "Position")),
            EventId(entity),
            Required(entity, "EventType"),
            data,
            metadata,
            Required(entity, "ContentType"),
            entity.GetDateTimeOffset("CreatedAt") ?? default);

    public static Guid EventId(TableEntity entity) => Guid.ParseExact(Required(entity, "EventId"), "N");

    public static long Version(TableEntity entity) => entity.GetInt64("StreamVersion") ?? 0;

    public static GlobalPosition Position(TableEntity entity) => new(Required(entity, "Position"));

    public static bool IsHead(TableEntity entity) => entity.RowKey == HeadRowKey;

    // --- per-stream descending index (reverse-version row key, so a range scan reads newest-first) ---

    public static string IndexRowKey(long version) =>
        (long.MaxValue - version).ToString("D19", CultureInfo.InvariantCulture);

    public static TableEntity IndexEntity(string streamId, long version) =>
        new(streamId, IndexRowKey(version)) { ["Version"] = version };

    public static long IndexedVersion(TableEntity entity) => entity.GetInt64("Version") ?? 0;

    // --- $all index: partitioned by a coarse time bucket of the position, row-keyed by the position,
    // so a catch-up read from a position is a bounded partition-range scan instead of a full table scan ---

    public static string AllIndexBucket(GlobalPosition position)
    {
        if (position == GlobalPosition.End)
            return "9999999999";
        var tick = string.IsNullOrEmpty(position.Token) ? 0L : long.Parse(position.Token, CultureInfo.InvariantCulture);
        return (tick / TimeSpan.TicksPerHour).ToString("D10", CultureInfo.InvariantCulture);
    }

    public static TableEntity AllIndexEntity(GlobalPosition position, string streamId, long version) =>
        new(AllIndexBucket(position), position.Token) { ["StreamId"] = streamId, ["Version"] = version };

    public static (string StreamId, long Version) AllIndexTarget(TableEntity entity) =>
        (Required(entity, "StreamId"), entity.GetInt64("Version") ?? 0);

    // --- snapshot entity (reverse-version row key, so the newest snapshot sorts first) ---

    public static string SnapshotRowKey(long version) =>
        (long.MaxValue - version).ToString("D19", CultureInfo.InvariantCulture);

    public static TableEntity SnapshotEntity(
        string streamId,
        long version,
        PayloadSlot payload,
        string contentType,
        DateTimeOffset createdAt)
    {
        var entity = new TableEntity(streamId, SnapshotRowKey(version))
        {
            ["StreamVersion"] = version,
            ["ContentType"] = contentType,
            ["CreatedAt"] = createdAt
        };
        WriteSlot(entity, "Payload", payload);
        return entity;
    }

    public static StoredSnapshot ToSnapshot(TableEntity entity, ReadOnlyMemory<byte> payload) =>
        new(
            entity.PartitionKey,
            entity.GetInt64("StreamVersion") ?? 0,
            payload,
            Required(entity, "ContentType"),
            entity.GetDateTimeOffset("CreatedAt") ?? default);

    public static long SnapshotVersion(TableEntity entity) => entity.GetInt64("StreamVersion") ?? 0;

    // --- payload slots (inline vs. blob reference) ---

    public static void WriteSlot(TableEntity entity, string prefix, PayloadSlot slot)
    {
        entity.Remove(prefix);
        entity.Remove(prefix + "Blob");
        entity[prefix + "Mode"] = (int)slot.Mode;
        switch (slot.Mode)
        {
            case PayloadMode.Inline:
                entity[prefix] = slot.Inline.ToArray();
                break;
            case PayloadMode.Blob:
                entity[prefix + "Blob"] = slot.BlobName;
                break;
        }
    }

    public static PayloadSlot ReadSlot(TableEntity entity, string prefix) =>
        (PayloadMode)(entity.GetInt32(prefix + "Mode") ?? (int)PayloadMode.Null) switch
        {
            PayloadMode.Inline => PayloadSlot.OfInline(entity.GetBinary(prefix) ?? []),
            PayloadMode.Blob => PayloadSlot.OfBlob(Required(entity, prefix + "Blob")),
            _ => PayloadSlot.None
        };

    private static string Required(TableEntity entity, string name) =>
        entity.GetString(name)
        ?? throw new InvalidOperationException($"Entity {entity.PartitionKey}/{entity.RowKey} is missing '{name}'.");
}
