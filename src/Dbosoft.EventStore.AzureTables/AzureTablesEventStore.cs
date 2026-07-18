using System.Runtime.CompilerServices;
using System.Text.Json;
using Azure;
using Azure.Data.Tables;

namespace Dbosoft.EventStore.AzureTables;

/// <summary>
/// The Azure Table Storage event store for one domain — backed by that domain's own tables and blob
/// container. Each stream is a partition whose HEAD entity carries the version, so an append is one
/// atomic entity-group transaction (check-and-bump HEAD plus the event rows). Large event payloads
/// spill to blobs. It declares the same capabilities as the relational provider, but not
/// <see cref="StoreCapabilities.ExactCatchup"/>: <c>$all</c> is ordered by a per-process monotonic
/// position rather than a dense committed sequence, so a catch-up read is complete only up to
/// redelivery, and consumers de-duplicate by <see cref="StoredEvent.EventId"/>.
/// </summary>
internal sealed class AzureTablesEventStore(
    TableClient eventsTable,
    TableClient snapshots,
    PayloadStore payloads,
    TimeProvider timeProvider,
    bool inBandMarkers,
    TimeSpan tombstoneRetention)
    : IEventStore, IAllStreamReader, ITimeNavigable, IStreamMaintenance
{
    // A single entity-group transaction holds at most 100 entities; one is the HEAD, so at most 99
    // events per append.
    private const int MaxEventsPerAppend = 99;

    private readonly Lock _tickGate = new();
    private long _lastTick;

    public StoreCapabilities Capabilities =>
        StoreCapabilities.AllStreamReads
        | StoreCapabilities.TimeNavigation
        | StoreCapabilities.Truncation
        | StoreCapabilities.StreamDeletion
        | StoreCapabilities.SnapshotHistory
        | StoreCapabilities.Redaction;

    public async Task<AppendResult> AppendToStreamAsync(
        string streamId,
        long expectedVersion,
        IReadOnlyList<EventData> events,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(streamId);
        ArgumentNullException.ThrowIfNull(events);
        if (events.Count > MaxEventsPerAppend)
            throw new NotSupportedByProviderException(
                $"Azure Table Storage appends are limited to {MaxEventsPerAppend} events per batch (the "
                + $"entity-group transaction limit); the append to '{streamId}' had {events.Count}.");

        var batchIds = new HashSet<Guid>();
        foreach (var evt in events)
        {
            if (!batchIds.Add(evt.EventId))
                throw new InvalidOperationException(
                    $"The append to stream '{streamId}' contains EventId {evt.EventId} more than once.");
        }

        if (events.Count == 0)
        {
            var head = await TryGetHeadAsync(streamId, ct).ConfigureAwait(false);
            if (head is not null && TableSchema.HeadHardDeleted(head))
                throw new StreamDeletedException(streamId);
            var version = head is null ? 0 : TableSchema.HeadVersion(head);
            EnsureExpectedVersion(streamId, expectedVersion, version);
            return new AppendResult(version, null);
        }

        const int maxAttempts = 64;
        for (var attempt = 1; ; attempt++)
        {
            var head = await TryGetHeadAsync(streamId, ct).ConfigureAwait(false);
            if (head is not null && TableSchema.HeadHardDeleted(head))
                throw new StreamDeletedException(streamId);

            var existing = await FindByEventIdsAsync(streamId, batchIds, ct).ConfigureAwait(false);
            if (TryReplay(existing, events.Count, out var replay))
                return replay;
            if (existing.Count > 0)
                throw new InvalidOperationException(
                    $"The append to stream '{streamId}' partially overlaps events already stored; an append "
                    + "must be either entirely new or an exact replay of a prior batch.");

            var current = head is null ? 0 : TableSchema.HeadVersion(head);
            EnsureExpectedVersion(streamId, expectedVersion, current);

            var positions = AllocatePositions(events.Count);
            var createdAt = timeProvider.GetUtcNow();
            var actions = new List<TableTransactionAction>(events.Count + 1);
            var version = current;
            for (var i = 0; i < events.Count; i++)
            {
                version++;
                var data = await payloads.PutAsync(TableSchema.DataBlobName(streamId, version), events[i].Data, ct).ConfigureAwait(false);
                var metadata = await payloads.PutAsync(TableSchema.MetaBlobName(streamId, version), events[i].Metadata, ct).ConfigureAwait(false);
                var entity = TableSchema.EventEntity(streamId, version, positions[i], events[i], createdAt, data, metadata);
                actions.Add(new TableTransactionAction(TableTransactionActionType.Add, entity));
            }

            if (head is null)
            {
                var newHead = TableSchema.NewHead(streamId);
                newHead["Version"] = version;
                actions.Add(new TableTransactionAction(TableTransactionActionType.Add, newHead));
            }
            else
            {
                head["Version"] = version;
                head["IsDeleted"] = false;
                head["DeleteMarked"] = false; // recreation: a later delete must emit a fresh marker
                actions.Add(new TableTransactionAction(TableTransactionActionType.UpdateReplace, head, head.ETag));
            }

            try
            {
                await eventsTable.SubmitTransactionAsync(actions, ct).ConfigureAwait(false);
                return new AppendResult(version, positions[^1]);
            }
            catch (RequestFailedException)
            {
                // A concurrent writer took the version (HEAD etag / entity-exists conflict). Classify by
                // re-reading, mirroring the relational provider: same batch now present is an idempotent
                // replay, an advanced head is a version conflict (retried when the caller left the version
                // open), an unchanged head is a genuine failure surfaced unmasked.
                var stored = await FindByEventIdsAsync(streamId, batchIds, ct).ConfigureAwait(false);
                if (TryReplay(stored, events.Count, out var raced))
                    return raced;

                var actualHead = await TryGetHeadAsync(streamId, ct).ConfigureAwait(false);
                var actual = actualHead is null ? 0 : TableSchema.HeadVersion(actualHead);
                if (stored.Count == 0 && actual == current)
                    throw;

                if (IsVersionAgnostic(expectedVersion, actual) && attempt < maxAttempts)
                    continue;

                throw new WrongExpectedVersionException(streamId, expectedVersion, actual);
            }
        }
    }

    public async IAsyncEnumerable<StoredEvent> ReadStreamAsync(
        string streamId,
        ReadDirection direction = ReadDirection.Forwards,
        long fromVersion = 0,
        long maxCount = long.MaxValue,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var take = Clamp(maxCount);
        if (take == 0)
            yield break;

        if (direction == ReadDirection.Forwards)
        {
            var lower = TableSchema.EventRowKey(Math.Max(1, fromVersion));
            var filter = $"PartitionKey eq '{Escape(streamId)}' and RowKey ge '{lower}' "
                         + $"and RowKey le '{TableSchema.EventRowKeyUpperBound}'";
            var count = 0;
            await foreach (var entity in eventsTable.QueryAsync<TableEntity>(filter, cancellationToken: ct).ConfigureAwait(false))
            {
                if (await TryMaterializeAsync(entity, ct).ConfigureAwait(false) is not { } stored)
                    continue;
                yield return stored;
                if (++count >= take)
                    yield break;
            }

            yield break;
        }

        var upper = fromVersion <= 0 ? TableSchema.EventRowKeyUpperBound : TableSchema.EventRowKey(fromVersion);
        var backward = $"PartitionKey eq '{Escape(streamId)}' and RowKey ge '{TableSchema.EventRowKeyLowerBound}' "
                       + $"and RowKey le '{upper}'";
        var page = new List<TableEntity>();
        await foreach (var entity in eventsTable.QueryAsync<TableEntity>(backward, cancellationToken: ct).ConfigureAwait(false))
            page.Add(entity);

        page.Reverse();
        foreach (var entity in page.Take(take))
            if (await TryMaterializeAsync(entity, ct).ConfigureAwait(false) is { } stored)
                yield return stored;
    }

    public async Task<StreamInfo?> GetStreamInfoAsync(string streamId, CancellationToken ct = default)
    {
        var head = await TryGetHeadAsync(streamId, ct).ConfigureAwait(false);
        return head is null || TableSchema.HeadHardDeleted(head)
            ? null
            : new StreamInfo(
                streamId,
                TableSchema.HeadVersion(head),
                TableSchema.HeadTruncatedBefore(head),
                TableSchema.HeadIsDeleted(head),
                Metadata: null);
    }

    public async IAsyncEnumerable<StoredEvent> ReadAllAsync(
        GlobalPosition from,
        ReadDirection direction = ReadDirection.Forwards,
        long maxCount = long.MaxValue,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var take = Clamp(maxCount);
        if (take == 0)
            yield break;

        var forwards = direction == ReadDirection.Forwards;
        var selected = new List<TableEntity>();
        await foreach (var entity in eventsTable.QueryAsync<TableEntity>(cancellationToken: ct).ConfigureAwait(false))
        {
            if (TableSchema.IsHead(entity))
                continue;
            var position = TableSchema.Position(entity);
            if (forwards ? position > from : position < from)
                selected.Add(entity);
        }

        selected.Sort((a, b) => forwards
            ? TableSchema.Position(a).CompareTo(TableSchema.Position(b))
            : TableSchema.Position(b).CompareTo(TableSchema.Position(a)));
        foreach (var entity in selected.Take(take))
            if (await TryMaterializeAsync(entity, ct).ConfigureAwait(false) is { } stored)
                yield return stored;
    }

    public async Task<GlobalPosition> FindPositionAtAsync(DateTimeOffset timestamp, CancellationToken ct = default)
    {
        var best = GlobalPosition.Start;
        var found = false;
        await foreach (var entity in eventsTable.QueryAsync<TableEntity>(cancellationToken: ct).ConfigureAwait(false))
        {
            if (TableSchema.IsHead(entity) || entity.GetDateTimeOffset("CreatedAt") is not { } createdAt || createdAt >= timestamp)
                continue;
            var position = TableSchema.Position(entity);
            if (!found || position > best)
            {
                best = position;
                found = true;
            }
        }

        return found ? best : GlobalPosition.Start;
    }

    public async Task TruncateStreamAsync(
        string streamId,
        long beforeVersion,
        bool force = false,
        CancellationToken ct = default)
    {
        for (var attempt = 1; ; attempt++)
        {
            var head = await TryGetHeadAsync(streamId, ct).ConfigureAwait(false);
            if (head is null || TableSchema.HeadHardDeleted(head))
                throw new StreamNotFoundException(streamId);

            var boundary = Math.Min(beforeVersion, TableSchema.HeadVersion(head) + 1);
            var dataDone = boundary <= TableSchema.HeadTruncatedBefore(head);
            var markerDone = boundary <= TableSchema.HeadMarkedBefore(head);
            if (dataDone && markerDone)
                return;

            if (!dataDone)
            {
                if (!force)
                {
                    var covering = await LatestSnapshotVersionAsync(streamId, ct).ConfigureAwait(false);
                    if (covering < boundary - 1)
                        throw new NotSupportedByProviderException(
                            $"Truncating stream '{streamId}' before version {boundary} would leave it impossible to "
                            + "reconstruct: no snapshot at or after that boundary exists. Pass force to override.");
                }

                var removed = new List<TableEntity>();
                var below = $"PartitionKey eq '{Escape(streamId)}' and RowKey ge '{TableSchema.EventRowKeyLowerBound}' "
                            + $"and RowKey lt '{TableSchema.EventRowKey(boundary)}'";
                await foreach (var entity in eventsTable.QueryAsync<TableEntity>(below, cancellationToken: ct).ConfigureAwait(false))
                    removed.Add(entity);

                var actions = removed.Select(e => new TableTransactionAction(TableTransactionActionType.Delete, e, e.ETag)).ToList();
                head["TruncatedBefore"] = boundary;
                actions.Add(new TableTransactionAction(TableTransactionActionType.UpdateReplace, head, head.ETag));

                if (!await TrySubmitAsync(actions, attempt, ct).ConfigureAwait(false))
                    continue;

                await DeletePayloadBlobsAsync(removed, ct).ConfigureAwait(false);
            }

            // Emit the marker (in a different partition, so not atomic with the data) and only then
            // record that it landed, so a crash or failure before this point re-emits it on retry.
            await EmitMarkerAsync(
                SystemEventTypes.StreamTruncated, new StreamTruncatedMarker(streamId, boundary), streamId, ct)
                .ConfigureAwait(false);
            await SetHeadWatermarkAsync(streamId, "MarkedBefore", boundary, ct).ConfigureAwait(false);
            return;
        }
    }

    public async Task DeleteStreamAsync(string streamId, DeleteMode mode, CancellationToken ct = default)
    {
        for (var attempt = 1; ; attempt++)
        {
            var head = await TryGetHeadAsync(streamId, ct).ConfigureAwait(false);
            if (head is null || TableSchema.HeadHardDeleted(head))
                throw new StreamNotFoundException(streamId);

            var alreadyDeleted = TableSchema.HeadIsDeleted(head);
            if (mode == DeleteMode.Soft && alreadyDeleted && TableSchema.HeadDeleteMarked(head))
                return;

            // Redo the physical deletion unless it already happened (a soft-delete re-run whose only
            // outstanding work is the marker skips straight to emitting it).
            if (!alreadyDeleted || mode == DeleteMode.Hard)
            {
                var removed = new List<TableEntity>();
                var all = $"PartitionKey eq '{Escape(streamId)}' and RowKey ge '{TableSchema.EventRowKeyLowerBound}' "
                          + $"and RowKey le '{TableSchema.EventRowKeyUpperBound}'";
                await foreach (var entity in eventsTable.QueryAsync<TableEntity>(all, cancellationToken: ct).ConfigureAwait(false))
                    removed.Add(entity);

                var actions = removed.Select(e => new TableTransactionAction(TableTransactionActionType.Delete, e, e.ETag)).ToList();
                head["IsDeleted"] = true;
                head["TruncatedBefore"] = TableSchema.HeadVersion(head) + 1;
                head["DeleteMarked"] = false;
                if (mode == DeleteMode.Hard)
                    head["HardDeleted"] = true;
                actions.Add(new TableTransactionAction(TableTransactionActionType.UpdateReplace, head, head.ETag));

                if (!await TrySubmitAsync(actions, attempt, ct).ConfigureAwait(false))
                    continue;

                await DeletePayloadBlobsAsync(removed, ct).ConfigureAwait(false);
                await DeleteAllSnapshotsAsync(streamId, ct).ConfigureAwait(false);
            }

            await EmitMarkerAsync(
                SystemEventTypes.StreamDeleted, new StreamDeletedMarker(streamId, mode), inBandStream: null, ct)
                .ConfigureAwait(false);
            await SetHeadWatermarkAsync(streamId, "DeleteMarked", true, ct).ConfigureAwait(false);
            return;
        }
    }

    public async Task RedactEventAsync(
        string streamId,
        long version,
        ReadOnlyMemory<byte> newData,
        ReadOnlyMemory<byte>? newMetadata = null,
        CancellationToken ct = default)
    {
        for (var attempt = 1; ; attempt++)
        {
            var response = await eventsTable
                .GetEntityIfExistsAsync<TableEntity>(streamId, TableSchema.EventRowKey(version), cancellationToken: ct)
                .ConfigureAwait(false);
            if (!response.HasValue)
                throw new StreamNotFoundException(streamId);

            var entity = response.Value!;
            var priorData = TableSchema.ReadSlot(entity, "Data");
            var priorMeta = TableSchema.ReadSlot(entity, "Meta");

            var data = await payloads.PutAsync(TableSchema.DataBlobName(streamId, version), newData, ct).ConfigureAwait(false);
            var metadata = await payloads.PutAsync(TableSchema.MetaBlobName(streamId, version), newMetadata, ct).ConfigureAwait(false);
            TableSchema.WriteSlot(entity, "Data", data);
            TableSchema.WriteSlot(entity, "Meta", metadata);

            try
            {
                await eventsTable.UpdateEntityAsync(entity, entity.ETag, TableUpdateMode.Replace, ct).ConfigureAwait(false);
            }
            catch (RequestFailedException) when (attempt < 32)
            {
                continue; // the row changed under us — re-read and redact again
            }

            // Drop a spilled blob the redaction no longer references (the payload shrank to inline).
            if (priorData is { Mode: PayloadMode.Blob } && data.Mode != PayloadMode.Blob)
                await payloads.DeleteAsync(priorData.BlobName!, ct).ConfigureAwait(false);
            if (priorMeta is { Mode: PayloadMode.Blob } && metadata.Mode != PayloadMode.Blob)
                await payloads.DeleteAsync(priorMeta.BlobName!, ct).ConfigureAwait(false);

            await EmitMarkerAsync(
                SystemEventTypes.EventRedacted, new EventRedactedMarker(streamId, version), streamId, ct)
                .ConfigureAwait(false);
            return;
        }
    }

    public async Task<ScavengeReport> ScavengeAsync(ScavengeOptions? options = null, CancellationToken ct = default)
    {
        var cutoff = timeProvider.GetUtcNow() - tombstoneRetention;
        var expired = new List<TableEntity>();
        var systemFilter = $"PartitionKey eq '{Escape(TableSchema.SystemStream)}' and RowKey ge '{TableSchema.EventRowKeyLowerBound}' "
                           + $"and RowKey le '{TableSchema.EventRowKeyUpperBound}'";
        await foreach (var entity in eventsTable.QueryAsync<TableEntity>(systemFilter, cancellationToken: ct).ConfigureAwait(false))
        {
            if (entity.GetDateTimeOffset("CreatedAt") is { } createdAt && createdAt < cutoff)
                expired.Add(entity);
        }

        foreach (var chunk in expired.Chunk(100))
        {
            await eventsTable.SubmitTransactionAsync(
                    chunk.Select(e => new TableTransactionAction(TableTransactionActionType.Delete, e, e.ETag)), ct)
                .ConfigureAwait(false);
        }

        await DeletePayloadBlobsAsync(expired, ct).ConfigureAwait(false);

        var streams = 0;
        await foreach (var _ in eventsTable
                           .QueryAsync<TableEntity>($"RowKey eq '{TableSchema.HeadRowKey}'", cancellationToken: ct)
                           .ConfigureAwait(false))
            streams++;

        return new ScavengeReport(streams, expired.Count);
    }

    // Resolves an event's payloads (downloading blobs for spilled fields). Returns null when a
    // referenced blob is gone — the event's deletion committed between reading the row and its blob, so
    // the event no longer exists; the reader skips it rather than surfacing a raw storage error.
    private async Task<StoredEvent?> TryMaterializeAsync(TableEntity entity, CancellationToken ct)
    {
        try
        {
            var data = await payloads.GetAsync(TableSchema.ReadSlot(entity, "Data"), ct).ConfigureAwait(false)
                       ?? ReadOnlyMemory<byte>.Empty;
            var metadata = await payloads.GetAsync(TableSchema.ReadSlot(entity, "Meta"), ct).ConfigureAwait(false);
            return TableSchema.ToStored(entity, data, metadata);
        }
        catch (RequestFailedException e) when (e.Status == 404)
        {
            return null;
        }
    }

    private async Task DeletePayloadBlobsAsync(IEnumerable<TableEntity> entities, CancellationToken ct)
    {
        foreach (var entity in entities)
        {
            foreach (var prefix in new[] { "Data", "Meta" })
            {
                var slot = TableSchema.ReadSlot(entity, prefix);
                if (slot.Mode == PayloadMode.Blob)
                    await payloads.DeleteAsync(slot.BlobName!, ct).ConfigureAwait(false);
            }
        }
    }

    private async Task EmitMarkerAsync(string eventType, object marker, string? inBandStream, CancellationToken ct)
    {
        var streamId = inBandMarkers && inBandStream is not null ? inBandStream : TableSchema.SystemStream;
        var data = JsonSerializer.SerializeToUtf8Bytes(marker, marker.GetType());
        var markerEvent = new EventData(Guid.NewGuid(), eventType, data);
        await AppendToStreamAsync(streamId, ExpectedVersion.Any, [markerEvent], ct).ConfigureAwait(false);
    }

    // Submits a same-partition transaction (chunked for very large deletes). Returns false on an etag
    // conflict so the caller retries against re-read state; other failures propagate.
    private async Task<bool> TrySubmitAsync(IReadOnlyList<TableTransactionAction> actions, int attempt, CancellationToken ct)
    {
        try
        {
            foreach (var chunk in actions.Chunk(100))
                await eventsTable.SubmitTransactionAsync(chunk, ct).ConfigureAwait(false);
            return true;
        }
        catch (RequestFailedException) when (attempt < 32)
        {
            return false;
        }
    }

    private async Task<TableEntity?> TryGetHeadAsync(string streamId, CancellationToken ct)
    {
        var response = await eventsTable
            .GetEntityIfExistsAsync<TableEntity>(streamId, TableSchema.HeadRowKey, cancellationToken: ct)
            .ConfigureAwait(false);
        return response.HasValue ? response.Value : null;
    }

    // Records that a destructive op's marker has been emitted (a merge so it never clobbers a concurrent
    // version change). Retried on an etag conflict; if the whole op is retried later the marker is simply
    // re-emitted, since this watermark is only advanced after the marker lands.
    private async Task SetHeadWatermarkAsync(string streamId, string property, object value, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            var head = await TryGetHeadAsync(streamId, ct).ConfigureAwait(false);
            if (head is null)
                return;

            head[property] = value;
            try
            {
                await eventsTable.UpdateEntityAsync(head, head.ETag, TableUpdateMode.Merge, ct).ConfigureAwait(false);
                return;
            }
            catch (RequestFailedException) when (attempt < 32)
            {
                // The head changed under us (a concurrent append); re-read and merge again.
            }
        }
    }

    private async Task<List<TableEntity>> FindByEventIdsAsync(string streamId, HashSet<Guid> batchIds, CancellationToken ct)
    {
        var ids = string.Join(" or ", batchIds.Select(id => $"EventId eq '{id:N}'"));
        var filter = $"PartitionKey eq '{Escape(streamId)}' and ({ids})";
        var found = new List<TableEntity>();
        await foreach (var entity in eventsTable.QueryAsync<TableEntity>(filter, cancellationToken: ct).ConfigureAwait(false))
            found.Add(entity);
        return found;
    }

    private async Task<long> LatestSnapshotVersionAsync(string streamId, CancellationToken ct)
    {
        await foreach (var entity in snapshots
                           .QueryAsync<TableEntity>($"PartitionKey eq '{Escape(streamId)}'", maxPerPage: 1, cancellationToken: ct)
                           .ConfigureAwait(false))
            return TableSchema.SnapshotVersion(entity);
        return 0;
    }

    private async Task DeleteAllSnapshotsAsync(string streamId, CancellationToken ct)
    {
        var stale = new List<TableEntity>();
        await foreach (var entity in snapshots
                           .QueryAsync<TableEntity>($"PartitionKey eq '{Escape(streamId)}'", cancellationToken: ct)
                           .ConfigureAwait(false))
            stale.Add(entity);

        foreach (var chunk in stale.Chunk(100))
        {
            await snapshots.SubmitTransactionAsync(
                    chunk.Select(e => new TableTransactionAction(TableTransactionActionType.Delete, e, e.ETag)), ct)
                .ConfigureAwait(false);
        }

        foreach (var entity in stale)
        {
            var slot = TableSchema.ReadSlot(entity, "Payload");
            if (slot.Mode == PayloadMode.Blob)
                await payloads.DeleteAsync(slot.BlobName!, ct).ConfigureAwait(false);
        }
    }

    private GlobalPosition[] AllocatePositions(int count)
    {
        // A per-instance monotonic tick, so events across the domain get strictly increasing positions
        // in a single process even when the clock does not advance between appends. Across processes the
        // positions may interleave — hence no ExactCatchup.
        long baseTick;
        lock (_tickGate)
        {
            baseTick = Math.Max(timeProvider.GetUtcNow().UtcTicks, _lastTick + 1);
            _lastTick = baseTick + count - 1;
        }

        var positions = new GlobalPosition[count];
        for (var i = 0; i < count; i++)
            positions[i] = GlobalPosition.FromSequence(baseTick + i);
        return positions;
    }

    private static bool TryReplay(List<TableEntity> stored, int expectedCount, out AppendResult result)
    {
        if (stored.Count == expectedCount && expectedCount > 0)
        {
            var last = stored.MaxBy(TableSchema.Version)!;
            result = new AppendResult(TableSchema.Version(last), TableSchema.Position(last));
            return true;
        }

        result = null!;
        return false;
    }

    private static bool IsVersionAgnostic(long expectedVersion, long actualVersion) =>
        expectedVersion == ExpectedVersion.Any
        || (expectedVersion == ExpectedVersion.StreamExists && actualVersion > 0);

    private static void EnsureExpectedVersion(string streamId, long expectedVersion, long currentVersion)
    {
        var satisfied = expectedVersion switch
        {
            ExpectedVersion.Any => true,
            ExpectedVersion.NoStream => currentVersion == 0,
            ExpectedVersion.StreamExists => currentVersion > 0,
            _ => expectedVersion == currentVersion
        };

        if (!satisfied)
            throw new WrongExpectedVersionException(streamId, expectedVersion, currentVersion);
    }

    private static int Clamp(long maxCount) => maxCount >= int.MaxValue ? int.MaxValue : (int)Math.Max(0, maxCount);

    private static string Escape(string value) => value.Replace("'", "''");
}
