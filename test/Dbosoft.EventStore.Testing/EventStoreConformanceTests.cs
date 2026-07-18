using System.Collections.Concurrent;
using AwesomeAssertions;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.Testing;

/// <summary>
/// The provider-agnostic contract every event store must satisfy. A provider derives this class,
/// supplies a fresh store from <see cref="CreateStore"/>, and inherits the whole suite. Tests for
/// optional features are gated on the store's declared <see cref="StoreCapabilities"/>, so a
/// provider is only held to what it claims to support.
/// </summary>
public abstract class EventStoreConformanceTests
{
    /// <summary>Creates a fresh, empty store for a single test.</summary>
    protected abstract IEventStore CreateStore();

    /// <summary>
    /// Returns the snapshot store paired with a store, or <c>null</c> if the provider has none.
    /// Providers with snapshot support override this to supply their snapshot store.
    /// </summary>
    protected virtual ISnapshotStore? GetSnapshotStore(IEventStore store) => null;

    // ---- core: append + read ----

    [Fact]
    public async Task Append_to_new_stream_then_read_round_trips()
    {
        var store = CreateStore();

        var result = await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));

        result.NextExpectedVersion.Should().Be(3);
        var events = await store.ReadAllOf("s1");
        events.Should().HaveCount(3);
        events.Select(e => e.StreamVersion).Should().Equal(1, 2, 3);
        events.Should().OnlyContain(e => e.StreamId == "s1");
    }

    [Fact]
    public async Task Append_assigns_contiguous_versions_across_calls()
    {
        var store = CreateStore();

        var first = await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(2));
        var second = await store.AppendToStreamAsync("s1", first.NextExpectedVersion, Events(2));

        second.NextExpectedVersion.Should().Be(4);
        (await store.ReadAllOf("s1")).Select(e => e.StreamVersion).Should().Equal(1, 2, 3, 4);
    }

    [Fact]
    public async Task Append_with_wrong_expected_version_throws_with_actual()
    {
        var store = CreateStore();
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(2));

        var act = () => store.AppendToStreamAsync("s1", 0, Events(1));

        var ex = await act.Should().ThrowAsync<WrongExpectedVersionException>();
        ex.Which.ActualVersion.Should().Be(2);
        ex.Which.ExpectedVersion.Should().Be(0);
    }

    [Fact]
    public async Task Append_with_expected_any_ignores_version()
    {
        var store = CreateStore();
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));

        var act = () => store.AppendToStreamAsync("s1", ExpectedVersion.Any, Events(1));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task Append_no_stream_fails_when_stream_exists()
    {
        var store = CreateStore();
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));

        var act = () => store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));

        await act.Should().ThrowAsync<WrongExpectedVersionException>();
    }

    [Fact]
    public async Task Append_stream_exists_fails_when_missing()
    {
        var store = CreateStore();

        var act = () => store.AppendToStreamAsync("s1", ExpectedVersion.StreamExists, Events(1));

        await act.Should().ThrowAsync<WrongExpectedVersionException>();
    }

    [Fact]
    public async Task Append_is_idempotent_by_event_id()
    {
        var store = CreateStore();
        var evt = Event();

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, [evt]);
        await store.AppendToStreamAsync("s1", ExpectedVersion.Any, [evt]);

        (await store.ReadAllOf("s1")).Should().HaveCount(1);
    }

    [Fact]
    public async Task Read_backwards_returns_descending()
    {
        var store = CreateStore();
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));

        var events = new List<StoredEvent>();
        await foreach (var e in store.ReadStreamAsync("s1", ReadDirection.Backwards, long.MaxValue))
            events.Add(e);

        events.Select(e => e.StreamVersion).Should().Equal(3, 2, 1);
    }

    [Fact]
    public async Task Read_from_version_skips_earlier_events()
    {
        var store = CreateStore();
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(5));

        var events = new List<StoredEvent>();
        await foreach (var e in store.ReadStreamAsync("s1", ReadDirection.Forwards, 3))
            events.Add(e);

        events.Select(e => e.StreamVersion).Should().Equal(3, 4, 5);
    }

    [Fact]
    public async Task Read_respects_max_count()
    {
        var store = CreateStore();
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(5));

        var events = new List<StoredEvent>();
        await foreach (var e in store.ReadStreamAsync("s1", ReadDirection.Forwards, 0, 2))
            events.Add(e);

        events.Should().HaveCount(2);
    }

    [Fact]
    public async Task Read_missing_stream_is_empty()
    {
        var store = CreateStore();
        (await store.ReadAllOf("missing")).Should().BeEmpty();
    }

    [Fact]
    public async Task Stream_info_is_null_for_missing_stream()
    {
        var store = CreateStore();
        (await store.GetStreamInfoAsync("missing")).Should().BeNull();
    }

    [Fact]
    public async Task Stream_info_reports_current_version()
    {
        var store = CreateStore();
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(4));

        var info = await store.GetStreamInfoAsync("s1");

        info.Should().NotBeNull();
        info.Version.Should().Be(4);
    }

    [Fact]
    public async Task Empty_append_is_a_no_op_returning_the_current_version()
    {
        var store = CreateStore();

        // On a missing stream: a no-op that leaves no trace — no phantom stream is materialized.
        var onMissing = await store.AppendToStreamAsync("s1", ExpectedVersion.Any, []);
        onMissing.NextExpectedVersion.Should().Be(0);
        (await store.GetStreamInfoAsync("s1")).Should().BeNull();

        // On an existing stream: reports the current version and writes nothing.
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));
        var onExisting = await store.AppendToStreamAsync("s1", ExpectedVersion.Any, []);
        onExisting.NextExpectedVersion.Should().Be(3);
        (await store.ReadAllOf("s1")).Should().HaveCount(3);
    }

    [Fact]
    public async Task Empty_append_still_honours_the_expected_version()
    {
        var store = CreateStore();
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));

        // An empty append writes nothing, but it is still an optimistic-concurrency-checked no-op: a
        // mismatched expected version is rejected, so it can gate "commit only if still at version N".
        var mismatch = () => store.AppendToStreamAsync("s1", 1, []);
        await mismatch.Should().ThrowAsync<WrongExpectedVersionException>();

        var matched = await store.AppendToStreamAsync("s1", 3, []);
        matched.NextExpectedVersion.Should().Be(3);
    }

    [Fact]
    public async Task Empty_append_to_a_soft_deleted_stream_leaves_it_deleted()
    {
        var store = CreateStore();
        if (store is not IStreamMaintenance maintenance || !store.Capabilities.HasFlag(StoreCapabilities.StreamDeletion))
            return;

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));
        await maintenance.DeleteStreamAsync("s1", DeleteMode.Soft);

        await store.AppendToStreamAsync("s1", ExpectedVersion.Any, []);

        // The no-op must not resurrect the stream: it stays soft-deleted and reads empty.
        var info = await store.GetStreamInfoAsync("s1");
        info.Should().NotBeNull();
        info.IsDeleted.Should().BeTrue();
        (await store.ReadAllOf("s1")).Should().BeEmpty();
    }

    [Fact]
    public async Task A_rejected_append_does_not_materialize_the_stream()
    {
        var store = CreateStore();

        var act = () => store.AppendToStreamAsync("s1", ExpectedVersion.StreamExists, Events(1));
        await act.Should().ThrowAsync<WrongExpectedVersionException>();

        // A rejected append must leave no trace: the stream still does not exist.
        (await store.GetStreamInfoAsync("s1")).Should().BeNull();
    }

    [Fact]
    public async Task Concurrent_any_appends_of_different_batches_all_succeed()
    {
        var store = CreateStore();
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));

        // ExpectedVersion.Any means "append regardless of the current version". Every racer must land;
        // none may be rejected as a version conflict just because another writer moved the head first.
        var failures = new ConcurrentBag<Exception>();
        var racers = Enumerable.Range(0, 12).Select(_ => Task.Run(async () =>
        {
            try
            {
                await store.AppendToStreamAsync("s1", ExpectedVersion.Any, Events(1));
            }
            catch (Exception ex)
            {
                failures.Add(ex);
            }
        }));
        await Task.WhenAll(racers);

        failures.Should().BeEmpty();
        (await store.ReadAllOf("s1")).Should().HaveCount(13);
    }

    [Fact]
    public async Task Concurrent_appends_to_same_version_yield_exactly_one_winner()
    {
        var store = CreateStore();
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));

        // 16 racers all believe the stream is at version 1. The store must let exactly one win and
        // reject the rest — this is the race the naive read-then-check implementation loses.
        var racers = Enumerable.Range(0, 16)
            .Select(_ => Task.Run(async () =>
            {
                try
                {
                    await store.AppendToStreamAsync("s1", 1, Events(1));
                    return true;
                }
                catch (WrongExpectedVersionException)
                {
                    return false;
                }
            }));

        var winners = (await Task.WhenAll(racers)).Count(won => won);

        winners.Should().Be(1);
        (await store.ReadAllOf("s1")).Should().HaveCount(2);
    }

    // ---- optional: global reads ----

    [Fact]
    public async Task All_stream_read_returns_events_across_streams_in_order()
    {
        var store = CreateStore();
        if (store is not IAllStreamReader reader || !store.Capabilities.HasFlag(StoreCapabilities.AllStreamReads))
            return;

        await store.AppendToStreamAsync("a", ExpectedVersion.NoStream, Events(2));
        await store.AppendToStreamAsync("b", ExpectedVersion.NoStream, Events(2));
        await store.AppendToStreamAsync("a", 2, Events(1));

        var all = new List<StoredEvent>();
        await foreach (var e in reader.ReadAllAsync(GlobalPosition.Start))
            all.Add(e);

        all.Should().HaveCount(5);
        all.Select(e => e.StreamId).Should().Equal("a", "a", "b", "b", "a");
        all.Select(e => e.Position).Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task All_stream_read_from_position_is_exclusive()
    {
        var store = CreateStore();
        if (store is not IAllStreamReader reader || !store.Capabilities.HasFlag(StoreCapabilities.AllStreamReads))
            return;

        await store.AppendToStreamAsync("a", ExpectedVersion.NoStream, Events(3));
        var all = new List<StoredEvent>();
        await foreach (var e in reader.ReadAllAsync(GlobalPosition.Start))
            all.Add(e);

        var after = new List<StoredEvent>();
        await foreach (var e in reader.ReadAllAsync(all[0].Position))
            after.Add(e);

        after.Should().HaveCount(2);
        after[0].Position.Should().Be(all[1].Position);
    }

    // ---- idempotency (batch-scoped) ----

    [Fact]
    public async Task Exact_replay_of_a_batch_is_an_idempotent_no_op()
    {
        var store = CreateStore();
        var batch = Events(3);

        var first = await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, batch);
        var replay = await store.AppendToStreamAsync("s1", ExpectedVersion.Any, batch);

        // The replay returns the outcome of the original append operation, and nothing is written twice.
        replay.NextExpectedVersion.Should().Be(first.NextExpectedVersion);
        (await store.ReadAllOf("s1")).Should().HaveCount(3);
    }

    [Fact]
    public async Task Partial_overlap_of_a_batch_is_rejected()
    {
        var store = CreateStore();
        var a = Event();
        var b = Event();
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, [a, b]);

        // [b, c] partially overlaps [a, b]: neither a clean retry nor a fully new append.
        var act = () => store.AppendToStreamAsync("s1", ExpectedVersion.Any, [b, Event()]);

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task Duplicate_event_id_within_a_batch_is_rejected()
    {
        var store = CreateStore();
        var evt = Event();

        var act = () => store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, [evt, evt]);

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task Concurrent_identical_batch_appends_are_idempotent()
    {
        var store = CreateStore();
        var batch = Events(3);

        // At-least-once redelivery of the very same batch: many callers race to append identical
        // events. Idempotency is batch-scoped, so every caller must see success at the same version
        // and the batch must land exactly once — no caller may get a version conflict.
        var outcomes = new ConcurrentBag<AppendResult>();
        var failures = new ConcurrentBag<Exception>();
        var racers = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            try
            {
                outcomes.Add(await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, batch));
            }
            catch (Exception ex)
            {
                failures.Add(ex);
            }
        }));
        await Task.WhenAll(racers);

        failures.Should().BeEmpty();
        outcomes.Should().OnlyContain(r => r.NextExpectedVersion == 3);
        (await store.ReadAllOf("s1")).Should().HaveCount(3);
    }

    // ---- optional: time navigation ----

    [Fact]
    public async Task Time_navigation_finds_position_at_timestamp()
    {
        var store = CreateStore();
        if (store is not ITimeNavigable navigable || !store.Capabilities.HasFlag(StoreCapabilities.TimeNavigation))
            return;
        if (store is not IAllStreamReader reader)
            return;

        await store.AppendToStreamAsync("a", ExpectedVersion.NoStream, Events(2));

        var position = await navigable.FindPositionAtAsync(DateTimeOffset.MinValue);

        var all = new List<StoredEvent>();
        await foreach (var e in reader.ReadAllAsync(position))
            all.Add(e);
        all.Should().HaveCount(2);
    }

    // ---- optional: truncation ----

    [Fact]
    public async Task Truncation_without_snapshot_is_refused_but_forced_succeeds()
    {
        var store = CreateStore();
        if (store is not IStreamMaintenance maintenance || !store.Capabilities.HasFlag(StoreCapabilities.Truncation))
            return;

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(5));

        var refused = () => maintenance.TruncateStreamAsync("s1", 3);
        await refused.Should().ThrowAsync<NotSupportedByProviderException>();

        await maintenance.TruncateStreamAsync("s1", 3, force: true);

        (await store.ReadAllOf("s1")).Select(e => e.StreamVersion).Should().Equal(3, 4, 5);
        var info = await store.GetStreamInfoAsync("s1");
        info.Should().NotBeNull();
        info.TruncatedBefore.Should().Be(3);
        info.Version.Should().Be(5);
    }

    [Fact]
    public async Task Truncation_beyond_the_head_is_clamped_to_the_end()
    {
        var store = CreateStore();
        if (store is not IStreamMaintenance maintenance || !store.Capabilities.HasFlag(StoreCapabilities.Truncation))
            return;

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(5));

        // A truncation point past the head drops every event but must not leave the head reporting a
        // nonsensical boundary beyond where the stream ends.
        await maintenance.TruncateStreamAsync("s1", 100, force: true);

        (await store.ReadAllOf("s1")).Should().BeEmpty();
        var info = await store.GetStreamInfoAsync("s1");
        info.Should().NotBeNull();
        info.TruncatedBefore.Should().Be(6);
        info.Version.Should().Be(5);
    }

    [Fact]
    public async Task Truncation_is_allowed_with_a_covering_snapshot()
    {
        var store = CreateStore();
        if (store is not IStreamMaintenance maintenance || !store.Capabilities.HasFlag(StoreCapabilities.Truncation))
            return;
        var snapshots = GetSnapshotStore(store);
        if (snapshots is null)
            return;

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(5));
        await snapshots.SaveAsync("s1", 3, Event("Snapshot").Data);

        var act = () => maintenance.TruncateStreamAsync("s1", 3);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task Truncation_emits_a_marker_into_the_global_stream()
    {
        var store = CreateStore();
        if (store is not IStreamMaintenance maintenance || store is not IAllStreamReader reader
            || !store.Capabilities.HasFlag(StoreCapabilities.Truncation))
            return;
        var snapshots = GetSnapshotStore(store);
        if (snapshots is null)
            return;

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(5));
        await snapshots.SaveAsync("s1", 3, Event("Snapshot").Data);
        await maintenance.TruncateStreamAsync("s1", 3);

        var all = new List<StoredEvent>();
        await foreach (var e in reader.ReadAllAsync(GlobalPosition.Start))
            all.Add(e);

        all.Should().Contain(e => e.EventType == SystemEventTypes.StreamTruncated);
    }

    // ---- optional: redaction ----

    [Fact]
    public async Task Redaction_overwrites_the_payload_in_place_and_emits_a_marker()
    {
        var store = CreateStore();
        if (store is not IStreamMaintenance maintenance || store is not IAllStreamReader reader
            || !store.Capabilities.HasFlag(StoreCapabilities.Redaction))
            return;

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, [Event("Registered", "{\"pii\":\"secret\"}")]);
        var redacted = "{\"pii\":\"[redacted]\"}"u8.ToArray();
        await maintenance.RedactEventAsync("s1", 1, redacted);

        var events = await store.ReadAllOf("s1");
        events.Should().ContainSingle();
        events[0].StreamVersion.Should().Be(1); // the slot survives — shape preserved
        events[0].Data.ToArray().Should().Equal(redacted);

        var all = new List<StoredEvent>();
        await foreach (var e in reader.ReadAllAsync(GlobalPosition.Start))
            all.Add(e);
        all.Should().Contain(e => e.EventType == SystemEventTypes.EventRedacted);
    }

    // ---- optional: deletion ----

    [Fact]
    public async Task Soft_delete_removes_events_and_allows_recreation()
    {
        var store = CreateStore();
        if (store is not IStreamMaintenance maintenance || !store.Capabilities.HasFlag(StoreCapabilities.StreamDeletion))
            return;

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));
        await maintenance.DeleteStreamAsync("s1", DeleteMode.Soft);

        (await store.ReadAllOf("s1")).Should().BeEmpty();

        var act = () => store.AppendToStreamAsync("s1", ExpectedVersion.Any, Events(1));
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task Hard_delete_burns_the_stream_id()
    {
        var store = CreateStore();
        if (store is not IStreamMaintenance maintenance || !store.Capabilities.HasFlag(StoreCapabilities.StreamDeletion))
            return;

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));
        await maintenance.DeleteStreamAsync("s1", DeleteMode.Hard);

        var act = () => store.AppendToStreamAsync("s1", ExpectedVersion.Any, Events(1));
        await act.Should().ThrowAsync<StreamDeletedException>();
    }

    [Fact]
    public async Task Soft_delete_then_recreate_continues_the_version_sequence()
    {
        var store = CreateStore();
        if (store is not IStreamMaintenance maintenance || !store.Capabilities.HasFlag(StoreCapabilities.StreamDeletion))
            return;

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));
        await maintenance.DeleteStreamAsync("s1", DeleteMode.Soft);

        // A soft-deleted id is reusable, but versioning resumes where it left off rather than resetting
        // — the recreation path is an ordinary append, not a fresh NoStream one.
        var recreated = await store.AppendToStreamAsync("s1", ExpectedVersion.Any, Events(1));

        recreated.NextExpectedVersion.Should().Be(4);
        var events = await store.ReadAllOf("s1");
        events.Should().ContainSingle();
        events[0].StreamVersion.Should().Be(4);
    }

    [Fact]
    public async Task Repeated_soft_delete_is_idempotent_and_emits_one_marker()
    {
        var store = CreateStore();
        if (store is not IStreamMaintenance maintenance || store is not IAllStreamReader reader
            || !store.Capabilities.HasFlag(StoreCapabilities.StreamDeletion)
            || !store.Capabilities.HasFlag(StoreCapabilities.AllStreamReads))
            return;

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));
        await maintenance.DeleteStreamAsync("s1", DeleteMode.Soft);
        await maintenance.DeleteStreamAsync("s1", DeleteMode.Soft);

        // Deleting an already-soft-deleted stream is a no-op: it must not re-emit a tombstone.
        var all = new List<StoredEvent>();
        await foreach (var e in reader.ReadAllAsync(GlobalPosition.Start))
            all.Add(e);

        all.Count(e => e.EventType == SystemEventTypes.StreamDeleted).Should().Be(1);
    }

    [Fact]
    public async Task Concurrent_maintenance_on_distinct_streams_emits_every_marker_without_raw_errors()
    {
        var store = CreateStore();
        if (store is not IStreamMaintenance maintenance || store is not IAllStreamReader reader
            || !store.Capabilities.HasFlag(StoreCapabilities.StreamDeletion)
            || !store.Capabilities.HasFlag(StoreCapabilities.AllStreamReads))
            return;

        const int streams = 8;
        for (var i = 0; i < streams; i++)
            await store.AppendToStreamAsync($"s{i}", ExpectedVersion.NoStream, Events(2));

        // Deleting distinct streams concurrently all target the shared $system marker head. Each op must
        // complete — retrying its marker write on a version race — rather than leak a raw provider
        // exception, and every tombstone must land exactly once.
        var failures = new ConcurrentBag<Exception>();
        var ops = Enumerable.Range(0, streams).Select(i => Task.Run(async () =>
        {
            try
            {
                await maintenance.DeleteStreamAsync($"s{i}", DeleteMode.Soft);
            }
            catch (Exception ex)
            {
                failures.Add(ex);
            }
        }));
        await Task.WhenAll(ops);

        failures.Should().BeEmpty();

        var markers = 0;
        await foreach (var e in reader.ReadAllAsync(GlobalPosition.Start))
            if (e.EventType == SystemEventTypes.StreamDeleted)
                markers++;
        markers.Should().Be(streams);
    }
}
