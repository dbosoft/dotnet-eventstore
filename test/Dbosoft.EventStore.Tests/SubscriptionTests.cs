using System.Runtime.CompilerServices;
using AwesomeAssertions;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Subscriptions;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.Tests;

public sealed class SubscriptionTests
{
    [Fact]
    public async Task Dispatches_every_event_in_order_and_resumes_from_the_checkpoint()
    {
        var store = new InMemoryEventStore();
        var checkpoints = new InMemoryCheckpointStore();
        var seen = new List<long>();
        var subscription = new EventStoreSubscription(
            store, checkpoints, "sub", (e, _) =>
            {
                seen.Add(e.StreamVersion);
                return Task.CompletedTask;
            });

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));
        var first = await subscription.PumpAsync();

        await store.AppendToStreamAsync("s1", 3, Events(2));
        var second = await subscription.PumpAsync(); // resumes after the checkpoint — only the new events

        first.Should().Be(3);
        second.Should().Be(2);
        seen.Should().Equal(1, 2, 3, 4, 5);
    }

    [Fact]
    public async Task De_duplicates_re_delivered_events_on_a_store_without_exact_catchup()
    {
        var store = new ScriptedStore(StoreCapabilities.None) { Script = ScriptedEvents(3) };
        var count = await CountDispatchedOverTwoPumps(store);

        // The scripted store re-delivers the same events on the second pump; the subscription must
        // suppress the duplicates by EventId, so the handler runs once per event across both pumps.
        count.Should().Be(3);
    }

    [Fact]
    public async Task Forwards_re_delivered_events_on_an_exact_catchup_store()
    {
        var store = new ScriptedStore(StoreCapabilities.ExactCatchup) { Script = ScriptedEvents(3) };
        var count = await CountDispatchedOverTwoPumps(store);

        // An exact-catch-up store never re-surfaces an event, so the subscription does not de-duplicate;
        // the scripted re-delivery is therefore passed straight through — the branch is observable.
        count.Should().Be(6);
    }

    [Fact]
    public async Task A_handler_failure_leaves_the_checkpoint_unmoved_and_re_delivers_the_failed_event()
    {
        var events = ScriptedEvents(3);
        var store = new ScriptedStore(StoreCapabilities.None) { Script = events };
        var checkpoints = new InMemoryCheckpointStore();
        var handled = new List<Guid>();
        var failOn = events[1].EventId;
        var thrownOnce = new HashSet<Guid>();

        var subscription = new EventStoreSubscription(
            store, checkpoints, "sub", (e, _) =>
            {
                if (e.EventId == failOn && thrownOnce.Add(e.EventId))
                    throw new InvalidOperationException("boom"); // fail the first time only
                handled.Add(e.EventId);
                return Task.CompletedTask;
            });

        await Assert.ThrowsAsync<InvalidOperationException>(() => subscription.PumpAsync());
        (await checkpoints.GetAsync("sub")).Should().BeNull(); // never advanced past the failure

        await subscription.PumpAsync();

        // Each event is handled exactly once overall: the pre-failure event is not re-run (it was
        // recorded), and the failed event is re-delivered rather than skipped as already-seen.
        handled.Should().Equal(events[0].EventId, events[1].EventId, events[2].EventId);
    }

    [Fact]
    public async Task Evicts_the_oldest_id_once_the_de_duplication_window_is_full()
    {
        var events = ScriptedEvents(3);
        var store = new ScriptedStore(StoreCapabilities.None) { Script = events };
        var handled = new List<Guid>();
        var subscription = new EventStoreSubscription(
            store, new InMemoryCheckpointStore(), "sub", (e, _) =>
            {
                handled.Add(e.EventId);
                return Task.CompletedTask;
            },
            new SubscriptionOptions { DeduplicationWindow = 2 });

        await subscription.PumpAsync(); // handles all three; the first id is evicted (window holds 2)

        store.Script = [events[0]]; // re-deliver only the evicted id
        await subscription.PumpAsync();

        // The first id fell out of the window, so its re-delivery is treated as new and handled again.
        handled.Should().Equal(events[0].EventId, events[1].EventId, events[2].EventId, events[0].EventId);
    }

    [Fact]
    public async Task RunAsync_polls_until_cancelled()
    {
        var store = new InMemoryEventStore();
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));
        var handled = new int[1];

        var subscription = new EventStoreSubscription(
            store, new InMemoryCheckpointStore(), "sub", (_, _) =>
            {
                Interlocked.Increment(ref handled[0]);
                return Task.CompletedTask;
            },
            new SubscriptionOptions { PollInterval = TimeSpan.FromMilliseconds(10) });

        var cts = new CancellationTokenSource();
        var run = subscription.RunAsync(cts.Token);

        for (var i = 0; i < 200 && Volatile.Read(ref handled[0]) < 3; i++)
            await Task.Delay(5, cts.Token);

        await cts.CancelAsync(); // the loop keeps polling after the backlog is drained; stop it
        try
        {
            await run;
        }
        catch (OperationCanceledException)
        {
            // cancellation may surface from the poll delay depending on timing
        }

        cts.Dispose();
        Volatile.Read(ref handled[0]).Should().Be(3);
    }

    [Fact]
    public async Task PumpAsync_rejects_a_reentrant_call()
    {
        var store = new InMemoryEventStore();
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));
        var holder = new EventStoreSubscription[1];
        var nested = new Exception?[1];

        holder[0] = new EventStoreSubscription(
            store, new InMemoryCheckpointStore(), "sub", async (_, ct) =>
            {
                try
                {
                    await holder[0].PumpAsync(ct); // reentrant call while the outer pump is in flight
                }
                catch (Exception ex)
                {
                    nested[0] = ex;
                }
            });

        await holder[0].PumpAsync();

        nested[0].Should().BeOfType<InvalidOperationException>();
    }

    [Fact]
    public async Task A_non_exact_store_holds_events_inside_the_safety_window()
    {
        var clock = new MutableClock(new DateTimeOffset(2026, 6, 1, 0, 0, 0, TimeSpan.Zero));
        var events = Enumerable.Range(1, 3)
            .Select(i => new StoredEvent(
                "s1", i, GlobalPosition.FromSequence(i), Guid.NewGuid(), "TestEvent",
                ReadOnlyMemory<byte>.Empty, null, "application/json", clock.Now))
            .ToList();
        var store = new ScriptedStore(StoreCapabilities.None) { Script = events }; // not exact-catch-up
        var handled = 0;
        var subscription = new EventStoreSubscription(
            store, new InMemoryCheckpointStore(), "sub",
            (_, _) =>
            {
                handled++;
                return Task.CompletedTask;
            },
            new SubscriptionOptions { SafetyWindow = TimeSpan.FromHours(1), TimeProvider = clock });

        // Recorded "now", inside the 1h window → held, checkpoint not advanced.
        (await subscription.PumpAsync()).Should().Be(0);
        handled.Should().Be(0);

        // Once they age past the window, they are delivered.
        clock.Advance(TimeSpan.FromHours(2));
        (await subscription.PumpAsync()).Should().Be(3);
        handled.Should().Be(3);
    }

    [Fact]
    public void A_non_positive_de_duplication_window_is_rejected()
    {
        var store = new ScriptedStore(StoreCapabilities.None);
        var act = () => new EventStoreSubscription(store, new InMemoryCheckpointStore(), "sub",
            (_, _) => Task.CompletedTask, new SubscriptionOptions { DeduplicationWindow = 0 });

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task StartingAt_begins_from_the_position_for_a_wall_clock_time()
    {
        var clock = new MutableClock(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var store = new InMemoryEventStore(clock);
        var seen = new List<long>();

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1)); // recorded at t0
        var cut = clock.Now.AddMinutes(1);
        clock.Advance(TimeSpan.FromHours(1));
        await store.AppendToStreamAsync("s1", 1, Events(1)); // recorded at t0 + 1h

        var subscription = await EventStoreSubscription.StartingAtAsync(
            store, new InMemoryCheckpointStore(), "sub",
            (e, _) =>
            {
                seen.Add(e.StreamVersion);
                return Task.CompletedTask;
            },
            cut);
        await subscription.PumpAsync();

        // Only the event recorded after the cut time is delivered.
        seen.Should().Equal(2);
    }

    [Fact]
    public async Task StartingAt_over_a_store_without_time_navigation_throws()
    {
        var store = new ScriptedStore(StoreCapabilities.None); // IAllStreamReader but not ITimeNavigable
        var act = () => EventStoreSubscription.StartingAtAsync(
            store, new InMemoryCheckpointStore(), "sub", (_, _) => Task.CompletedTask, DateTimeOffset.UnixEpoch);

        await act.Should().ThrowAsync<NotSupportedByProviderException>();
    }

    [Fact]
    public void Constructing_over_a_store_without_all_stream_reads_throws()
    {
        var store = new NoAllReadsStore();
        var act = () => new EventStoreSubscription(store, new InMemoryCheckpointStore(), "sub",
            (_, _) => Task.CompletedTask);

        act.Should().Throw<NotSupportedByProviderException>();
    }

    private static async Task<int> CountDispatchedOverTwoPumps(IEventStore store)
    {
        var handled = 0;
        var subscription = new EventStoreSubscription(
            store, new InMemoryCheckpointStore(), "sub", (_, _) =>
            {
                handled++;
                return Task.CompletedTask;
            });

        await subscription.PumpAsync();
        await subscription.PumpAsync();
        return handled;
    }

    private static List<StoredEvent> ScriptedEvents(int count) =>
        Enumerable.Range(1, count)
            .Select(i => new StoredEvent(
                "s1", i, GlobalPosition.FromSequence(i), Guid.NewGuid(), "TestEvent",
                ReadOnlyMemory<byte>.Empty, null, "application/json", DateTimeOffset.UnixEpoch))
            .ToList();

    // A store that replays a fixed, settable sequence regardless of the requested position, so a second
    // pump re-delivers whatever Script holds — exactly the re-read a non-exact provider may produce.
    private sealed class ScriptedStore(StoreCapabilities capabilities) : IEventStore, IAllStreamReader
    {
        public IReadOnlyList<StoredEvent> Script { get; set; } = [];

        public StoreCapabilities Capabilities => capabilities;

        public async IAsyncEnumerable<StoredEvent> ReadAllAsync(
            GlobalPosition from,
            ReadDirection direction = ReadDirection.Forwards,
            long maxCount = long.MaxValue,
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            foreach (var e in Script)
            {
                await Task.Yield();
                yield return e;
            }
        }

        public Task<AppendResult> AppendToStreamAsync(
            string streamId, long expectedVersion, IReadOnlyList<EventData> events, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public IAsyncEnumerable<StoredEvent> ReadStreamAsync(
            string streamId, ReadDirection direction = ReadDirection.Forwards,
            long fromVersion = 0, long maxCount = long.MaxValue, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<StreamInfo?> GetStreamInfoAsync(string streamId, CancellationToken ct = default) =>
            throw new NotSupportedException();
    }

    // A core-only store: it appends and reads streams but offers no $all, so a subscription cannot run.
    private sealed class NoAllReadsStore : IEventStore
    {
        public StoreCapabilities Capabilities => StoreCapabilities.None;

        public Task<AppendResult> AppendToStreamAsync(
            string streamId, long expectedVersion, IReadOnlyList<EventData> events, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public IAsyncEnumerable<StoredEvent> ReadStreamAsync(
            string streamId, ReadDirection direction = ReadDirection.Forwards,
            long fromVersion = 0, long maxCount = long.MaxValue, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<StreamInfo?> GetStreamInfoAsync(string streamId, CancellationToken ct = default) =>
            throw new NotSupportedException();
    }
}
