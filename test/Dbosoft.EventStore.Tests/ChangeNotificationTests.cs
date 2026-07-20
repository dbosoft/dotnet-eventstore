using AwesomeAssertions;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Subscriptions;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.Tests;

public sealed class ChangeNotificationTests
{
    [Fact]
    public async Task A_watcher_receives_changes_for_its_domain_only()
    {
        var notifier = new InProcessChangeNotifier();
        using var watchA = notifier.Watch("a");
        using var watchB = notifier.Watch("b");

        await notifier.NotifyAsync("a", "stream-1");

        (await watchA.WaitAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2))).Should().Be("stream-1");

        var noneForB = watchB.WaitAsync().AsTask().WaitAsync(TimeSpan.FromMilliseconds(100));
        await FluentActions.Awaiting(() => noneForB).Should().ThrowAsync<TimeoutException>(); // "b" saw nothing
    }

    [Fact]
    public async Task The_store_signals_the_changed_stream_on_append()
    {
        var notifier = new InProcessChangeNotifier();
        var store = new InMemoryEventStore(changeNotifier: notifier, notifyDomain: "banking");
        using var watch = notifier.Watch("banking");

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));

        (await watch.WaitAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2))).Should().Be("s1");
    }

    [Fact]
    public async Task A_watcher_coalesces_repeated_changes_but_keeps_every_distinct_stream()
    {
        var notifier = new InProcessChangeNotifier();
        using var watch = notifier.Watch("d");

        await notifier.NotifyAsync("d", "s1");
        await notifier.NotifyAsync("d", "s1"); // coalesced with the first
        await notifier.NotifyAsync("d", "s2");

        var seen = new HashSet<string>
        {
            await watch.WaitAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2)),
            await watch.WaitAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2))
        };
        seen.Should().BeEquivalentTo("s1", "s2");

        var nothingLeft = watch.WaitAsync().AsTask().WaitAsync(TimeSpan.FromMilliseconds(100));
        await FluentActions.Awaiting(() => nothingLeft)
            .Should().ThrowAsync<TimeoutException>(); // nothing left — the duplicate s1 was coalesced
    }

    [Fact]
    public async Task RunAsync_wakes_on_a_write_without_waiting_out_the_poll_interval()
    {
        var notifier = new InProcessChangeNotifier();
        var store = new InMemoryEventStore(changeNotifier: notifier, notifyDomain: "banking");
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var subscription = new EventStoreSubscription(
            store, new InMemoryCheckpointStore(), "sub",
            (_, _) =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            },
            new SubscriptionOptions
            {
                ChangeNotifier = notifier,
                NotifyKey = "banking",
                PollInterval = TimeSpan.FromSeconds(30) // long, so a fast handling proves it was the signal, not a poll
            });

        using var cts = new CancellationTokenSource();
        var run = subscription.RunAsync(cts.Token);

        await Task.Delay(100, cts.Token); // let the loop reach its wait after the first (empty) pump
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1), cts.Token);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5), cts.Token); // must fire well before the 30s poll tick

        await cts.CancelAsync();
        try
        {
            await run;
        }
        catch (OperationCanceledException)
        {
        }
    }
}
