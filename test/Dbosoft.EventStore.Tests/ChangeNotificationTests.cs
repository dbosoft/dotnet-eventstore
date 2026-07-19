using AwesomeAssertions;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Subscriptions;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.Tests;

public sealed class ChangeNotificationTests
{
    [Fact]
    public async Task Notify_releases_waiters_for_that_domain_only()
    {
        var notifier = new InProcessChangeNotifier();
        var waitOnA = notifier.WaitForChangeAsync("a");
        var waitOnB = notifier.WaitForChangeAsync("b");

        await notifier.NotifyAsync("a");

        await waitOnA.WaitAsync(TimeSpan.FromSeconds(2)); // released
        await FluentActions.Awaiting(() => waitOnB.WaitAsync(TimeSpan.FromMilliseconds(100)))
            .Should().ThrowAsync<TimeoutException>(); // "b" was not signalled
    }

    [Fact]
    public async Task The_store_signals_a_change_on_append()
    {
        var notifier = new InProcessChangeNotifier();
        var store = new InMemoryEventStore(changeNotifier: notifier, notifyDomain: "banking");
        var wait = notifier.WaitForChangeAsync("banking");

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));

        await wait.WaitAsync(TimeSpan.FromSeconds(2)); // the append woke the waiter — no error means it completed
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
