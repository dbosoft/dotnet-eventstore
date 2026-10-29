using AwesomeAssertions;
using Dbosoft.EventStore.Subscriptions;
using Npgsql;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.EfCore.PostgreSql.Tests;

/// <summary>
/// A subscription over a store whose writers hold their transactions open: an application appends inside a
/// message's transaction and commits only when the message is done, which can be minutes after the append
/// took its position. A reader on PostgreSQL does not wait for such a row; it does not see it. A later
/// append that committed meanwhile has a higher position and is seen.
/// </summary>
[UsedImplicitly]
[Collection(PostgreSqlCollection.Name)]
public sealed class PostgreSqlSubscriptionTests(PostgreSqlContainerFixture container)
{
    [Fact]
    public async Task An_event_committed_after_a_later_one_was_delivered_is_still_delivered()
    {
        await using var testStore = await PostgreSqlTestStore.CreateAsync(container.ConnectionString);
        var store = testStore.Provider.GetStore("test");
        var delivered = new List<string>();
        var subscription = new EventStoreSubscription(store, new Checkpoints(), "sub", (e, _) =>
        {
            delivered.Add(e.StreamId);
            return Task.CompletedTask;
        }, new SubscriptionOptions { SafetyWindow = TimeSpan.FromSeconds(1) });

        await using var conn = new NpgsqlConnection(testStore.ConnectionString);
        await conn.OpenAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await using (var unitOfWork = testStore.UnitOfWorks.Enlist(conn, tx))
        {
            // Takes the lower position, then stays uncommitted past the window, as a long message does.
            await store.AppendToStreamAsync("early", ExpectedVersion.NoStream, Events(1));

            // Another message, outside this one's transaction, commits a higher position right away.
            Task other;
            using (ExecutionContext.SuppressFlow())
                other = Task.Run(() => store.AppendToStreamAsync("late", ExpectedVersion.NoStream, Events(1)));
            await other;
            await Task.Delay(TimeSpan.FromSeconds(2));
            // The subscription reads on its own, as it does beside the messages, not through this transaction.
            // A reader that waits for the open row is let go by the commit.
            Task<int> pump;
            using (ExecutionContext.SuppressFlow())
                pump = Task.Run(() => subscription.PumpAsync());
            await Task.WhenAny(pump, Task.Delay(TimeSpan.FromSeconds(3)));

            await unitOfWork.CommitAsync();
            await pump;
        }
        await Task.Delay(TimeSpan.FromSeconds(2));
        await subscription.PumpAsync();

        delivered.Should().BeEquivalentTo(["late", "early"]);
    }

    [Fact]
    public async Task Concurrent_long_transactions_and_a_running_subscription_lose_and_repeat_nothing()
    {
        // Every earlier test of the collection keeps a pool of idle connections to its own database in the
        // shared container; these writers need connections of their own beyond them.
        NpgsqlConnection.ClearAllPools();
        await using var testStore = await PostgreSqlTestStore.CreateAsync(container.ConnectionString);
        var store = testStore.Provider.GetStore("test");
        var delivered = new List<Guid>();
        var subscription = new EventStoreSubscription(store, new Checkpoints(), "sub", (e, _) =>
        {
            lock (delivered)
                delivered.Add(e.EventId);
            return Task.CompletedTask;
        });

        const int writers = 6;
        var appended = new List<Guid>();
        var writing = Enumerable.Range(0, writers).Select(i => Task.Run(async () =>
        {
            for (var round = 0; round < 3; round++)
            {
                var events = Events(2);
                await using var conn = new NpgsqlConnection(testStore.ConnectionString);
                await conn.OpenAsync();
                await using var tx = await conn.BeginTransactionAsync();
                await using var unitOfWork = testStore.UnitOfWorks.Enlist(conn, tx);
                await store.AppendToStreamAsync($"w{i}-{round}", ExpectedVersion.NoStream, events);
                // Held open for different lengths, so commits land in another order than the inserts.
                await Task.Delay(Random.Shared.Next(0, 400));
                await unitOfWork.CommitAsync();
                lock (appended)
                    appended.AddRange(events.Select(e => e.EventId));
            }
        })).ToList();

        using var stop = new CancellationTokenSource();
        Task pumping;
        using (ExecutionContext.SuppressFlow())
            pumping = Task.Run(async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    await subscription.PumpAsync();
                    await Task.Delay(20);
                }
            });

        await Task.WhenAll(writing);
        await stop.CancelAsync();
        await pumping;
        await subscription.PumpAsync();

        delivered.Should().OnlyHaveUniqueItems().And.BeEquivalentTo(appended);
    }

    private sealed class Checkpoints : ICheckpointStore
    {
        private readonly Dictionary<string, GlobalPosition> _saved = new();

        public Task<GlobalPosition?> GetAsync(string name, CancellationToken ct = default) =>
            Task.FromResult(_saved.TryGetValue(name, out var p) ? p : (GlobalPosition?)null);

        public Task SetAsync(string name, GlobalPosition position, CancellationToken ct = default)
        {
            _saved[name] = position;
            return Task.CompletedTask;
        }
    }
}
