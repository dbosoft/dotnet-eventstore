using AwesomeAssertions;
using Dbosoft.EventStore.Subscriptions;
using Microsoft.Data.SqlClient;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.EfCore.SqlServer.Tests;

/// <summary>
/// A subscription over a store whose writers hold their transactions open: an application appends inside a
/// message's transaction and commits only when the message is done, which can be minutes after the append
/// took its position. Under read-committed snapshot, which Azure SQL and every database set up for an
/// application's state run with, a reader does not wait for such a row; it does not see it. A later append
/// that committed meanwhile has a higher position and is seen. Without it, the reader waits for the row.
/// </summary>
[UsedImplicitly]
[Collection(SqlServerCollection.Name)]
public sealed class SqlServerSubscriptionTests(SqlServerContainerFixture container)
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task An_event_committed_after_a_later_one_was_delivered_is_still_delivered(bool readCommittedSnapshot)
    {
        await using var testStore = await SqlServerTestStore.CreateAsync(container.ConnectionString, readCommittedSnapshot);
        var store = testStore.Provider.GetStore("test");
        var delivered = new List<string>();
        var subscription = new EventStoreSubscription(store, new Checkpoints(), "sub", (e, _) =>
        {
            delivered.Add(e.StreamId);
            return Task.CompletedTask;
        });

        await using var conn = new SqlConnection(testStore.ConnectionString);
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

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task An_append_to_a_stream_an_open_frame_also_writes_waits_for_it_and_neither_deadlocks(bool readCommittedSnapshot)
    {
        await using var testStore = await SqlServerTestStore.CreateAsync(container.ConnectionString, readCommittedSnapshot);
        var store = testStore.Provider.GetStore("test");

        await using var conn = new SqlConnection(testStore.ConnectionString);
        await conn.OpenAsync();
        await using var tx = await conn.BeginTransactionAsync();
        Task other;
        await using (var unitOfWork = testStore.UnitOfWorks.Enlist(conn, tx))
        {
            await store.AppendToStreamAsync("s", ExpectedVersion.Any, Events(1));

            // An append of its own to the same stream waits on the frame's stream head. Were the store's commit
            // order taken before that head, the frame's own prepare would wait on it in turn.
            using (ExecutionContext.SuppressFlow())
                other = Task.Run(() => store.AppendToStreamAsync("s", ExpectedVersion.Any, Events(1)));
            await Task.WhenAny(other, Task.Delay(TimeSpan.FromSeconds(1)));

            await unitOfWork.CommitAsync();
        }
        await other.WaitAsync(TimeSpan.FromSeconds(30));

        var all = await ((IAllStreamReader)store).ReadAllAsync(GlobalPosition.Start).ToListAsync();
        all.Select(e => e.StreamVersion).Should().Equal(1, 2);
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
