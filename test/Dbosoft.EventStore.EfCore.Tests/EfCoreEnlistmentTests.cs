using System.Data.Common;
using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.EfCore.Tests;

/// <summary>
/// Proves the EF Core store enlists onto a caller-owned transaction: an append and a second writer's row on
/// the same connection commit or roll back as one unit. A side table stands in for a consumer's own state
/// store (SAPID's), sharing the borrowed connection and transaction exactly as it would in production.
/// </summary>
public sealed class EfCoreEnlistmentTests
{
    [Fact]
    public async Task An_enlisted_append_and_a_side_write_commit_atomically()
    {
        await using var harness = Harness.Create();
        var store = harness.Store;

        await using (var conn = await harness.OpenConnectionAsync())
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await using var unitOfWork = harness.UnitOfWorkFactory.Enlist(conn, tx);
            await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));
            await InsertSideRowAsync(conn, tx, "k", "v");
            await unitOfWork.CommitAsync();
        }

        (await store.GetStreamInfoAsync("s1"))!.Version.Should().Be(1);
        (await harness.ReadSideValueAsync("k")).Should().Be("v");
    }

    [Fact]
    public async Task Rolling_back_discards_both_the_append_and_the_side_write()
    {
        await using var harness = Harness.Create();
        var store = harness.Store;

        await using (var conn = await harness.OpenConnectionAsync())
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await using var unitOfWork = harness.UnitOfWorkFactory.Enlist(conn, tx);
            await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));
            await InsertSideRowAsync(conn, tx, "k", "v");
            await unitOfWork.RollbackAsync();
        }

        (await store.GetStreamInfoAsync("s1")).Should().BeNull("the rolled-back append left nothing");
        (await harness.ReadSideValueAsync("k")).Should().BeNull("the rolled-back side write left nothing");
    }

    [Fact]
    public async Task A_multi_stream_enlisted_append_commits_atomically_with_a_side_write()
    {
        await using var harness = Harness.Create();
        var store = harness.Store;

        await using (var conn = await harness.OpenConnectionAsync())
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await using var unitOfWork = harness.UnitOfWorkFactory.Enlist(conn, tx);
            var appends = new[]
            {
                new StreamAppend("a", ExpectedVersion.NoStream, Events(1)),
                new StreamAppend("b", ExpectedVersion.NoStream, Events(2))
            };
            await ((IMultiStreamAppend)store).AppendToStreamsAsync(appends);
            await InsertSideRowAsync(conn, tx, "k", "v");
            await unitOfWork.CommitAsync();
        }

        (await store.GetStreamInfoAsync("a"))!.Version.Should().Be(1);
        (await store.GetStreamInfoAsync("b"))!.Version.Should().Be(2);
        (await harness.ReadSideValueAsync("k")).Should().Be("v");
    }

    [Fact]
    public async Task The_change_signal_is_deferred_until_the_frame_commits()
    {
        await using var harness = Harness.Create();
        using var watch = harness.Notifier.Watch("test");
        var signal = watch.WaitAsync().AsTask();

        await using (var conn = await harness.OpenConnectionAsync())
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await using var unitOfWork = harness.UnitOfWorkFactory.Enlist(conn, tx);
            await harness.Store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));

            await Task.Delay(200);
            signal.IsCompleted.Should().BeFalse("an enlisted append must not signal before its frame commits");

            await unitOfWork.CommitAsync();
        }

        (await signal.WaitAsync(TimeSpan.FromSeconds(2))).Should().Be("s1");
    }

    [Fact]
    public async Task An_externally_committed_transaction_is_completed_and_signals_only_after_the_commit()
    {
        // The Rebus flow: the message's unit of work owns the commit; the frame does not commit, it only
        // fires its deferred signals afterwards via CompleteAsync. Here the transaction is committed directly,
        // standing in for that owner.
        await using var harness = Harness.Create();
        var store = harness.Store;
        using var watch = harness.Notifier.Watch("test");
        var signal = watch.WaitAsync().AsTask();

        await using (var conn = await harness.OpenConnectionAsync())
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await using var unitOfWork = harness.UnitOfWorkFactory.Enlist(conn, tx);
            await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));
            await InsertSideRowAsync(conn, tx, "k", "v");

            await tx.CommitAsync();
            signal.IsCompleted.Should().BeFalse("signals fire only when the frame is completed after the commit");

            await unitOfWork.CompleteAsync();
        }

        (await signal.WaitAsync(TimeSpan.FromSeconds(2))).Should().Be("s1");
        (await store.GetStreamInfoAsync("s1"))!.Version.Should().Be(1);
        (await harness.ReadSideValueAsync("k")).Should().Be("v");
    }

    [Fact]
    public async Task An_enlisted_append_at_a_wrong_expected_version_throws_and_the_stream_is_unchanged()
    {
        await using var harness = Harness.Create();
        var store = harness.Store;
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1)); // committed at version 1

        await using (var conn = await harness.OpenConnectionAsync())
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await using var unitOfWork = harness.UnitOfWorkFactory.Enlist(conn, tx);

            // A conflict surfaces to the caller rather than retrying in place; the frame then rolls back.
            WrongExpectedVersionException? conflict = null;
            try
            {
                await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));
            }
            catch (WrongExpectedVersionException ex)
            {
                conflict = ex;
            }

            conflict.Should().NotBeNull("an enlisted append at a wrong version must surface the conflict");
            await unitOfWork.RollbackAsync();
        }

        (await store.GetStreamInfoAsync("s1"))!.Version.Should().Be(1);
    }

    [Fact]
    public async Task After_the_frame_ends_it_is_no_longer_ambient()
    {
        await using var harness = Harness.Create();
        await using var conn = await harness.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await using var unitOfWork = harness.UnitOfWorkFactory.Enlist(conn, tx);

        AmbientEventStoreUnitOfWork.Current.Should().NotBeNull();
        await unitOfWork.CommitAsync();
        AmbientEventStoreUnitOfWork.Current.Should().BeNull("a committed frame stops being ambient");
    }

    [Fact]
    public async Task The_store_advertises_enlistment_and_registers_a_unit_of_work_factory()
    {
        await using var harness = Harness.Create();
        harness.Services.GetService<IEventStoreUnitOfWorkFactory>().Should().NotBeNull();
        harness.Store.Should().BeAssignableTo<IEnlistableStore>();
    }

    [Fact]
    public async Task Opening_a_second_unit_of_work_on_the_same_flow_throws()
    {
        await using var harness = Harness.Create();
        await using var conn = await harness.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await using var unitOfWork = harness.UnitOfWorkFactory.Enlist(conn, tx);

        // A second frame on the same async flow is rejected — event store frames do not nest.
        var threw = false;
        try
        {
            harness.UnitOfWorkFactory.Enlist(conn, tx);
        }
        catch (InvalidOperationException)
        {
            threw = true;
        }

        threw.Should().BeTrue("event store frames do not nest");
    }

    [Fact]
    public async Task An_enlisted_read_sees_the_frames_own_uncommitted_append()
    {
        await using var harness = Harness.Create();
        var store = harness.Store;

        await using var conn = await harness.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await using var unitOfWork = harness.UnitOfWorkFactory.Enlist(conn, tx);

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));

        // Reading back within the frame runs on the borrowed connection, so it sees the append that has been
        // saved into — but not yet committed on — this transaction (read-your-writes). Before the read paths
        // enlisted, this ran on a second connection and, on shared-cache SQLite, blocked on the frame's own
        // uncommitted es_streams write until the busy timeout. Bounded so a regression fails fast, not hangs.
        var info = await store.GetStreamInfoAsync("s1").WaitAsync(TimeSpan.FromSeconds(10));
        info!.Version.Should().Be(1);

        var read = new List<StoredEvent>();
        await foreach (var e in store.ReadStreamAsync("s1"))
            read.Add(e);
        read.Should().ContainSingle().Which.StreamVersion.Should().Be(1);
    }

    [Fact]
    public async Task An_enlisted_read_of_another_stream_does_not_self_lock()
    {
        await using var harness = Harness.Create();
        var store = harness.Store;

        await using var conn = await harness.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await using var unitOfWork = harness.UnitOfWorkFactory.Enlist(conn, tx);

        // The reported failure: one append holds an uncommitted es_streams write, then a read of a *different*
        // stream (a per-object correlation read) ran on a second connection and locked the whole es_streams
        // table on shared-cache SQLite. Enlisted, the read shares the one connection and cannot self-lock.
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));

        var other = await store.GetStreamInfoAsync("s2").WaitAsync(TimeSpan.FromSeconds(10));
        other.Should().BeNull("the second stream does not exist yet — and the read must not deadlock reaching that answer");
    }

    private static async Task InsertSideRowAsync(DbConnection connection, DbTransaction transaction, string id, string value)
    {
        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = "INSERT INTO side_state (id, value) VALUES ($id, $value)";
        AddParameter(cmd, "$id", id);
        AddParameter(cmd, "$value", value);
        await cmd.ExecuteNonQueryAsync();
    }

    private static void AddParameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    // A live store over a fresh SQLite shared-cache in-memory database, plus a side table that stands in for a
    // consumer's own state store enlisting on the same borrowed connection.
    private sealed class Harness : IAsyncDisposable
    {
        private readonly ServiceProvider _services;
        private readonly SqliteConnection _keepAlive;
        private readonly string _connectionString;

        private Harness(ServiceProvider services, SqliteConnection keepAlive, string connectionString)
        {
            _services = services;
            _keepAlive = keepAlive;
            _connectionString = connectionString;
        }

        public static Harness Create()
        {
            var connectionString = $"DataSource=file:memdb-{Guid.NewGuid():N}?mode=memory&cache=shared";
            var keepAlive = new SqliteConnection(connectionString);
            keepAlive.Open();

            var services = new ServiceCollection();
            services.AddEventStore(es => es
                .UseInProcessNotifications()
                .UseSqlite(connectionString)
                .AddDomain("test", _ => { }));
            var provider = services.BuildServiceProvider();

            using (var ctx = provider.GetRequiredService<IDbContextFactory<EventStoreDbContext>>().CreateDbContext())
                ctx.Database.EnsureCreated();

            using var cmd = keepAlive.CreateCommand();
            cmd.CommandText = "CREATE TABLE side_state (id TEXT PRIMARY KEY, value TEXT)";
            cmd.ExecuteNonQuery();

            return new Harness(provider, keepAlive, connectionString);
        }

        public IServiceProvider Services => _services;
        public IEventStore Store => _services.GetRequiredKeyedService<IEventStore>("test");
        public IChangeNotifier Notifier => _services.GetRequiredService<IChangeNotifier>();
        public IEventStoreUnitOfWorkFactory UnitOfWorkFactory => _services.GetRequiredService<IEventStoreUnitOfWorkFactory>();

        public async Task<SqliteConnection> OpenConnectionAsync()
        {
            var connection = new SqliteConnection(_connectionString);
            await connection.OpenAsync();
            return connection;
        }

        public async Task<string?> ReadSideValueAsync(string id)
        {
            await using var cmd = _keepAlive.CreateCommand();
            cmd.CommandText = "SELECT value FROM side_state WHERE id = $id";
            AddParameter(cmd, "$id", id);
            return await cmd.ExecuteScalarAsync() as string;
        }

        public async ValueTask DisposeAsync()
        {
            await _services.DisposeAsync();
            await _keepAlive.DisposeAsync();
        }
    }
}
