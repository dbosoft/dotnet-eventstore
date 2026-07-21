using System.Data.Common;
using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore;
using Dbosoft.EventStore.EfCore.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Pipeline;
using Rebus.Transport;
using Rebus.Transport.InMem;
using Xunit;

namespace Dbosoft.EventStore.Rebus.Tests;

/// <summary>
/// End-to-end proof that <c>EnableEventStoreUnitOfWork</c> works over a real Rebus bus: a handler appends
/// events and writes its own state on the message's shared connection + transaction; the message's unit of
/// work commits both together, and the store's change signal fires only after that commit.
/// </summary>
public sealed class RebusEventStoreUnitOfWorkTests
{
    private sealed record DoWork(string StreamId);

    [Fact]
    public async Task A_handler_append_and_state_write_commit_atomically_with_the_message()
    {
        var connectionString = $"DataSource=file:memdb-{Guid.NewGuid():N}?mode=memory&cache=shared";
        await using var keepAlive = new SqliteConnection(connectionString);
        await keepAlive.OpenAsync();

        var services = new ServiceCollection();
        services.AddEventStore(es => es
            .UseInProcessNotifications()
            .UseSqlite(connectionString)
            .AddDomain("test", _ => { }));
        await using var provider = services.BuildServiceProvider();

        await using (var ctx = await provider.GetRequiredService<IDbContextFactory<EventStoreDbContext>>().CreateDbContextAsync())
            await ctx.Database.EnsureCreatedAsync();
        await using (var cmd = keepAlive.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE side_state (id TEXT PRIMARY KEY, value TEXT)";
            await cmd.ExecuteNonQueryAsync();
        }

        var store = provider.GetRequiredService<IEventStoreProvider>().GetStore("test");
        var unitOfWorkFactory = provider.GetRequiredService<IEventStoreUnitOfWorkFactory>();
        var notifier = provider.GetRequiredService<IChangeNotifier>();
        using var watch = notifier.Watch("test");
        var signal = watch.WaitAsync().AsTask();

        using var activator = new BuiltinHandlerActivator();
        activator.Handle<DoWork>(async message =>
        {
            var transactionContext = MessageContext.Current!.TransactionContext;
            var connection = (SqliteConnection)transactionContext.Items["connection"];
            var transaction = (DbTransaction)transactionContext.Items["transaction"];

            // The store's append enlists onto the shared transaction; the handler's own state write goes on
            // the same connection + transaction.
            await store.AppendToStreamAsync(message.StreamId, ExpectedVersion.NoStream,
                [new EventData(Guid.NewGuid(), "Worked", new byte[] { 1 })]);
            await using DbCommand cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = "INSERT INTO side_state (id, value) VALUES ('k', 'v')";
            await cmd.ExecuteNonQueryAsync();
        });

        Configure.With(activator)
            .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "uow"))
            .Options(o => o.EnableEventStoreUnitOfWork(unitOfWorkFactory, OpenSharedTransaction(connectionString)))
            .Start();

        await activator.Bus.SendLocal(new DoWork("s1"));

        // The signal fires from the frame's post-commit drain, so awaiting it means the message committed.
        (await signal.WaitAsync(TimeSpan.FromSeconds(5))).Should().Be("s1");
        (await store.GetStreamInfoAsync("s1"))!.Version.Should().Be(1);
        (await ReadSideValueAsync(keepAlive, "k")).Should().Be("v");
    }

    // Stands in for the app's own per-message transaction setup: opens one shared connection + transaction
    // for the message, stashes it for the handler, commits it when the message commits, and disposes it after.
    private static Func<ITransactionContext, (DbConnection Connection, DbTransaction Transaction)?> OpenSharedTransaction(
        string connectionString) =>
        transactionContext =>
        {
            if (transactionContext.Items.TryGetValue("connection", out var existing))
                return ((SqliteConnection)existing, (DbTransaction)transactionContext.Items["transaction"]);

            var connection = new SqliteConnection(connectionString);
            connection.Open();
            var transaction = connection.BeginTransaction();
            transactionContext.Items["connection"] = connection;
            transactionContext.Items["transaction"] = transaction;
            transactionContext.OnCommit(_ => transaction.CommitAsync());
            transactionContext.OnDisposed(_ =>
            {
                transaction.Dispose();
                connection.Dispose();
            });
            return (connection, transaction);
        };

    private static async Task<string?> ReadSideValueAsync(SqliteConnection connection, string id)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT value FROM side_state WHERE id = $id";
        var parameter = cmd.CreateParameter();
        parameter.ParameterName = "$id";
        parameter.Value = id;
        cmd.Parameters.Add(parameter);
        return await cmd.ExecuteScalarAsync() as string;
    }
}
