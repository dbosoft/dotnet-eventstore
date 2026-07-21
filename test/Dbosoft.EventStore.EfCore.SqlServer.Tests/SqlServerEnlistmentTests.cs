using System.Data.Common;
using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.EfCore.SqlServer.Tests;

/// <summary>
/// Proves enlistment works on SQL Server even with connection resiliency (<c>EnableRetryOnFailure</c>)
/// switched on. A retrying execution strategy refuses a user-initiated transaction, so the enlisted context
/// must force a non-retrying one — this test would throw at <c>Enlist</c>/commit if that override were missing.
/// </summary>
[UsedImplicitly]
[Collection(SqlServerCollection.Name)]
public sealed class SqlServerEnlistmentTests(SqlServerContainerFixture container)
{
    [Fact]
    public async Task Enlistment_commits_atomically_even_with_retry_on_failure_enabled()
    {
        var connectionString = await CreateDatabaseAsync(container.ConnectionString);

        var services = new ServiceCollection();
        services.AddEventStore(es => es
            .UseSqlServer(connectionString, o => o.EnableRetryOnFailure())
            .AddDomain("test", _ => { }));
        await using var provider = services.BuildServiceProvider();

        await using (var ctx = await provider.GetRequiredService<IDbContextFactory<EventStoreDbContext>>().CreateDbContextAsync())
            await ctx.Database.EnsureCreatedAsync();
        await CreateSideTableAsync(connectionString);

        var store = provider.GetRequiredService<IEventStoreProvider>().GetStore("test");
        var unitOfWorkFactory = provider.GetRequiredService<IEventStoreUnitOfWorkFactory>();

        await using (var conn = new SqlConnection(connectionString))
        {
            await conn.OpenAsync();
            await using var tx = await conn.BeginTransactionAsync();
            await using var unitOfWork = unitOfWorkFactory.Enlist(conn, tx);
            await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));
            await InsertSideRowAsync(conn, tx, "k", "v");
            await unitOfWork.CommitAsync();
        }

        (await store.GetStreamInfoAsync("s1"))!.Version.Should().Be(1);
        (await ReadSideValueAsync(connectionString, "k")).Should().Be("v");
    }

    private static async Task<string> CreateDatabaseAsync(string adminConnectionString)
    {
        var database = $"es_enlist_{Guid.NewGuid():N}";
        await using (var connection = new SqlConnection(adminConnectionString))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE [{database}]";
            await command.ExecuteNonQueryAsync();
        }

        return new SqlConnectionStringBuilder(adminConnectionString) { InitialCatalog = database }.ConnectionString;
    }

    private static async Task CreateSideTableAsync(string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE side_state (id NVARCHAR(100) PRIMARY KEY, value NVARCHAR(100))";
        await command.ExecuteNonQueryAsync();
    }

    private static async Task InsertSideRowAsync(DbConnection connection, DbTransaction transaction, string id, string value)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO side_state (id, value) VALUES (@id, @value)";
        AddParameter(command, "@id", id);
        AddParameter(command, "@value", value);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<string?> ReadSideValueAsync(string connectionString, string id)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT value FROM side_state WHERE id = @id";
        AddParameter(command, "@id", id);
        return await command.ExecuteScalarAsync() as string;
    }

    private static void AddParameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
