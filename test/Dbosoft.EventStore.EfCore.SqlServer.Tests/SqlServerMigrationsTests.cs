using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.EfCore.SqlServer.Tests;

/// <summary>
/// Applies the shipped SQL Server migrations to a fresh database, as a consumer would: the schema is usable,
/// records itself in the isolated default history table, and stays in sync with the model.
/// </summary>
[UsedImplicitly]
[Collection(SqlServerCollection.Name)]
public sealed class SqlServerMigrationsTests(SqlServerContainerFixture container)
{
    [Fact]
    public async Task Migrate_builds_a_usable_schema_recorded_in_the_isolated_history_table()
    {
        var connectionString = await CreateDatabaseAsync(container.ConnectionString);

        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseSqlServer(connectionString).AddDomain("test", _ => { }));
        await using var provider = services.BuildServiceProvider();

        await using (var ctx = await provider.GetRequiredService<IDbContextFactory<EventStoreDbContext>>().CreateDbContextAsync())
        {
            await ctx.Database.MigrateAsync();
            ctx.Database.HasPendingModelChanges().Should().BeFalse();
            (await ctx.Database.GetAppliedMigrationsAsync()).Select(m => m[(m.IndexOf('_') + 1)..])
                .Should().Equal("InitialCreate", "CommitSequence");
        }

        (await TableExistsAsync(connectionString, "es_migrations_history")).Should().BeTrue();
        (await TableExistsAsync(connectionString, "__EFMigrationsHistory")).Should().BeFalse();

        var store = provider.GetRequiredService<IEventStoreProvider>().GetStore("test");
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(2));
        (await store.GetStreamInfoAsync("s1"))!.Version.Should().Be(2);
    }

    private static async Task<string> CreateDatabaseAsync(string adminConnectionString)
    {
        var database = $"es_migrate_{Guid.NewGuid():N}";
        await using (var connection = new SqlConnection(adminConnectionString))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE [{database}]";
            await command.ExecuteNonQueryAsync();
        }

        return new SqlConnectionStringBuilder(adminConnectionString) { InitialCatalog = database }.ConnectionString;
    }

    private static async Task<bool> TableExistsAsync(string connectionString, string name)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @name";
        var parameter = command.CreateParameter();
        parameter.ParameterName = "@name";
        parameter.Value = name;
        command.Parameters.Add(parameter);
        return Convert.ToInt32(await command.ExecuteScalarAsync()) > 0;
    }
}
