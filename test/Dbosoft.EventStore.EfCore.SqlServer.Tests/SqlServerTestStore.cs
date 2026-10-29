using Dbosoft.EventStore.DependencyInjection;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Dbosoft.EventStore.EfCore.SqlServer.Tests;

/// <summary>
/// A provider over a freshly created database on the shared SQL Server container — one per test, wired
/// exactly as an app would (<c>AddEventStore(es =&gt; es.UseSqlServer(...))</c>) and schema-created.
/// </summary>
internal sealed class SqlServerTestStore : IAsyncDisposable
{
    private ServiceProvider _services = null!;

    public IEventStoreProvider Provider { get; private set; } = null!;

    public string ConnectionString { get; private set; } = "";

    public IEventStoreUnitOfWorkFactory UnitOfWorks => _services.GetRequiredService<IEventStoreUnitOfWorkFactory>();

    /// <param name="adminConnectionString">A connection to the server, allowed to create databases.</param>
    /// <param name="readCommittedSnapshot">Readers see the last committed rows instead of waiting for open writes, as on Azure SQL by default.</param>
    public static async Task<SqlServerTestStore> CreateAsync(string adminConnectionString, bool readCommittedSnapshot = false)
    {
        var connectionString = await CreateDatabaseAsync(adminConnectionString, readCommittedSnapshot);

        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseSqlServer(connectionString).AddDomain("test", _ => { }));

        var store = new SqlServerTestStore { _services = services.BuildServiceProvider(), ConnectionString = connectionString };
        await using (var ctx = await store._services.GetRequiredService<IDbContextFactory<EventStoreDbContext>>().CreateDbContextAsync())
            await ctx.Database.EnsureCreatedAsync();

        store.Provider = store._services.GetRequiredService<IEventStoreProvider>();
        return store;
    }

    private static async Task<string> CreateDatabaseAsync(string adminConnectionString, bool readCommittedSnapshot)
    {
        var database = $"es_test_{Guid.NewGuid():N}";
        await using (var connection = new SqlConnection(adminConnectionString))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE [{database}]";
            await command.ExecuteNonQueryAsync();
            if (readCommittedSnapshot)
            {
                command.CommandText = $"ALTER DATABASE [{database}] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE";
                await command.ExecuteNonQueryAsync();
            }
        }

        return new SqlConnectionStringBuilder(adminConnectionString) { InitialCatalog = database }.ConnectionString;
    }

    public async ValueTask DisposeAsync() => await _services.DisposeAsync();
}
