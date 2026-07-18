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

    public static async Task<SqlServerTestStore> CreateAsync(string adminConnectionString)
    {
        var connectionString = await CreateDatabaseAsync(adminConnectionString);

        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseSqlServer(connectionString).AddDomain("test", _ => { }));

        var store = new SqlServerTestStore { _services = services.BuildServiceProvider() };
        await using (var ctx = await store._services.GetRequiredService<IDbContextFactory<EventStoreDbContext>>().CreateDbContextAsync())
            await ctx.Database.EnsureCreatedAsync();

        store.Provider = store._services.GetRequiredService<IEventStoreProvider>();
        return store;
    }

    private static async Task<string> CreateDatabaseAsync(string adminConnectionString)
    {
        var database = $"es_test_{Guid.NewGuid():N}";
        await using (var connection = new SqlConnection(adminConnectionString))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE [{database}]";
            await command.ExecuteNonQueryAsync();
        }

        return new SqlConnectionStringBuilder(adminConnectionString) { InitialCatalog = database }.ConnectionString;
    }

    public async ValueTask DisposeAsync() => await _services.DisposeAsync();
}
