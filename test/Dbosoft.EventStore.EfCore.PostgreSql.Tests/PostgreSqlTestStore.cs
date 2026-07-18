using Dbosoft.EventStore.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Dbosoft.EventStore.EfCore.PostgreSql.Tests;

/// <summary>
/// A provider over a freshly created database on the shared PostgreSQL container — one per test, wired
/// exactly as an app would (<c>AddEventStore(es =&gt; es.UseNpgsql(...))</c>) and schema-created.
/// </summary>
internal sealed class PostgreSqlTestStore : IAsyncDisposable
{
    private ServiceProvider _services = null!;

    public IEventStoreProvider Provider { get; private set; } = null!;

    public static async Task<PostgreSqlTestStore> CreateAsync(string adminConnectionString)
    {
        var connectionString = await CreateDatabaseAsync(adminConnectionString);

        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseNpgsql(connectionString).AddDomain("test", _ => { }));

        var store = new PostgreSqlTestStore { _services = services.BuildServiceProvider() };
        await using (var ctx = await store._services.GetRequiredService<IDbContextFactory<EventStoreDbContext>>().CreateDbContextAsync())
            await ctx.Database.EnsureCreatedAsync();

        store.Provider = store._services.GetRequiredService<IEventStoreProvider>();
        return store;
    }

    private static async Task<string> CreateDatabaseAsync(string adminConnectionString)
    {
        var database = $"es_test_{Guid.NewGuid():N}";
        await using (var connection = new NpgsqlConnection(adminConnectionString))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE \"{database}\"";
            await command.ExecuteNonQueryAsync();
        }

        return new NpgsqlConnectionStringBuilder(adminConnectionString) { Database = database }.ConnectionString;
    }

    public async ValueTask DisposeAsync() => await _services.DisposeAsync();
}
