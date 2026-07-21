using Dbosoft.EventStore.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace Dbosoft.EventStore.EfCore.Sqlite;

/// <summary>Registers the EF Core provider on SQLite with the model tweaks SQLite needs.</summary>
[PublicAPI]
public static class SqliteEventStoreBuilderExtensions
{
    /// <summary>
    /// Backs the event store with EF Core on SQLite. The SQLite-specific model configuration is applied
    /// here — timestamps as sortable ticks (SQLite cannot order native DateTimeOffset) and the version
    /// concurrency token — so the caller only supplies the connection.
    /// </summary>
    public static EventStoreBuilder UseSqlite(
        this EventStoreBuilder builder,
        string connectionString,
        Action<SqliteDbContextOptionsBuilder>? configureSqlite = null) =>
        builder.UseEfCore(
            options => options.UseSqlite(connectionString, configureSqlite),
            model => model.StoreTimestampsAsTicks().UseVersionConcurrencyToken(),
            (options, connection) => options.UseSqlite(connection, sqlite =>
            {
                configureSqlite?.Invoke(sqlite);
                sqlite.ExecutionStrategy(dependencies => new NonRetryingExecutionStrategy(dependencies));
            }));
}
