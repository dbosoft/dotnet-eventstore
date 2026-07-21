using Dbosoft.EventStore.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace Dbosoft.EventStore.EfCore.Sqlite;

/// <summary>Registers the EF Core provider on SQLite with the model tweaks SQLite needs.</summary>
[PublicAPI]
public static class SqliteEventStoreBuilderExtensions
{
    // The assembly this provider's migrations live in, so a factory-created context finds them (Migrate,
    // scripts) and `dotnet ef migrations add` targets the right project.
    private static readonly string? MigrationsAssembly =
        typeof(SqliteEventStoreBuilderExtensions).Assembly.GetName().Name;

    /// <summary>
    /// Backs the event store with EF Core on SQLite. The SQLite-specific model configuration is applied
    /// here — timestamps as sortable ticks (SQLite cannot order native DateTimeOffset) and the version
    /// concurrency token — so the caller only supplies the connection.
    /// </summary>
    /// <param name="builder">The event store builder being configured.</param>
    /// <param name="connectionString">The SQLite connection string.</param>
    /// <param name="configureSqlite">Optional extra SQLite options.</param>
    /// <param name="migrationsHistoryTable">
    /// The migrations-history table the store records its migrations in. Defaults to
    /// <see cref="EventStoreDbContext.DefaultMigrationsHistoryTable"/>, kept distinct from EF's
    /// <c>__EFMigrationsHistory</c> so a database shared with a consumer's own migrations never mixes histories.
    /// </param>
    public static EventStoreBuilder UseSqlite(
        this EventStoreBuilder builder,
        string connectionString,
        Action<SqliteDbContextOptionsBuilder>? configureSqlite = null,
        string migrationsHistoryTable = EventStoreDbContext.DefaultMigrationsHistoryTable) =>
        builder.UseEfCore(
            options => options.UseSqlite(connectionString,
                sqlite => ConfigureSqlite(sqlite, configureSqlite, migrationsHistoryTable)),
            ConfigureModel,
            (options, connection) => options.UseSqlite(connection, sqlite =>
            {
                ConfigureSqlite(sqlite, configureSqlite, migrationsHistoryTable);
                sqlite.ExecutionStrategy(dependencies => new NonRetryingExecutionStrategy(dependencies));
            }));

    // The SQLite model configuration, shared with the design-time factory so the migrations snapshot always
    // matches the runtime model.
    internal static void ConfigureModel(ModelBuilder model) =>
        model.StoreTimestampsAsTicks().UseVersionConcurrencyToken();

    private static void ConfigureSqlite(
        SqliteDbContextOptionsBuilder sqlite,
        Action<SqliteDbContextOptionsBuilder>? configure,
        string migrationsHistoryTable)
    {
        configure?.Invoke(sqlite);
        sqlite.MigrationsAssembly(MigrationsAssembly);
        sqlite.MigrationsHistoryTable(migrationsHistoryTable);
    }
}
