using Dbosoft.EventStore.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace Dbosoft.EventStore.EfCore.SqlServer;

/// <summary>Registers the EF Core provider on SQL Server with the model tweaks SQL Server needs.</summary>
[PublicAPI]
public static class SqlServerEventStoreBuilderExtensions
{
    // The assembly this provider's migrations live in, so a factory-created context finds them (Migrate,
    // scripts) and `dotnet ef migrations add` targets the right project.
    private static readonly string? MigrationsAssembly =
        typeof(SqlServerEventStoreBuilderExtensions).Assembly.GetName().Name;

    /// <summary>
    /// Backs the event store with EF Core on SQL Server. Only the version concurrency token is applied —
    /// SQL Server orders its native <c>datetimeoffset</c> directly, so no timestamp converter is needed —
    /// and the caller supplies just the connection.
    /// </summary>
    /// <param name="builder">The event store builder being configured.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <param name="configureSqlServer">Optional extra SQL Server options.</param>
    /// <param name="migrationsHistoryTable">
    /// The migrations-history table the store records its migrations in. Defaults to
    /// <see cref="EventStoreDbContext.DefaultMigrationsHistoryTable"/>, kept distinct from EF's
    /// <c>__EFMigrationsHistory</c> so a database shared with a consumer's own migrations never mixes histories.
    /// </param>
    /// <param name="migrationsHistorySchema">Optional schema for the migrations-history table.</param>
    public static EventStoreBuilder UseSqlServer(
        this EventStoreBuilder builder,
        string connectionString,
        Action<SqlServerDbContextOptionsBuilder>? configureSqlServer = null,
        string migrationsHistoryTable = EventStoreDbContext.DefaultMigrationsHistoryTable,
        string? migrationsHistorySchema = null) =>
        builder.UseEfCore(
            options => options.UseSqlServer(connectionString,
                sqlServer => ConfigureSqlServer(sqlServer, configureSqlServer, migrationsHistoryTable, migrationsHistorySchema)),
            ConfigureModel,
            (options, connection) => options.UseSqlServer(connection, sqlServer =>
            {
                ConfigureSqlServer(sqlServer, configureSqlServer, migrationsHistoryTable, migrationsHistorySchema);
                // A retrying strategy refuses a user-initiated transaction; the frame supplies the retry
                // boundary itself (a failed commit rolls the whole frame back and the caller retries).
                sqlServer.ExecutionStrategy(dependencies => new NonRetryingExecutionStrategy(dependencies));
            }));

    // The SQL Server model configuration, shared with the design-time factory so the migrations snapshot
    // always matches the runtime model.
    internal static void ConfigureModel(ModelBuilder model) =>
        model.UseVersionConcurrencyToken();

    private static void ConfigureSqlServer(
        SqlServerDbContextOptionsBuilder sqlServer,
        Action<SqlServerDbContextOptionsBuilder>? configure,
        string migrationsHistoryTable,
        string? migrationsHistorySchema)
    {
        configure?.Invoke(sqlServer);
        sqlServer.MigrationsAssembly(MigrationsAssembly);
        sqlServer.MigrationsHistoryTable(migrationsHistoryTable, migrationsHistorySchema);
    }
}
