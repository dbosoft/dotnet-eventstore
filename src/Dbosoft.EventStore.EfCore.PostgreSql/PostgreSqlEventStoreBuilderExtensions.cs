using Dbosoft.EventStore.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure;

namespace Dbosoft.EventStore.EfCore.PostgreSql;

/// <summary>Registers the EF Core provider on PostgreSQL with the model tweaks PostgreSQL needs.</summary>
[PublicAPI]
public static class PostgreSqlEventStoreBuilderExtensions
{
    // The assembly this provider's migrations live in, so a factory-created context finds them (Migrate,
    // scripts) and `dotnet ef migrations add` targets the right project.
    private static readonly string? MigrationsAssembly =
        typeof(PostgreSqlEventStoreBuilderExtensions).Assembly.GetName().Name;

    /// <summary>
    /// Backs the event store with EF Core on PostgreSQL. Only the version concurrency token is applied —
    /// PostgreSQL orders its native <c>timestamptz</c> directly, so no timestamp converter is needed —
    /// and the caller supplies just the connection.
    /// </summary>
    /// <param name="builder">The event store builder being configured.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <param name="configureNpgsql">Optional extra Npgsql options.</param>
    /// <param name="migrationsHistoryTable">
    /// The migrations-history table the store records its migrations in. Defaults to
    /// <see cref="EventStoreDbContext.DefaultMigrationsHistoryTable"/>, kept distinct from EF's
    /// <c>__EFMigrationsHistory</c> so a database shared with a consumer's own migrations never mixes histories.
    /// </param>
    /// <param name="migrationsHistorySchema">Optional schema for the migrations-history table.</param>
    public static EventStoreBuilder UseNpgsql(
        this EventStoreBuilder builder,
        string connectionString,
        Action<NpgsqlDbContextOptionsBuilder>? configureNpgsql = null,
        string migrationsHistoryTable = EventStoreDbContext.DefaultMigrationsHistoryTable,
        string? migrationsHistorySchema = null) =>
        builder.UseEfCore(
            options => options.UseNpgsql(connectionString,
                npgsql => ConfigureNpgsql(npgsql, configureNpgsql, migrationsHistoryTable, migrationsHistorySchema)),
            ConfigureModel,
            (options, connection) => options.UseNpgsql(connection, npgsql =>
            {
                ConfigureNpgsql(npgsql, configureNpgsql, migrationsHistoryTable, migrationsHistorySchema);
                // A retrying strategy refuses a user-initiated transaction; the frame supplies the retry
                // boundary itself (a failed commit rolls the whole frame back and the caller retries).
                npgsql.ExecutionStrategy(dependencies => new NonRetryingExecutionStrategy(dependencies));
            }));

    // The PostgreSQL model configuration, shared with the design-time factory so the migrations snapshot
    // always matches the runtime model.
    internal static void ConfigureModel(ModelBuilder model) =>
        model.UseVersionConcurrencyToken();

    private static void ConfigureNpgsql(
        NpgsqlDbContextOptionsBuilder npgsql,
        Action<NpgsqlDbContextOptionsBuilder>? configure,
        string migrationsHistoryTable,
        string? migrationsHistorySchema)
    {
        configure?.Invoke(npgsql);
        npgsql.MigrationsAssembly(MigrationsAssembly);
        npgsql.MigrationsHistoryTable(migrationsHistoryTable, migrationsHistorySchema);
    }
}
