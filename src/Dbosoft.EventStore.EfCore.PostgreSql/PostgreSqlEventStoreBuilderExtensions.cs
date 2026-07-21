using Dbosoft.EventStore.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure;

namespace Dbosoft.EventStore.EfCore.PostgreSql;

/// <summary>Registers the EF Core provider on PostgreSQL with the model tweaks PostgreSQL needs.</summary>
[PublicAPI]
public static class PostgreSqlEventStoreBuilderExtensions
{
    /// <summary>
    /// Backs the event store with EF Core on PostgreSQL. Only the version concurrency token is applied —
    /// PostgreSQL orders its native <c>timestamptz</c> directly, so no timestamp converter is needed —
    /// and the caller supplies just the connection.
    /// </summary>
    public static EventStoreBuilder UseNpgsql(
        this EventStoreBuilder builder,
        string connectionString,
        Action<NpgsqlDbContextOptionsBuilder>? configureNpgsql = null) =>
        builder.UseEfCore(
            options => options.UseNpgsql(connectionString, configureNpgsql),
            model => model.UseVersionConcurrencyToken(),
            (options, connection) => options.UseNpgsql(connection, npgsql =>
            {
                configureNpgsql?.Invoke(npgsql);
                // A retrying strategy refuses a user-initiated transaction; the frame supplies the retry
                // boundary itself (a failed commit rolls the whole frame back and the caller retries).
                npgsql.ExecutionStrategy(dependencies => new NonRetryingExecutionStrategy(dependencies));
            }));
}
