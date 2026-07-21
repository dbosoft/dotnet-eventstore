using Dbosoft.EventStore.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace Dbosoft.EventStore.EfCore.SqlServer;

/// <summary>Registers the EF Core provider on SQL Server with the model tweaks SQL Server needs.</summary>
[PublicAPI]
public static class SqlServerEventStoreBuilderExtensions
{
    /// <summary>
    /// Backs the event store with EF Core on SQL Server. Only the version concurrency token is applied —
    /// SQL Server orders its native <c>datetimeoffset</c> directly, so no timestamp converter is needed —
    /// and the caller supplies just the connection.
    /// </summary>
    public static EventStoreBuilder UseSqlServer(
        this EventStoreBuilder builder,
        string connectionString,
        Action<SqlServerDbContextOptionsBuilder>? configureSqlServer = null) =>
        builder.UseEfCore(
            options => options.UseSqlServer(connectionString, configureSqlServer),
            model => model.UseVersionConcurrencyToken(),
            (options, connection) => options.UseSqlServer(connection, sqlServer =>
            {
                configureSqlServer?.Invoke(sqlServer);
                // A retrying strategy refuses a user-initiated transaction; the frame supplies the retry
                // boundary itself (a failed commit rolls the whole frame back and the caller retries).
                sqlServer.ExecutionStrategy(dependencies => new NonRetryingExecutionStrategy(dependencies));
            }));
}
