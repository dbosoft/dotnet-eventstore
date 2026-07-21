using System.Data.Common;
using Microsoft.EntityFrameworkCore;

namespace Dbosoft.EventStore.EfCore;

/// <summary>
/// Builds an <see cref="EventStoreDbContext"/> bound to a caller-owned, already-open <see cref="DbConnection"/>
/// so the store can enlist onto the caller's transaction. Unlike the autonomous
/// <see cref="EventStoreDbContextFactory"/> — which holds prebuilt options over its own connection string —
/// this re-applies the registered database dialect to a supplied connection, reusing the same model hook.
/// It exists only when the registration provided an enlistment dialect applicator (every <c>UseXxx</c>
/// wrapper does; a bare <see cref="EfCoreEventStoreBuilderExtensions.UseEfCore"/> caller opts in explicitly).
/// </summary>
internal sealed class EnlistedContextFactory(
    Action<DbContextOptionsBuilder, DbConnection> applyDialect,
    Action<ModelBuilder>? configureModel)
{
    /// <summary>
    /// Creates a context on <paramref name="connection"/>. The connection is treated as externally owned —
    /// EF Core will not open, close, or dispose it — and the dialect applicator forces a non-retrying
    /// execution strategy, because a retrying strategy refuses user-initiated transactions.
    /// </summary>
    public EventStoreDbContext Create(DbConnection connection)
    {
        var optionsBuilder = new DbContextOptionsBuilder<EventStoreDbContext>();
        applyDialect(optionsBuilder, connection);
        return new EventStoreDbContext(optionsBuilder.Options, configureModel);
    }
}
