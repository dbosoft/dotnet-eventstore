using System.Data.Common;

namespace Dbosoft.EventStore.EfCore;

/// <summary>
/// Opens EF Core unit-of-work frames over a caller-owned transaction. Registered by
/// <see cref="EfCoreEventStoreBuilderExtensions.UseEfCore"/> only when an enlistment dialect applicator was
/// supplied — so a store configured without one exposes no factory, and enlistment reads as unavailable.
/// </summary>
internal sealed class EventStoreUnitOfWorkFactory(EnlistedContextFactory contextFactory)
    : IEventStoreUnitOfWorkFactory
{
    /// <inheritdoc />
    public IEventStoreUnitOfWork Enlist(DbConnection connection, DbTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        if (AmbientEventStoreUnitOfWork.Current is not null)
            throw new InvalidOperationException(
                "A unit of work is already active on this async flow; event store frames do not nest.");

        var unitOfWork = new EfCoreEventStoreUnitOfWork(contextFactory, connection, transaction);
        AmbientEventStoreUnitOfWork.SetCurrent(unitOfWork);
        return unitOfWork;
    }
}
