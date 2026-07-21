using System.Data.Common;

namespace Dbosoft.EventStore;

/// <summary>
/// Opens an <see cref="IEventStoreUnitOfWork"/> over a caller-owned database transaction, so the event
/// store's appends enlist onto it. Registered in DI only by a provider that can enlist (the EF Core
/// provider); its presence in the container <em>is</em> the "this deployment can enlist" capability. A
/// provider that cannot enlist (in-memory, a partitioned key/value store) registers no factory, and a
/// caller that resolves none knows enlistment is unavailable.
/// </summary>
[PublicAPI]
public interface IEventStoreUnitOfWorkFactory
{
    /// <summary>
    /// Makes the given open <paramref name="connection"/> and its <paramref name="transaction"/> the ambient
    /// unit of work (<see cref="AmbientEventStoreUnitOfWork.Current"/>) and returns the frame. The connection
    /// and transaction stay owned by the caller — the frame commits or rolls the transaction back on request
    /// but never opens, closes, or disposes the connection. Dispose the returned frame (an <c>await using</c>)
    /// to clear the ambient slot.
    /// </summary>
    /// <exception cref="InvalidOperationException">A unit of work is already ambient on this async flow.</exception>
    IEventStoreUnitOfWork Enlist(DbConnection connection, DbTransaction transaction);
}
