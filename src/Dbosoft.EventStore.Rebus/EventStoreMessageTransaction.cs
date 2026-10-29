using Rebus.Transport;

namespace Dbosoft.EventStore.Rebus;

/// <summary>
/// The event store's frame of a message, for the unit of work that owns the message's transaction.
/// </summary>
[PublicAPI]
public static class EventStoreMessageTransaction
{
    private const string UnitOfWorkKey = "dbosoft-eventstore-unit-of-work";

    /// <summary>
    /// Prepares the event store's frame of this message for the commit (see
    /// <see cref="IEventStoreUnitOfWork.PrepareAsync"/>); does nothing when the message has none. The owner of the
    /// message's transaction calls it as its very last step before committing — after its own writes are flushed,
    /// since from here to the commit every other committing writer of the store waits. Without it, the frame
    /// reports the commit as incomplete once the message is acknowledged.
    /// </summary>
    public static ValueTask PrepareEventStoreAsync(this ITransactionContext context, CancellationToken ct = default) =>
        context.Items.TryGetValue(UnitOfWorkKey, out var unitOfWork) && unitOfWork is IEventStoreUnitOfWork { IsActive: true } frame
            ? frame.PrepareAsync(ct)
            : ValueTask.CompletedTask;

    internal static void Remember(ITransactionContext context, IEventStoreUnitOfWork unitOfWork) =>
        context.Items[UnitOfWorkKey] = unitOfWork;
}
