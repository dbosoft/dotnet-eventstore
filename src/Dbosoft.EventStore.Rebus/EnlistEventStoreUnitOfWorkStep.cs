using System.Data.Common;
using Rebus.Pipeline;
using Rebus.Transport;

namespace Dbosoft.EventStore.Rebus;

/// <summary>
/// Incoming pipeline step that opens an event store unit-of-work frame for the message before it is
/// dispatched, so an append in the handler enlists onto the message's transaction. The message's own unit of
/// work owns the commit — committing that one shared transaction makes both the app's staged state and the
/// enlisted events durable. So the frame does not commit or roll the transaction back; the owner prepares it
/// right before committing (see <see cref="EventStoreMessageTransaction.PrepareEventStoreAsync"/>), and the frame
/// fires its deferred change signals once the transaction has committed and releases its context when the
/// context is disposed (which also discards the signals untouched if the message failed).
/// </summary>
internal sealed class EnlistEventStoreUnitOfWorkStep(
    IEventStoreUnitOfWorkFactory unitOfWorkFactory,
    Func<ITransactionContext, (DbConnection Connection, DbTransaction Transaction)?> resolveTransaction)
    : IIncomingStep
{
    public async Task Process(IncomingStepContext context, Func<Task> next)
    {
        var transactionContext = context.Load<ITransactionContext>();

        if (resolveTransaction(transactionContext) is not { } resolved)
        {
            // No per-message transaction to join — run the handler with no frame.
            await next().ConfigureAwait(false);
            return;
        }

        var unitOfWork = unitOfWorkFactory.Enlist(resolved.Connection, resolved.Transaction);
        EventStoreMessageTransaction.Remember(transactionContext, unitOfWork);

        // OnAck runs after the message's transaction has committed (the app's own OnCommit did the commit) —
        // fire the deferred signals then. A failed message never acks, so its signals are simply dropped when
        // the frame is disposed.
        transactionContext.OnAck(_ => unitOfWork.CompleteAsync().AsTask());
        transactionContext.OnDisposed(_ => FireAndForgetDispose(unitOfWork));

        await next().ConfigureAwait(false);
    }

    // OnDisposed is synchronous and runs after the message has committed or failed, so releasing the frame's
    // context is best-effort cleanup — discard the disposal task rather than block the callback.
    private static void FireAndForgetDispose(IEventStoreUnitOfWork unitOfWork) => _ = unitOfWork.DisposeAsync();
}
