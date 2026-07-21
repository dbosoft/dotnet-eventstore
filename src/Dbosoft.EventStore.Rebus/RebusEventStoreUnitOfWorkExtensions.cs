using System.Data.Common;
using Rebus.Config;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.Transport;

namespace Dbosoft.EventStore.Rebus;

/// <summary>
/// Bridges the event store's transactional enlistment onto Rebus's per-message transaction, so that within a
/// handler an append joins the message's database transaction and commits with it. Optional and opt-in — the
/// store stays bus-agnostic; this only wires the ambient unit of work into the incoming pipeline.
/// </summary>
[PublicAPI]
public static class RebusEventStoreUnitOfWorkExtensions
{
    /// <summary>
    /// Enlists the event store onto the message's transaction. For each incoming message,
    /// <paramref name="resolveTransaction"/> supplies the app's open connection and transaction — only the app
    /// knows where its per-message transaction lives, typically stashed in
    /// <see cref="ITransactionContext.Items"/> by its own <c>DbContext</c> setup. The store's appends in the
    /// handler enlist onto it and stage there. The <em>message's</em> unit of work owns the commit: committing
    /// that one shared transaction makes both the app's staged state and the enlisted events durable together.
    /// The store does not commit or roll it back — it only fires its deferred change signals once the
    /// transaction has committed. Both writers must share the same open <see cref="DbConnection"/> instance (a
    /// shared connection string is not enough — that is two connections and two transactions). A message whose
    /// resolver returns <c>null</c> runs with no frame, exactly as without this integration.
    /// </summary>
    public static void EnableEventStoreUnitOfWork(
        this OptionsConfigurer options,
        IEventStoreUnitOfWorkFactory unitOfWorkFactory,
        Func<ITransactionContext, (DbConnection Connection, DbTransaction Transaction)?> resolveTransaction)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(unitOfWorkFactory);
        ArgumentNullException.ThrowIfNull(resolveTransaction);

        options.Decorate<IPipeline>(context =>
        {
            var step = new EnlistEventStoreUnitOfWorkStep(unitOfWorkFactory, resolveTransaction);
            return new PipelineStepInjector(context.Get<IPipeline>())
                .OnReceive(step, PipelineRelativePosition.Before, typeof(DispatchIncomingMessageStep));
        });
    }
}
