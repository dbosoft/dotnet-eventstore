using Dbosoft.EventStore.Rebus;
using Rebus.Handlers;
using Rebus.Pipeline;

namespace Dbosoft.EventStore.Sample.RebusReadModel;

/// <summary>
/// The Rebus consumer. It does both things the sample is about:
/// (a) the <b>direct</b> typed event off the bus feeds a live activity log — fine with out-of-order,
/// at-least-once delivery, because it is only a notification;
/// (b) the same message is treated as a <b>signal</b> and handed to the <see cref="OrderedEventDispatcher"/>,
/// which applies the stream from the <b>store</b> in order, exactly once — the authority for the read model.
/// </summary>
internal sealed class AccountBusHandler(OrderedEventDispatcher dispatcher, List<string> activityLog, int[] handled)
    : IHandleMessages<AccountOpened>, IHandleMessages<MoneyDeposited>, IHandleMessages<MoneyWithdrawn>
{
    public Task Handle(AccountOpened message) => OnEvent(message);

    public Task Handle(MoneyDeposited message) => OnEvent(message);

    public Task Handle(MoneyWithdrawn message) => OnEvent(message);

    private async Task OnEvent(IAccountEvent message)
    {
        if (!EventStoreMetadata.TryRead(MessageContext.Current!.Headers, out var meta))
            return;

        lock (activityLog)
            activityLog.Add($"{meta.StreamId} v{meta.StreamVersion}: {message}");

        await dispatcher.DispatchAsync(meta);
        Interlocked.Increment(ref handled[0]);
    }
}
