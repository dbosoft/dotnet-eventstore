using AuctionHouse.Contracts;
using AuctionHouse.Hosting;
using Rebus.Bus;

namespace AuctionHouse.UIModule;

/// <summary>
/// The UI's command surface: it sends app commands to the writer module's queue over the shared bus. That is
/// the UI reaching the write side the same way it would across a process split — the read side it consumes
/// straight from the shared read model.
/// </summary>
public sealed class AuctionUiClient(IBus bus)
{
    public Task ListLot(string title, decimal startPrice, string seller) =>
        bus.Advanced.Routing.Send(QueueNames.Writer, new ListLotCommand(NewLotId(), title, startPrice, seller));

    public Task PlaceBid(string lotId, decimal amount, string bidder) =>
        bus.Advanced.Routing.Send(QueueNames.Writer, new PlaceBidCommand(lotId, amount, bidder));

    public Task CloseLot(string lotId) =>
        bus.Advanced.Routing.Send(QueueNames.Writer, new CloseLotCommand(lotId));

    private static string NewLotId() => Guid.NewGuid().ToString("N")[..6];
}
