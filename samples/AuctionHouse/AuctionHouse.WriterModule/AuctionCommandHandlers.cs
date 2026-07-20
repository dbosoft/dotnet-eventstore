using AuctionHouse.Contracts;
using Dbosoft.EventStore.Aggregates;
using Microsoft.Extensions.Logging;
using Rebus.Handlers;

namespace AuctionHouse.WriterModule;

/// <summary>
/// Handles the app commands that arrive on the writer's queue and turns them into appended events. Appending
/// makes the store fire its best-effort change ping, which the projection module wakes on. Domain rejections
/// (a bid that does not beat the price) are swallowed — a losing bid simply does not happen.
/// </summary>
public sealed class AuctionCommandHandlers(IAggregateRepository<Lot> lots, ILogger<AuctionCommandHandlers> log)
    : IHandleMessages<ListLotCommand>, IHandleMessages<PlaceBidCommand>, IHandleMessages<CloseLotCommand>
{
    public async Task Handle(ListLotCommand message)
    {
        var lot = Lot.List(message.LotId, message.Title, message.StartPrice, message.Seller);
        await lots.SaveAsync(lot);
        log.LogInformation("Listed lot {LotId} '{Title}' (start {Start})", message.LotId, message.Title, message.StartPrice);
    }

    public async Task Handle(PlaceBidCommand message)
    {
        var lot = await lots.LoadAsync(message.LotId);
        if (lot is null)
        {
            log.LogWarning("Bid on unknown lot {LotId} ignored", message.LotId);
            return;
        }

        try
        {
            lot.PlaceBid(message.Amount, message.Bidder);
        }
        catch (InvalidOperationException ex)
        {
            log.LogInformation("Bid rejected: {Reason}", ex.Message);
            return;
        }

        await lots.SaveAsync(lot);
        log.LogInformation("Bid {Amount} by {Bidder} accepted on lot {LotId}", message.Amount, message.Bidder, message.LotId);
    }

    public async Task Handle(CloseLotCommand message)
    {
        var lot = await lots.LoadAsync(message.LotId);
        if (lot is null)
            return;
        lot.Close();
        await lots.SaveAsync(lot);
        log.LogInformation("Closed lot {LotId}", message.LotId);
    }
}
