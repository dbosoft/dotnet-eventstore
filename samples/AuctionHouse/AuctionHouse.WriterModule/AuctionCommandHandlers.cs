using AuctionHouse.Contracts;
using Dbosoft.EventStore;
using Dbosoft.EventStore.Aggregates;
using Microsoft.Extensions.Logging;
using Rebus.Handlers;

namespace AuctionHouse.WriterModule;

/// <summary>
/// Handles the app commands that arrive on the writer's queue and turns them into appended events. Appending
/// makes the store fire its best-effort change ping, which the reader wakes on. A bid that does not beat the
/// price is a domain rejection and simply does not happen; a bid that loses a concurrency race is reloaded and
/// re-evaluated against the new state.
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
        // Reload and re-evaluate on a concurrency race: a competing bid may have landed first (raising the
        // price) between the load and the save, so the bid is re-checked against the new state.
        for (var attempt = 0; attempt < 5; attempt++)
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

            try
            {
                await lots.SaveAsync(lot);
            }
            catch (WrongExpectedVersionException)
            {
                continue; // another bid landed first — reload and re-evaluate
            }

            log.LogInformation("Bid {Amount} by {Bidder} accepted on lot {LotId}", message.Amount, message.Bidder, message.LotId);
            return;
        }

        log.LogWarning("Bid on lot {LotId} gave up after repeated version conflicts", message.LotId);
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
