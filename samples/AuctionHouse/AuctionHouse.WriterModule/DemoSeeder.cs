using AuctionHouse.Contracts;
using Microsoft.Extensions.Hosting;
using Rebus.Bus;

namespace AuctionHouse.WriterModule;

/// <summary>
/// Drives the sample without a UI: after a short warm-up (so every module's bus and the projection are up),
/// it lists a couple of lots and places a rising series of bids, pacing them so the read model — and the
/// live UI — visibly update one step at a time. In the Blazor build the UI sends these commands instead.
/// </summary>
public sealed class DemoSeeder(IBus bus) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken).ConfigureAwait(false);

        await bus.SendLocal(new ListLotCommand("42", "Antique vase", 100m, "alice")).ConfigureAwait(false);
        await bus.SendLocal(new ListLotCommand("43", "Vintage watch", 250m, "bob")).ConfigureAwait(false);

        var bids = new (string Lot, decimal Amount, string Bidder)[]
        {
            ("42", 120m, "carol"),
            ("43", 300m, "dave"),
            ("42", 150m, "erin"),
            ("43", 325m, "carol"),
            ("42", 175m, "dave")
        };

        foreach (var (lot, amount, bidder) in bids)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(1400), stoppingToken).ConfigureAwait(false);
            await bus.SendLocal(new PlaceBidCommand(lot, amount, bidder)).ConfigureAwait(false);
        }
    }
}
