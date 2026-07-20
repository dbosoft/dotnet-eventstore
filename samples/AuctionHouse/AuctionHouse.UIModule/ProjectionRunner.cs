using AuctionHouse.Contracts;
using AuctionHouse.Hosting;
using Dbosoft.EventStore;
using Dbosoft.EventStore.Rebus;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Rebus.Bus;

namespace AuctionHouse.UIModule;

/// <summary>
/// Keeps the UI's read model current — <b>not</b> with an ordered <c>$all</c> catch-up, but by reacting to a
/// "this stream changed" signal: it re-reads that one lot's stream and re-folds its whole view. Because the
/// read is a pure function of the committed stream, it is idempotent and self-healing — there is no global
/// position, no checkpoint and no safety window, and a lost signal only means a lot stays stale until the next
/// signal or the periodic sweep. The writer's ping reaches this module across the bus (the Rebus notify bridge).
/// </summary>
public sealed class ProjectionRunner(
    [FromKeyedServices(AuctionDomain.Name)] IEventReader reader,
    [FromKeyedServices(AuctionDomain.Name)] IAllStreamReader allReader,
    [FromKeyedServices(AuctionDomain.Name)] IEventMaterializer materializer,
    IChangeNotifier notifier,
    ILotReadStore readStore,
    IBus bus,
    ILogger<ProjectionRunner> log) : BackgroundService
{
    private static readonly TimeSpan SweepInterval = TimeSpan.FromSeconds(10);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Receive the writer's cross-module change pings over the bus, and watch for the changed stream ids.
        await bus.SubscribeToEventStoreChangesAsync().ConfigureAwait(false);
        using var watcher = notifier.Watch(AuctionDomain.Name);
        log.LogInformation("UI projection watching '{Domain}' for changed lots", AuctionDomain.Name);

        await SweepAsync(stoppingToken).ConfigureAwait(false); // pick up anything listed before we started

        while (!stoppingToken.IsCancellationRequested)
        {
            string streamId;
            try
            {
                using var timeout = new CancellationTokenSource(SweepInterval);
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, timeout.Token);
                streamId = await watcher.WaitAsync(linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
            {
                await SweepAsync(stoppingToken).ConfigureAwait(false); // backstop for any missed ping
                continue;
            }

            await RederiveAsync(streamId, stoppingToken).ConfigureAwait(false);
        }
    }

    // Re-read one changed lot's stream and re-fold its whole view — the unit of work.
    private async Task RederiveAsync(string streamId, CancellationToken ct)
    {
        var lotId = AuctionDomain.LotId(streamId);
        LotView? view = null;
        await foreach (var stored in reader.ReadStreamAsync(streamId, ct: ct).ConfigureAwait(false))
        {
            if (SystemEventTypes.IsSystem(stored.EventType) || !materializer.TryMaterialize(stored, out var @event))
                continue;
            view = Fold(view, lotId, @event);
        }

        if (view is null)
            return;

        readStore.Upsert(view);
        log.LogInformation("Re-derived lot {LotId} -> price {Price} ({Bids} bids)", lotId, view.CurrentPrice, view.BidCount);
    }

    // Backstop: discover every lot from $all and re-derive each. Only a safety net for a missed ping — the
    // per-stream re-derive above is the live path.
    private async Task SweepAsync(CancellationToken ct)
    {
        var streams = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var stored in allReader.ReadAllAsync(GlobalPosition.Start, ct: ct).ConfigureAwait(false))
            if (!SystemEventTypes.IsSystem(stored.EventType))
                streams.Add(stored.StreamId);

        foreach (var streamId in streams)
            await RederiveAsync(streamId, ct).ConfigureAwait(false);
    }

    private static LotView? Fold(LotView? current, string lotId, object @event) => @event switch
    {
        LotListed e => new LotView { LotId = lotId, Title = e.Title, Seller = e.Seller, StartPrice = e.StartPrice },
        BidPlaced e when current is not null => current with { HighBid = e.Amount, HighBidder = e.Bidder, BidCount = current.BidCount + 1 },
        AuctionClosed e when current is not null => current with { Closed = true, Winner = e.Winner },
        _ => current
    };
}
