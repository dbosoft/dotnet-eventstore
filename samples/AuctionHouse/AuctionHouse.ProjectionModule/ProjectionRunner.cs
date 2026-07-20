using AuctionHouse.Contracts;
using AuctionHouse.Hosting;
using Dbosoft.EventStore;
using Dbosoft.EventStore.Rebus;
using Dbosoft.EventStore.Serialization;
using Dbosoft.EventStore.Subscriptions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Rebus.Bus;

namespace AuctionHouse.ProjectionModule;

/// <summary>
/// Tails the store's ordered <c>$all</c> stream and folds each event into the shared read model. It subscribes
/// to the cross-process change ping and hands the notifier to the subscription, so it wakes the instant the
/// writer appends — with polling only as the backstop. Delivery is at-least-once, so it skips any event whose
/// stream version it has already folded (idempotent).
/// </summary>
public sealed class ProjectionRunner(
    [FromKeyedServices(AuctionDomain.Name)] IAllStreamReader reader,
    [FromKeyedServices(AuctionDomain.Name)] IEventMaterializer materializer,
    ICheckpointStore checkpoints,
    IChangeNotifier notifier,
    ILotReadStore readStore,
    IBus bus,
    ILogger<ProjectionRunner> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Receive the writer's cross-module change pings over the bus.
        await bus.SubscribeToEventStoreChangesAsync().ConfigureAwait(false);

        var options = new SubscriptionOptions
        {
            ChangeNotifier = notifier,     // wake on the ping...
            NotifyKey = AuctionDomain.Name,
            PollInterval = TimeSpan.FromSeconds(1), // ...with a short poll as the backstop for any missed ping

            // EF Core assigns $all positions before commit, so the subscription holds back events newer than
            // this window to stay complete under concurrent appends. A single writer here commits in order, so
            // a small window keeps the UI snappy while still guarding the rare race. (Default is 5s.)
            SafetyWindow = TimeSpan.FromMilliseconds(250)
        };

        var subscription = EventStoreSubscription.ForReader(reader, checkpoints, "auction-projection", Project, options);
        log.LogInformation("Projection tailing $all for domain '{Domain}'", AuctionDomain.Name);
        await subscription.RunAsync(stoppingToken).ConfigureAwait(false);
    }

    private Task Project(StoredEvent stored, CancellationToken ct)
    {
        if (SystemEventTypes.IsSystem(stored.EventType) || !materializer.TryMaterialize(stored, out var @event) || @event is null)
            return Task.CompletedTask;

        var lotId = AuctionDomain.LotId(stored.StreamId);
        var current = readStore.Get(lotId);
        if (current is not null && stored.StreamVersion <= current.LastVersion)
            return Task.CompletedTask; // already folded (a re-delivery) — skip

        var next = Fold(current, lotId, @event);
        if (next is null)
            return Task.CompletedTask;

        readStore.Upsert(next with { LastVersion = stored.StreamVersion });
        log.LogInformation("Folded {Event} on lot {LotId} -> price {Price}", @event.GetType().Name, lotId, next.CurrentPrice);
        return Task.CompletedTask;
    }

    private static LotView? Fold(LotView? current, string lotId, object @event) => @event switch
    {
        LotListed e => new LotView { LotId = lotId, Title = e.Title, Seller = e.Seller, StartPrice = e.StartPrice },
        BidPlaced e when current is not null => current with { HighBid = e.Amount, HighBidder = e.Bidder, BidCount = current.BidCount + 1 },
        AuctionClosed e when current is not null => current with { Closed = true, Winner = e.Winner },
        _ => current
    };
}
