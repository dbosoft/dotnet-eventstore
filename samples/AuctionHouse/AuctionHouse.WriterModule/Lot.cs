using AuctionHouse.Contracts;
using Dbosoft.EventStore.Aggregates;

namespace AuctionHouse.WriterModule;

/// <summary>
/// The lot aggregate — the write model. It decides what may happen (a bid must beat the running price),
/// raises delta events, and folds them into its own state. It never touches the store.
/// </summary>
public sealed class Lot(string id) : AggregateBase(id)
{
    /// <summary>Stream-id category — a lot's stream is "lot-{id}".</summary>
    public const string Category = "lot";

    private bool _listed;
    private bool _closed;
    private decimal _startPrice;
    private decimal _highBid;
    private string? _highBidder;

    public static Lot List(string id, string title, decimal startPrice, string seller)
    {
        var lot = new Lot(id);
        lot.Raise(new LotListed(title, startPrice, seller));
        return lot;
    }

    public void PlaceBid(decimal amount, string bidder)
    {
        if (!_listed) throw new InvalidOperationException($"Lot '{Id}' is not listed.");
        if (_closed) throw new InvalidOperationException($"Lot '{Id}' auction is closed.");

        // First bid must at least meet the start price; every later bid must beat the running high bid.
        var floor = _highBid > 0 ? _highBid : _startPrice;
        var mustExceed = _highBid > 0;
        if (mustExceed ? amount <= floor : amount < floor)
            throw new InvalidOperationException(
                $"Bid {amount} does not beat the current price {floor} on lot '{Id}'.");

        Raise(new BidPlaced(amount, bidder));
    }

    public void Close()
    {
        if (!_listed || _closed) return;
        Raise(new AuctionClosed(_highBid, _highBidder));
    }

    protected override void When(object @event)
    {
        switch (@event)
        {
            case LotListed e:
                _listed = true;
                _startPrice = e.StartPrice;
                break;
            case BidPlaced e:
                _highBid = e.Amount;
                _highBidder = e.Bidder;
                break;
            case AuctionClosed:
                _closed = true;
                break;
        }
    }
}
