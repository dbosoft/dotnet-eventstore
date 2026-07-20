namespace AuctionHouse.Contracts;

/// <summary>The read model for one lot — a pure fold of its event stream, kept by the projection module.</summary>
public sealed record LotView
{
    public required string LotId { get; init; }
    public required string Title { get; init; }
    public required string Seller { get; init; }
    public decimal StartPrice { get; init; }
    public decimal HighBid { get; init; }
    public string? HighBidder { get; init; }
    public int BidCount { get; init; }
    public bool Closed { get; init; }
    public string? Winner { get; init; }

    /// <summary>The current asking/selling price: the high bid once someone has bid, otherwise the start price.</summary>
    public decimal CurrentPrice => BidCount > 0 ? HighBid : StartPrice;
}
