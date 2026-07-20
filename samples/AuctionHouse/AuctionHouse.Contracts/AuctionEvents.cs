namespace AuctionHouse.Contracts;

/// <summary>Marker for the events stored on a lot's stream. They are deltas: order matters (each bid builds on the last).</summary>
public interface IAuctionEvent;

/// <summary>A lot was put up for auction — the first event on its stream.</summary>
public sealed record LotListed(string Title, decimal StartPrice, string Seller) : IAuctionEvent;

/// <summary>A bid was accepted (it exceeded the running high bid).</summary>
public sealed record BidPlaced(decimal Amount, string Bidder) : IAuctionEvent;

/// <summary>The auction closed; the winner is the last (highest) bidder, or none if there were no bids.</summary>
public sealed record AuctionClosed(decimal FinalPrice, string? Winner) : IAuctionEvent;
