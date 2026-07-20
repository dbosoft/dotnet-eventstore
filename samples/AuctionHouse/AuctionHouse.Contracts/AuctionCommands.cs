namespace AuctionHouse.Contracts;

// App-level commands the UI sends to the writer module over the bus. These are the app's own messaging —
// separate from the event store's change-notification ping. The writer handles them and appends events.

/// <summary>Ask the writer to list a new lot.</summary>
public sealed record ListLotCommand(string LotId, string Title, decimal StartPrice, string Seller);

/// <summary>Ask the writer to place a bid on a lot.</summary>
public sealed record PlaceBidCommand(string LotId, decimal Amount, string Bidder);

/// <summary>Ask the writer to close a lot's auction.</summary>
public sealed record CloseLotCommand(string LotId);
