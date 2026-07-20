namespace Dbosoft.EventStore.Rebus;

/// <summary>
/// The cross-process change notification carried over Rebus: "the given domain's <c>$all</c> advanced". It
/// is a best-effort wake signal, not an event — no payload, no ordering, no delivery guarantee — and a lost
/// one only costs latency, because the subscription's polling is the backstop.
/// </summary>
/// <param name="Domain">The domain the changed stream belongs to.</param>
/// <param name="StreamId">The stream that changed.</param>
[PublicAPI]
public sealed record EventStoreChanged(string Domain, string StreamId);
