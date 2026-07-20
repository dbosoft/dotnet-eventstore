namespace Dbosoft.EventStore.Rebus;

/// <summary>
/// The cross-process change notification carried over Rebus: "a stream in this domain changed". It is a
/// best-effort signal, not an event — it carries only the stream id, with no ordering or delivery guarantee —
/// and a lost one only costs latency, because re-reading (or polling) is the backstop.
/// </summary>
/// <param name="Domain">The domain the changed stream belongs to.</param>
/// <param name="StreamId">The stream that changed.</param>
[PublicAPI]
public sealed record EventStoreChanged(string Domain, string StreamId);
