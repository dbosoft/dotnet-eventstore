using System.Collections.Concurrent;

namespace Dbosoft.EventStore.Rebus;

/// <summary>How an incoming event relates to what a stream has already applied.</summary>
[PublicAPI]
public enum StreamMessageOrder
{
    /// <summary>The next version in order — apply it, then <see cref="StreamOrderingTracker.MarkApplied"/>.</summary>
    Next,

    /// <summary>Already applied (this version or an older one) — a Rebus redelivery or reorder; skip it.</summary>
    Duplicate,

    /// <summary>Ahead of the next expected version — earlier events have not arrived yet (Rebus reordered
    /// or dropped them); catch up from the store for the missing versions before applying.</summary>
    Gap
}

/// <summary>
/// Turns Rebus's out-of-order, at-least-once delivery into an in-order, de-duplicated feed per stream, by
/// tracking the highest version applied for each stream and classifying each incoming event as
/// <see cref="StreamMessageOrder.Next"/>, <see cref="StreamMessageOrder.Duplicate"/> or
/// <see cref="StreamMessageOrder.Gap"/>. A <see cref="StreamMessageOrder.Gap"/> means the bus delivered a
/// later event first — read the stream from <see cref="LastApplied"/><c> + 1</c> to fill it in from the
/// store, which is the authority for order.
/// </summary>
/// <remarks>
/// State is in-memory and per-process. A durable read model persists its own per-stream watermark and
/// <see cref="Seed"/>s the tracker from it on startup, then persists the watermark together with the read
/// model after applying each event — so a restart resumes exactly, not from zero. It is safe for concurrent
/// use across streams; apply one stream's events on a single worker so its versions are not raced.
/// </remarks>
[PublicAPI]
public sealed class StreamOrderingTracker
{
    private readonly ConcurrentDictionary<string, long> _applied = new(StringComparer.Ordinal);

    /// <summary>Classifies an incoming event without changing state.</summary>
    public StreamMessageOrder Classify(string streamId, long version)
    {
        var last = _applied.GetValueOrDefault(streamId);
        if (version <= last)
            return StreamMessageOrder.Duplicate;
        return version == last + 1 ? StreamMessageOrder.Next : StreamMessageOrder.Gap;
    }

    /// <summary>Records that a stream has been applied up to <paramref name="version"/> (never moves backward).</summary>
    public void MarkApplied(string streamId, long version) =>
        _applied.AddOrUpdate(streamId, version, (_, existing) => Math.Max(existing, version));

    /// <summary>The highest version applied for a stream, or 0 if none.</summary>
    public long LastApplied(string streamId) => _applied.GetValueOrDefault(streamId);

    /// <summary>Seeds a stream's applied version from a persisted watermark, so a restart resumes in order.</summary>
    public void Seed(string streamId, long version) => MarkApplied(streamId, version);
}
