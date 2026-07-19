using System.Collections.Concurrent;
using Dbosoft.EventStore.Serialization;

namespace Dbosoft.EventStore.Rebus;

/// <summary>
/// Turns Rebus's unordered, at-least-once, possibly-gapped delivery into strictly ordered, exactly-once,
/// gapless per-stream application. It treats each bus message as a <i>signal</i> ("stream S has an event at
/// version V") and reads the ordered, authoritative content from the store: a duplicate signal is skipped,
/// a signal that jumps ahead fills the missing versions from the store, and every event is handled once,
/// in ascending version order per stream. Streams are independent and dispatched concurrently; a single
/// stream's events are serialized so their versions are never raced.
/// </summary>
/// <remarks>
/// The consumer needs read access to the event store — that is precisely why the store, not the bus, is the
/// authority for order. A consumer with no store access cannot reconstruct order from an unordered
/// at-least-once bus alone; it must either configure Rebus for ordered delivery (a single worker, no
/// concurrent handlers) or accept best-effort with a per-stream version guard. This dispatcher orders
/// <i>within</i> a stream; it does not impose a global order across streams — use the store's <c>$all</c>
/// catch-up (<c>EventStoreSubscription</c>) when a read model needs that.
/// </remarks>
[PublicAPI]
public sealed class OrderedEventDispatcher
{
    private readonly IEventStore _store;
    private readonly IEventMaterializer _materializer;
    private readonly Func<object, EventStoreMetadata, CancellationToken, Task> _handler;
    private readonly bool _includeSystemMarkers;
    private readonly StreamOrderingTracker _tracker = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _locks = new(StringComparer.Ordinal);

    /// <summary>Creates the dispatcher.</summary>
    /// <param name="store">The domain's store — the authority for order; must support per-stream reads (all do).</param>
    /// <param name="materializer">The domain's materializer (resolve it keyed by domain name).</param>
    /// <param name="handler">Invoked once per event, in order, with the typed event and its identity.</param>
    /// <param name="includeSystemMarkers">Whether to deliver in-band <c>$system</c> markers found on a stream.</param>
    public OrderedEventDispatcher(
        IEventStore store,
        IEventMaterializer materializer,
        Func<object, EventStoreMetadata, CancellationToken, Task> handler,
        bool includeSystemMarkers = false)
    {
        _store = store;
        _materializer = materializer;
        _handler = handler;
        _includeSystemMarkers = includeSystemMarkers;
    }

    /// <summary>Seeds a stream's applied version from a persisted watermark, so a restart resumes in order.</summary>
    public void Seed(string streamId, long version) => _tracker.Seed(streamId, version);

    /// <summary>The highest version applied for a stream, or 0 if none.</summary>
    public long LastApplied(string streamId) => _tracker.LastApplied(streamId);

    /// <summary>
    /// Handles a bus message identified by <paramref name="signal"/> (from <see cref="EventStoreMetadata.TryRead"/>):
    /// applies every not-yet-applied event of the stream up to its version, in order, exactly once. Safe to
    /// call with signals arriving out of order or more than once.
    /// </summary>
    public async Task DispatchAsync(EventStoreMetadata signal, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(signal);

        // Serialize per stream: two signals for the same stream must not read-and-apply concurrently.
        var gate = _locks.GetOrAdd(signal.StreamId, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (signal.StreamVersion <= _tracker.LastApplied(signal.StreamId))
                return; // this version, and everything before it, is already applied

            var from = _tracker.LastApplied(signal.StreamId) + 1;
            await foreach (var stored in _store
                               .ReadStreamAsync(signal.StreamId, ReadDirection.Forwards, from, ct: ct)
                               .ConfigureAwait(false))
            {
                if (stored.StreamVersion > signal.StreamVersion)
                    break; // caught up to what this signal announced; later versions await their own signal
                if (stored.StreamVersion <= _tracker.LastApplied(signal.StreamId))
                    continue; // defensive: already applied

                var isSystem = SystemEventTypes.IsSystem(stored.EventType);
                if ((!isSystem || _includeSystemMarkers) && _materializer.TryMaterialize(stored, out var payload))
                    await _handler(payload, MetadataOf(stored), ct).ConfigureAwait(false);

                _tracker.MarkApplied(signal.StreamId, stored.StreamVersion); // only after the handler succeeds
            }
        }
        finally
        {
            gate.Release();
        }
    }

    private static EventStoreMetadata MetadataOf(StoredEvent e) =>
        new(e.StreamId, e.StreamVersion, e.Position, e.EventId, e.EventType);
}
