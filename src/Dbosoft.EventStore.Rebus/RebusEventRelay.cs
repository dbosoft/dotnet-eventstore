using System.Globalization;
using Dbosoft.EventStore.Serialization;
using Rebus.Bus;

namespace Dbosoft.EventStore.Rebus;

/// <summary>
/// Forwards a domain's <c>$all</c> stream onto Rebus — the store→bus outbox seam. It reads the global
/// stream from a durable <see cref="ICheckpointStore"/>, materializes each stored event to its typed
/// payload, and publishes it with <see cref="EventStoreHeaders"/> stamped on the message, then advances
/// the checkpoint. Re-running resumes where it left off.
/// </summary>
/// <remarks>
/// <para>
/// The relay publishes in <c>$all</c> order, but <b>Rebus does not guarantee delivery order and delivers
/// at-least-once</b>: with competing consumers, retries and redelivery, handlers see events out of order
/// and more than once. Handlers must therefore sequence and de-duplicate using the headers
/// (<see cref="EventStoreMetadata"/>), not the order or the fact of arrival — see
/// <see cref="StreamOrderingTracker"/>. Treat the bus as a low-latency notification; the store's ordered
/// catch-up read remains the authority for rebuilding a read model or filling a gap.
/// </para>
/// <para>
/// Delivery from the relay is itself at-least-once (a crash between publishing and checkpointing re-publishes
/// the in-flight events), which is consistent with Rebus's own guarantee and the same reason handlers must
/// be idempotent.
/// </para>
/// </remarks>
[PublicAPI]
public sealed class RebusEventRelay
{
    private readonly IAllStreamReader _source;
    private readonly IEventMaterializer _materializer;
    private readonly ICheckpointStore _checkpoints;
    private readonly IBus _bus;
    private readonly string _checkpointName;
    private readonly RebusRelayOptions _options;

    /// <summary>Creates the relay for one domain's global stream.</summary>
    /// <param name="source">The domain's store, as an <see cref="IAllStreamReader"/>.</param>
    /// <param name="materializer">The domain's materializer (resolve it keyed by domain name).</param>
    /// <param name="checkpoints">Durable checkpoint storage; the relay resumes from the saved position.</param>
    /// <param name="bus">The Rebus bus to publish onto.</param>
    /// <param name="checkpointName">The name under which this relay's progress is stored.</param>
    /// <param name="options">Optional tuning; see <see cref="RebusRelayOptions"/>.</param>
    public RebusEventRelay(
        IAllStreamReader source,
        IEventMaterializer materializer,
        ICheckpointStore checkpoints,
        IBus bus,
        string checkpointName,
        RebusRelayOptions? options = null)
    {
        _source = source;
        _materializer = materializer;
        _checkpoints = checkpoints;
        _bus = bus;
        _checkpointName = checkpointName;
        _options = options ?? new RebusRelayOptions();
    }

    /// <summary>
    /// Publishes every event after the saved checkpoint, then advances it. Returns how many were published.
    /// </summary>
    public async Task<int> PumpAsync(CancellationToken ct = default)
    {
        var from = await _checkpoints.GetAsync(_checkpointName, ct).ConfigureAwait(false) ?? GlobalPosition.Start;
        var last = from;
        var moved = false;
        var published = 0;

        await foreach (var stored in _source.ReadAllAsync(from, ct: ct).ConfigureAwait(false))
        {
            if (ShouldPublish(stored) && _materializer.TryMaterialize(stored, out var payload))
            {
                await _bus.Publish(payload, HeadersFor(stored)).ConfigureAwait(false);
                published++;
            }

            last = stored.Position;
            moved = true;
        }

        if (moved)
            await _checkpoints.SetAsync(_checkpointName, last, ct).ConfigureAwait(false);
        return published;
    }

    /// <summary>
    /// Runs the relay until cancelled: pumps repeatedly, waiting <see cref="RebusRelayOptions.PollInterval"/>
    /// whenever a pump finds nothing new. Any exception from a read or a publish ends the run and
    /// propagates — the host owns the restart/backoff policy.
    /// </summary>
    public async Task RunAsync(CancellationToken ct = default)
    {
        while (!ct.IsCancellationRequested)
        {
            var published = await PumpAsync(ct).ConfigureAwait(false);
            if (published == 0)
                await Task.Delay(_options.PollInterval, _options.TimeProvider, ct).ConfigureAwait(false);
        }
    }

    private bool ShouldPublish(StoredEvent stored) =>
        _options.PublishSystemMarkers || !SystemEventTypes.IsSystem(stored.EventType);

    private static Dictionary<string, string> HeadersFor(StoredEvent stored) =>
        new()
        {
            [EventStoreHeaders.StreamId] = stored.StreamId,
            [EventStoreHeaders.StreamVersion] = stored.StreamVersion.ToString(CultureInfo.InvariantCulture),
            [EventStoreHeaders.GlobalPosition] = stored.Position.Token,
            [EventStoreHeaders.EventId] = stored.EventId.ToString("N"),
            [EventStoreHeaders.EventType] = stored.EventType
        };
}
