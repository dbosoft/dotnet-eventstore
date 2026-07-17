using Dbosoft.EventStore.Serialization;

namespace Dbosoft.EventStore.InMemory;

/// <summary>
/// The outbox/relay reference: pumps a domain's <c>$all</c> onto the <see cref="InMemoryBus"/>,
/// materializing each stored event to a typed payload and remembering its place in an
/// <see cref="ICheckpointStore"/>. This is the store→bus seam a real bus adapter (e.g. Rebus) would
/// fill; with a real bus the store side is exactly this — a catch-up read plus a checkpoint.
/// </summary>
[PublicAPI]
public sealed class StoreToBusRelay
{
    private readonly IAllStreamReader _source;
    private readonly IEventMaterializer _materializer;
    private readonly ICheckpointStore _checkpoints;
    private readonly InMemoryBus _bus;
    private readonly string _checkpointName;

    /// <summary>Creates the relay for one domain's global stream.</summary>
    public StoreToBusRelay(
        IAllStreamReader source,
        IEventMaterializer materializer,
        ICheckpointStore checkpoints,
        InMemoryBus bus,
        string checkpointName)
    {
        _source = source;
        _materializer = materializer;
        _checkpoints = checkpoints;
        _bus = bus;
        _checkpointName = checkpointName;
    }

    /// <summary>
    /// Publishes every event after the saved checkpoint, then advances the checkpoint. Idempotent to
    /// re-run: it resumes from where it left off. Returns how many events were published.
    /// </summary>
    public async Task<int> PumpAsync(CancellationToken ct = default)
    {
        var from = await _checkpoints.GetAsync(_checkpointName, ct).ConfigureAwait(false) ?? GlobalPosition.Start;
        var last = from;
        var count = 0;

        await foreach (var stored in _source.ReadAllAsync(from, ct: ct).ConfigureAwait(false))
        {
            var payload = _materializer.TryMaterialize(stored, out var typed) ? typed : stored;
            await _bus.PublishAsync(new BusMessage(stored, payload), ct).ConfigureAwait(false);
            last = stored.Position;
            count++;
        }

        if (count > 0)
            await _checkpoints.SetAsync(_checkpointName, last, ct).ConfigureAwait(false);

        return count;
    }
}
