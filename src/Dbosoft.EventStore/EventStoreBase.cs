namespace Dbosoft.EventStore;

/// <summary>
/// Base class for provider event stores. It owns the one cross-cutting concern that has to fire on every
/// write but is identical for every provider: the best-effort change signal that lets a subscription wake
/// on a write instead of waiting out its poll interval. A provider derives from this, implements the core
/// operations, and gets "signal subscribers on write" for free on the core append path — no provider
/// re-implements the signalling.
/// </summary>
/// <remarks>
/// The signal is best-effort and carries no content or ordering: the in-process notifier completes inline,
/// anything slower is fire-and-forget, and a lost signal only costs latency because the subscription's
/// polling is the backstop. The core <see cref="AppendToStreamAsync"/> is a template method that fires the
/// signal once the provider's <see cref="AppendToStreamCoreAsync"/> returns; a provider that exposes further
/// write paths beyond the core append (a multi-stream append, a maintenance marker) calls
/// <see cref="NotifyChanged"/> itself after that write, since only the write site knows a write happened.
/// </remarks>
[PublicAPI]
public abstract class EventStoreBase : IEventStore
{
    private readonly IChangeNotifier? _changeNotifier;

    /// <summary>
    /// Initialises the base with the change notifier to signal on write, if any. A null notifier disables
    /// signalling. The key signalled under is <see cref="NotifyDomain"/>, which the derived store supplies.
    /// </summary>
    protected EventStoreBase(IChangeNotifier? changeNotifier) => _changeNotifier = changeNotifier;

    /// <summary>
    /// The key this store signals changes under — the domain a subscription waits on. Supplied by the
    /// derived store (each store backs one domain), so the base need not capture it separately.
    /// </summary>
    protected abstract string NotifyDomain { get; }

    /// <inheritdoc />
    public abstract StoreCapabilities Capabilities { get; }

    /// <inheritdoc />
    public async Task<AppendResult> AppendToStreamAsync(
        string streamId,
        long expectedVersion,
        IReadOnlyList<EventData> events,
        CancellationToken ct = default)
    {
        var result = await AppendToStreamCoreAsync(streamId, expectedVersion, events, ct).ConfigureAwait(false);
        NotifyChanged(streamId);
        return result;
    }

    /// <summary>
    /// The provider's append implementation. The base wraps it (see <see cref="AppendToStreamAsync"/>) and
    /// fires the change signal once it returns; a throwing append propagates without signalling. Semantics
    /// are exactly those of <see cref="IEventStore.AppendToStreamAsync"/>.
    /// </summary>
    protected abstract Task<AppendResult> AppendToStreamCoreAsync(
        string streamId,
        long expectedVersion,
        IReadOnlyList<EventData> events,
        CancellationToken ct);

    /// <inheritdoc />
    public abstract IAsyncEnumerable<StoredEvent> ReadStreamAsync(
        string streamId,
        ReadDirection direction = ReadDirection.Forwards,
        long fromVersion = 0,
        long maxCount = long.MaxValue,
        CancellationToken ct = default);

    /// <inheritdoc />
    public abstract Task<StreamInfo?> GetStreamInfoAsync(string streamId, CancellationToken ct = default);

    /// <summary>
    /// Fires the best-effort "a stream changed" signal for this store's domain, if a notifier is configured,
    /// so a reader wakes without waiting out its poll interval. The core append path calls this for the
    /// provider; a provider calls it after any additional write path it exposes (a multi-stream append, a
    /// maintenance marker), passing the id of the changed stream. It never blocks: the in-process notifier
    /// completes inline, anything slower is fire-and-forget, and a lost signal is caught by re-reading.
    /// </summary>
    protected void NotifyChanged(string streamId)
    {
        if (_changeNotifier is null)
            return;
        var pending = _changeNotifier.NotifyAsync(NotifyDomain, streamId);
        if (!pending.IsCompleted)
            _ = pending.AsTask();
    }
}
