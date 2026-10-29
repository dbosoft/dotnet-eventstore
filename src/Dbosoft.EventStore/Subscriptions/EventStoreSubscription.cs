namespace Dbosoft.EventStore.Subscriptions;

/// <summary>
/// A durable catch-up subscription over a domain's <c>$all</c> sequence. It reads forward from a saved
/// checkpoint, invokes a handler for each event, and advances the checkpoint once the handler succeeds —
/// the shape a read-model rebuilder or a bus relay needs. Delivery is <b>at-least-once</b>: a crash
/// between the handler and the checkpoint re-delivers the in-flight events, so handlers must be idempotent.
/// </summary>
/// <remarks>
/// The backing store's <see cref="StoreCapabilities.ExactCatchup"/> selects the strategy:
/// <list type="bullet">
/// <item>
/// With it, a forward read from a position is complete and never re-surfaces an event, so the
/// subscription simply forwards each event and advances the checkpoint (the fast path). The checkpoint
/// advances only at the end of a pump, so a handler that throws makes the next pump read the events before
/// it again; the de-duplication window keeps those from reaching the handler twice in this process.
/// </item>
/// <item>
/// Without it, positions are assigned before the commit that makes an event visible, so a naive forward
/// reader could skip an event that commits below a position it has already passed. The subscription
/// prevents that with a <see cref="SubscriptionOptions.SafetyWindow"/>: it does not deliver or checkpoint
/// past an event recorded more recently than the window, so a not-yet-committed lower sibling is never
/// skipped — completeness holds provided no append stays uncommitted longer than the window (the price is
/// that much delivery latency). De-duplication by <see cref="StoredEvent.EventId"/> over a bounded window
/// (<see cref="SubscriptionOptions.DeduplicationWindow"/>) makes the re-reads across that overlap idempotent.
/// </item>
/// </list>
/// Requires the store to support <c>$all</c> reads (<see cref="IAllStreamReader"/>); constructing a
/// subscription over a store that does not throws <see cref="NotSupportedByProviderException"/>.
/// A single subscription instance pumps one call at a time — <see cref="PumpAsync"/> and
/// <see cref="RunAsync"/> are not safe to invoke concurrently on the same instance (they share the
/// checkpoint read-modify-write and the de-duplication window), and a concurrent call throws.
/// </remarks>
[PublicAPI]
public sealed class EventStoreSubscription
{
    private readonly IAllStreamReader _reader;
    private readonly ICheckpointStore _checkpoints;
    private readonly string _checkpointName;
    private readonly Func<StoredEvent, CancellationToken, Task> _handler;
    private readonly SubscriptionOptions _options;
    private readonly bool _exactCatchup;

    // De-duplication state, used only when the store is not exact-catch-up. A FIFO window bounds memory:
    // the set answers "seen?", the queue evicts the oldest id once the window is full. An id is recorded
    // only after its handler succeeds, so a handler that throws re-delivers the event on the next pump
    // instead of being skipped as already-seen.
    private readonly HashSet<Guid> _seen = [];
    private readonly Queue<Guid> _seenOrder = new();

    // Reentrancy guard: 0 = idle, 1 = a pump is in flight. Set with Interlocked so a concurrent pump is
    // rejected rather than corrupting the dedup window or racing the checkpoint.
    private int _pumping;

    /// <summary>Creates a subscription that dispatches each event of the store's global sequence to a handler.</summary>
    /// <param name="store">The backing event store; must support <c>$all</c> reads.</param>
    /// <param name="checkpoints">Durable checkpoint storage; the subscription resumes from the saved position.</param>
    /// <param name="checkpointName">The name under which this subscription's progress is stored.</param>
    /// <param name="handler">Invoked once per event, before the checkpoint advances past it.</param>
    /// <param name="options">Optional tuning; see <see cref="SubscriptionOptions"/>.</param>
    /// <exception cref="NotSupportedByProviderException">The store does not support <c>$all</c> reads.</exception>
    public EventStoreSubscription(
        IEventStore store,
        ICheckpointStore checkpoints,
        string checkpointName,
        Func<StoredEvent, CancellationToken, Task> handler,
        SubscriptionOptions? options = null)
        // Availability is a structural contract: a store supports $all iff it implements IAllStreamReader.
        : this(
            store as IAllStreamReader ?? throw new NotSupportedByProviderException(
                $"The store '{store.GetType().Name}' does not support $all reads, which a subscription requires."),
            checkpoints, checkpointName, handler, options)
    {
    }

    // The real constructor, over the read-only reader. Private so the two public entry points — the
    // IEventStore ctor above and the ForReader factory below — never collide in overload resolution when a
    // store (which is both an IEventStore and an IAllStreamReader) is passed.
    private EventStoreSubscription(
        IAllStreamReader reader,
        ICheckpointStore checkpoints,
        string checkpointName,
        Func<StoredEvent, CancellationToken, Task> handler,
        SubscriptionOptions? options)
    {
        ArgumentNullException.ThrowIfNull(reader);

        _options = options ?? new SubscriptionOptions();
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_options.DeduplicationWindow, nameof(SubscriptionOptions.DeduplicationWindow));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_options.PollInterval.Ticks, nameof(SubscriptionOptions.PollInterval));
        ArgumentOutOfRangeException.ThrowIfNegative(_options.SafetyWindow.Ticks, nameof(SubscriptionOptions.SafetyWindow));

        _reader = reader;
        _checkpoints = checkpoints;
        _checkpointName = checkpointName;
        _handler = handler;

        // Exact-catch-up is a store behaviour. When the reader is the store itself (the in-process case),
        // honour its flag; a bare reader — e.g. a cross-process read-only reader that is not an IEventStore —
        // is treated as non-exact, which is the safe path (safety window + de-dup): always correct, only slower.
        _exactCatchup = reader is IEventStore store && store.Capabilities.HasFlag(StoreCapabilities.ExactCatchup);
    }

    /// <summary>
    /// Creates a subscription over a read-only <c>$all</c> reader — the shape a projection or relay
    /// component gets when it is wired with only the keyed <see cref="IAllStreamReader"/> (plus a checkpoint
    /// store) and so is structurally unable to write. A reader that is not also the store is treated as
    /// non-exact-catch-up (the safe path), since the <see cref="StoreCapabilities.ExactCatchup"/> guarantee
    /// belongs to the store.
    /// </summary>
    /// <param name="reader">The read-only global-sequence reader.</param>
    /// <param name="checkpoints">Durable checkpoint storage; the subscription resumes from the saved position.</param>
    /// <param name="checkpointName">The name under which this subscription's progress is stored.</param>
    /// <param name="handler">Invoked once per event, before the checkpoint advances past it.</param>
    /// <param name="options">Optional tuning; see <see cref="SubscriptionOptions"/>.</param>
    public static EventStoreSubscription ForReader(
        IAllStreamReader reader,
        ICheckpointStore checkpoints,
        string checkpointName,
        Func<StoredEvent, CancellationToken, Task> handler,
        SubscriptionOptions? options = null) =>
        new(reader, checkpoints, checkpointName, handler, options);

    /// <summary>
    /// Creates a subscription whose first run starts at the global position for a wall-clock time,
    /// resolved through the store's <see cref="ITimeNavigable"/> support — so a fresh consumer can replay
    /// the global stream from a point in time. Once it has checkpointed it resumes from the checkpoint as
    /// usual, so the start time only applies until the first pump saves progress. Requires the store to
    /// support both <c>$all</c> reads and time navigation.
    /// </summary>
    /// <exception cref="NotSupportedByProviderException">The store does not support time navigation (or <c>$all</c> reads).</exception>
    public static async Task<EventStoreSubscription> StartingAtAsync(
        IEventStore store,
        ICheckpointStore checkpoints,
        string checkpointName,
        Func<StoredEvent, CancellationToken, Task> handler,
        DateTimeOffset from,
        SubscriptionOptions? options = null,
        CancellationToken ct = default)
    {
        if (store is not ITimeNavigable navigable)
            throw new NotSupportedByProviderException(
                $"The store '{store.GetType().Name}' does not support time navigation, which starting a subscription at a time requires.");

        var start = await navigable.FindPositionAtAsync(from, ct).ConfigureAwait(false);
        var effective = (options ?? new SubscriptionOptions()) with { StartFrom = start };
        return new EventStoreSubscription(store, checkpoints, checkpointName, handler, effective);
    }

    /// <summary>
    /// Reads everything available after the saved checkpoint, dispatches it, and advances the checkpoint.
    /// Returns how many events the handler was invoked for (de-duplicated events are not counted).
    /// </summary>
    public async Task<int> PumpAsync(CancellationToken ct = default)
    {
        if (Interlocked.Exchange(ref _pumping, 1) == 1)
            throw new InvalidOperationException(
                "A pump is already in flight on this subscription; PumpAsync/RunAsync are not reentrant.");
        try
        {
            var from = await _checkpoints.GetAsync(_checkpointName, ct).ConfigureAwait(false) ?? _options.StartFrom;
            var last = from;
            var moved = false;
            var dispatched = 0;

            // On a non-exact store, do not advance past an event newer than the safety horizon: a
            // lower-positioned sibling may still be in flight and would be skipped. On an exact store the
            // horizon is open (events are complete by position).
            var horizon = _exactCatchup
                ? DateTimeOffset.MaxValue
                : _options.TimeProvider.GetUtcNow() - _options.SafetyWindow;

            await foreach (var stored in _reader.ReadAllAsync(from, ct: ct).ConfigureAwait(false))
            {
                if (stored.CreatedAt > horizon)
                    break; // too fresh; hold it (and everything after) for a later pump

                if (!AlreadySeen(stored.EventId))
                {
                    await _handler(stored, ct).ConfigureAwait(false);
                    Record(stored.EventId); // record only after success, so a throw re-delivers
                    dispatched++;
                }

                last = stored.Position;
                moved = true;
            }

            if (moved)
                await _checkpoints.SetAsync(_checkpointName, last, ct).ConfigureAwait(false);
            return dispatched;
        }
        finally
        {
            Interlocked.Exchange(ref _pumping, 0);
        }
    }

    /// <summary>
    /// Runs the subscription until cancelled: pumps repeatedly, waiting <see cref="SubscriptionOptions.PollInterval"/>
    /// whenever a pump finds nothing new. It does <b>not</b> retry on failure — any exception from a read or a
    /// handler ends the run and propagates, so the host owns the restart/backoff policy; only "nothing new"
    /// is handled internally, by polling. Cancelling <paramref name="ct"/> stops the loop.
    /// </summary>
    public async Task RunAsync(CancellationToken ct = default)
    {
        // A change watcher (if configured) wakes the loop on a write; the changed stream id is not needed
        // here, since the pump re-reads $all from the checkpoint. Polling is the backstop for a missed signal.
        using var watcher = _options is { ChangeNotifier: { } notifier, NotifyKey: { } key }
            ? notifier.Watch(key)
            : null;

        while (!ct.IsCancellationRequested)
        {
            var dispatched = await PumpAsync(ct).ConfigureAwait(false);
            if (dispatched == 0)
                await WaitForWorkAsync(watcher, ct).ConfigureAwait(false);
        }
    }

    // After an empty pump, wait for the next change (bounded by the poll interval as a backstop), or just
    // poll when no watcher is configured. A missed signal is caught by the poll tick.
    private async Task WaitForWorkAsync(IChangeWatcher? watcher, CancellationToken ct)
    {
        if (watcher is null)
        {
            await Task.Delay(_options.PollInterval, _options.TimeProvider, ct).ConfigureAwait(false);
            return;
        }

        using var timeout = new CancellationTokenSource(_options.PollInterval, _options.TimeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeout.Token);
        try
        {
            await watcher.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // The poll interval elapsed with no change — pump anyway.
        }
    }

    // Peeks the window without mutating it, so an event is only committed as "seen" after its handler
    // succeeds (see Record).
    private bool AlreadySeen(Guid eventId) => _seen.Contains(eventId);

    // Records an id as seen. Evicts the oldest id once the window is full, so memory is bounded to
    // DeduplicationWindow.
    private void Record(Guid eventId)
    {
        if (!_seen.Add(eventId))
            return;

        _seenOrder.Enqueue(eventId);
        if (_seenOrder.Count > _options.DeduplicationWindow)
            _seen.Remove(_seenOrder.Dequeue());
    }
}
