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
/// subscription simply forwards each event and advances the checkpoint (the fast path).
/// </item>
/// <item>
/// Without it, a catch-up read is complete only to a bounded-staleness window and may re-surface events
/// on a later read, so the subscription de-duplicates by <see cref="StoredEvent.EventId"/> over a bounded
/// window (<see cref="SubscriptionOptions.DeduplicationWindow"/>). This makes re-reading harmless, which
/// is what lets a host safely re-pump from an earlier checkpoint to pick up late-arriving events.
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
    {
        // Availability is a structural contract: a store supports $all iff it implements IAllStreamReader.
        if (store is not IAllStreamReader reader)
            throw new NotSupportedByProviderException(
                $"The store '{store.GetType().Name}' does not support $all reads, which a subscription requires.");

        _options = options ?? new SubscriptionOptions();
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_options.DeduplicationWindow, nameof(SubscriptionOptions.DeduplicationWindow));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_options.PollInterval.Ticks, nameof(SubscriptionOptions.PollInterval));

        _reader = reader;
        _checkpoints = checkpoints;
        _checkpointName = checkpointName;
        _handler = handler;
        _exactCatchup = store.Capabilities.HasFlag(StoreCapabilities.ExactCatchup);
    }

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

            await foreach (var stored in _reader.ReadAllAsync(from, ct: ct).ConfigureAwait(false))
            {
                if (_exactCatchup || !AlreadySeen(stored.EventId))
                {
                    await _handler(stored, ct).ConfigureAwait(false);
                    if (!_exactCatchup)
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
        while (!ct.IsCancellationRequested)
        {
            var dispatched = await PumpAsync(ct).ConfigureAwait(false);
            if (dispatched == 0)
                await Task.Delay(_options.PollInterval, _options.TimeProvider, ct).ConfigureAwait(false);
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
