namespace Dbosoft.EventStore.InMemory;

/// <summary>
/// A catch-up subscription over an <see cref="InMemoryEventStore"/>: it reads forward from a
/// checkpoint, then waits for a signal that new events were appended and reads again. Because the
/// store serializes writes into a dense sequence, the handler sees every event exactly once, in
/// order, with no gaps.
/// </summary>
internal sealed class InMemorySubscription : IAsyncDisposable
{
    private readonly InMemoryEventStore _store;
    private readonly IEventHandler _handler;
    private readonly CancellationTokenSource _cts = new();
    private readonly SemaphoreSlim _signal = new(0, 1);
    private GlobalPosition _position;
    private Task? _loop;

    public InMemorySubscription(InMemoryEventStore store, GlobalPosition checkpoint, IEventHandler handler)
    {
        _store = store;
        _handler = handler;
        _position = checkpoint;
    }

    public void Start() => _loop = Task.Run(() => RunAsync(_cts.Token));

    public void Notify()
    {
        try
        {
            if (_signal.CurrentCount == 0)
                _signal.Release();
        }
        catch (SemaphoreFullException)
        {
            // Already signalled; the loop will pick up all pending events in one pass.
        }
    }

    private async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var delivered = false;
            await foreach (var stored in _store.ReadAllAsync(_position, ct: ct).ConfigureAwait(false))
            {
                await _handler.HandleAsync(stored, ct).ConfigureAwait(false);
                _position = stored.Position;
                delivered = true;
            }

            if (delivered)
                continue;

            try
            {
                await _signal.WaitAsync(TimeSpan.FromMilliseconds(50), ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _store.RemoveSubscription(this);
        await _cts.CancelAsync().ConfigureAwait(false);

        if (_loop is not null)
        {
            try
            {
                await _loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected on disposal.
            }
        }

        _cts.Dispose();
        _signal.Dispose();
    }
}
