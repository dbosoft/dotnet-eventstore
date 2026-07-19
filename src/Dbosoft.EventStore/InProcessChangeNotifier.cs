using System.Collections.Concurrent;

namespace Dbosoft.EventStore;

/// <summary>
/// In-process <see cref="IChangeNotifier"/>: everyone waiting on a domain is released when that domain is
/// notified. Works only when the writer and the reader share the instance (same process); across processes
/// use a bridge over your own ping transport. No infrastructure, and — since it is best-effort — no
/// durability: a notify that races an in-flight <c>WaitForChangeAsync</c> is simply caught by the poll tick.
/// </summary>
[PublicAPI]
public sealed class InProcessChangeNotifier : IChangeNotifier
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _waiters = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public ValueTask NotifyAsync(string domain, CancellationToken ct = default)
    {
        // Release the current waiter for the domain; a subsequent WaitForChangeAsync installs a fresh one.
        if (_waiters.TryRemove(domain, out var waiter))
            waiter.TrySetResult();
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public Task WaitForChangeAsync(string domain, CancellationToken ct = default)
    {
        var waiter = _waiters.GetOrAdd(domain, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        return waiter.Task.WaitAsync(ct);
    }
}
