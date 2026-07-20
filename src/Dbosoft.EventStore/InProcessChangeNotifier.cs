using System.Collections.Concurrent;

namespace Dbosoft.EventStore;

/// <summary>
/// In-process <see cref="IChangeNotifier"/>: every watcher of a domain receives the id of each changed stream.
/// Works only when the writer and the readers share the instance (same process); across processes use a bridge
/// over your own ping transport. No infrastructure and no durability — best-effort: a watcher opened after a
/// change never sees it, and re-reading (or polling) is the backstop.
/// </summary>
[PublicAPI]
public sealed class InProcessChangeNotifier : IChangeNotifier
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Watcher, byte>> _watchers = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public ValueTask NotifyAsync(string domain, string streamId, CancellationToken ct = default)
    {
        if (_watchers.TryGetValue(domain, out var set))
            foreach (var watcher in set.Keys)
                watcher.Post(streamId);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public IChangeWatcher Watch(string domain)
    {
        var set = _watchers.GetOrAdd(domain, _ => new ConcurrentDictionary<Watcher, byte>());
        var watcher = new Watcher(set);
        set[watcher] = 0;
        return watcher;
    }

    // A per-watcher dirty set: repeated changes to the same stream coalesce, but no distinct changed stream is
    // dropped between reads. A key is removed exactly when it is handed out, so a change that lands while the
    // consumer is processing re-adds it and gets handed out again.
    private sealed class Watcher(ConcurrentDictionary<Watcher, byte> owner) : IChangeWatcher
    {
        private readonly Lock _gate = new();
        private readonly HashSet<string> _dirty = new(StringComparer.Ordinal);
        private TaskCompletionSource _signal = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private bool _disposed;

        public void Post(string streamId)
        {
            lock (_gate)
            {
                _dirty.Add(streamId);
                _signal.TrySetResult();
            }
        }

        public async ValueTask<string> WaitAsync(CancellationToken ct = default)
        {
            while (true)
            {
                Task wait;
                lock (_gate)
                {
                    ObjectDisposedException.ThrowIf(_disposed, this);
                    if (_dirty.Count > 0)
                    {
                        var id = _dirty.First();
                        _dirty.Remove(id);
                        return id;
                    }

                    // Nothing pending: reset the signal (if a prior notify already completed it) and wait.
                    if (_signal.Task.IsCompleted)
                        _signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    wait = _signal.Task;
                }

                await wait.WaitAsync(ct).ConfigureAwait(false);
            }
        }

        public void Dispose()
        {
            lock (_gate)
            {
                _disposed = true;
                _signal.TrySetResult();
            }

            owner.TryRemove(this, out _);
        }
    }
}
