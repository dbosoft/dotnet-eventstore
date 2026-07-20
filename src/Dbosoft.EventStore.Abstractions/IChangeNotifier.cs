namespace Dbosoft.EventStore;

/// <summary>
/// A best-effort "a stream changed" signal, so a reader can react immediately instead of waiting out a poll
/// interval — without a message bus. It carries only the id of the changed stream, and makes <b>no</b>
/// delivery or ordering guarantee: the store stays the source of truth and re-reading (or polling) is the
/// backstop, so a missed notification only costs a little latency, never a change. That is exactly why it is
/// not a bus, and why any transport will do.
/// </summary>
/// <remarks>
/// Two shapes of reader use it. A catch-up subscription treats any signal as a wake and re-reads <c>$all</c>
/// from its checkpoint (it can ignore which stream changed). A fold-style read model instead re-derives just
/// the changed stream from the store — for that it needs to know <i>which</i> stream changed, which is why the
/// signal carries the stream id and the watcher does not drop them.
/// Optional and pluggable: the in-process notifier covers a writer and reader in the same process; a
/// cross-process notifier is a thin bridge over whatever ping transport you already run (a bus used only for
/// the ping, Redis pub/sub, Postgres <c>LISTEN/NOTIFY</c>, …).
/// </remarks>
[PublicAPI]
public interface IChangeNotifier
{
    /// <summary>Signals that a stream in a domain changed. Best-effort — a dropped signal is not an error.</summary>
    ValueTask NotifyAsync(string domain, string streamId, CancellationToken ct = default);

    /// <summary>
    /// Watches a domain for changes. The returned watcher yields the id of each changed stream and buffers
    /// changes until read — coalescing repeated changes to the same stream — so no changed stream is missed
    /// between reads. Dispose it to stop watching.
    /// </summary>
    IChangeWatcher Watch(string domain);
}

/// <summary>Yields the id of each changed stream in a watched domain. Dispose to stop watching.</summary>
[PublicAPI]
public interface IChangeWatcher : IDisposable
{
    /// <summary>
    /// Waits for the next changed stream id, or throws <see cref="OperationCanceledException"/> when
    /// <paramref name="ct"/> cancels. A reader that only needs to wake can ignore the returned id.
    /// </summary>
    ValueTask<string> WaitAsync(CancellationToken ct = default);
}
