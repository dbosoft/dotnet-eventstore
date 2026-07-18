namespace Dbosoft.EventStore;

/// <summary>
/// A store that can append to several streams in one atomic, all-or-nothing operation. Optional — a
/// provider supports it iff it implements this interface; a backend that cannot commit across streams
/// (for example a key/value store with one partition per stream) simply does not. Each
/// <see cref="StreamAppend"/> is checked against its own expected version, and if any check fails the
/// whole operation is rejected with nothing written. Unlike the single-stream append, a version
/// conflict is not retried under a version-agnostic expectation — the caller reloads and retries, the
/// usual optimistic-concurrency flow.
/// </summary>
[PublicAPI]
public interface IMultiStreamAppend
{
    /// <summary>
    /// Atomically appends to every stream in <paramref name="appends"/>, returning one
    /// <see cref="AppendResult"/> per entry in the same order. A stream may appear at most once; an
    /// entry whose events are already stored is treated as an idempotent replay (as for a single append).
    /// </summary>
    /// <exception cref="WrongExpectedVersionException">A stream was not at its expected version.</exception>
    /// <exception cref="StreamDeletedException">A stream has been hard-deleted.</exception>
    /// <exception cref="InvalidOperationException">A stream appears twice, or a batch overlaps stored events.</exception>
    Task<IReadOnlyList<AppendResult>> AppendToStreamsAsync(
        IReadOnlyList<StreamAppend> appends,
        CancellationToken ct = default);
}
