namespace Dbosoft.EventStore;

/// <summary>
/// An ambient transaction frame that a caller opens so that its own writes and the event store's appends
/// commit as one unit. The frame is <em>borrowed</em>: the caller owns a database transaction and hands it
/// in (see <see cref="IEventStoreUnitOfWorkFactory"/>); while the frame is the ambient one
/// (<see cref="AmbientEventStoreUnitOfWork.Current"/>) a supporting store enlists its writes onto that
/// transaction instead of opening its own. The caller ends the frame with <see cref="CommitAsync"/> or
/// <see cref="RollbackAsync"/> — nothing the store wrote is durable until then.
/// </summary>
/// <remarks>
/// Only a provider that can enlist implements the backing store side (see <see cref="IEnlistableStore"/>);
/// the frame itself is provider-agnostic. Change notifications a store would fire on write are deferred
/// through <see cref="OnCommitted"/> and only run after a successful commit, so a reader never wakes on
/// events that a rollback then discards. A frame is single-threaded: the borrowed connection carries one
/// database operation at a time, so no two enlisted writes may run concurrently within it.
/// </remarks>
[PublicAPI]
public interface IEventStoreUnitOfWork : IAsyncDisposable
{
    /// <summary>
    /// Whether the frame is still open. Turns false once it has committed, rolled back, or been disposed —
    /// at which point <see cref="AmbientEventStoreUnitOfWork.Current"/> stops reporting it, so a later write
    /// on the same async flow runs on its own again rather than against a spent transaction.
    /// </summary>
    bool IsActive { get; }

    /// <summary>
    /// Registers a callback to run after the frame commits — used to defer a store's best-effort change
    /// signal until the write is durable. Callbacks run in registration order after <see cref="CommitAsync"/>
    /// succeeds and never on rollback. A throwing callback is swallowed: post-commit signalling is
    /// best-effort and must not turn a committed unit of work into a failure.
    /// </summary>
    void OnCommitted(Func<CancellationToken, ValueTask> callback);

    /// <summary>
    /// Commits the borrowed transaction, making every enlisted write durable together, then runs the
    /// <see cref="OnCommitted"/> callbacks. Use when the frame owns the commit — a standalone unit of work.
    /// When the transaction is committed by its owner instead (for example a Rebus message unit of work),
    /// leave the commit to the owner and call <see cref="CompleteAsync"/>. After this the frame is no longer
    /// active.
    /// </summary>
    ValueTask CommitAsync(CancellationToken ct = default);

    /// <summary>
    /// Runs the deferred <see cref="OnCommitted"/> callbacks and ends the frame, for when the borrowed
    /// transaction has already been committed by its owner (a Rebus message unit of work, say) rather than by
    /// <see cref="CommitAsync"/>. Does not touch the transaction. After this the frame is no longer active.
    /// </summary>
    ValueTask CompleteAsync(CancellationToken ct = default);

    /// <summary>
    /// Rolls the borrowed transaction back, discarding every enlisted write and every pending
    /// <see cref="OnCommitted"/> callback. After this the frame is no longer active. When the transaction's
    /// owner rolls it back instead, just dispose the frame — disposal discards the callbacks untouched.
    /// </summary>
    ValueTask RollbackAsync(CancellationToken ct = default);
}
