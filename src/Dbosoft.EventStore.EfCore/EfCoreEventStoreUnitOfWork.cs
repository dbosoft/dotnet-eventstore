using System.Data.Common;
using Microsoft.EntityFrameworkCore;

namespace Dbosoft.EventStore.EfCore;

/// <summary>
/// The EF Core unit-of-work frame. It borrows the caller's open <see cref="DbConnection"/> and
/// <see cref="DbTransaction"/> and exposes a single <see cref="EventStoreDbContext"/> enlisted onto that
/// transaction, which every event-store write under the frame shares. Committing commits the borrowed
/// transaction and then runs the deferred change signals; the connection stays the caller's to close.
/// </summary>
internal sealed class EfCoreEventStoreUnitOfWork(
    EnlistedContextFactory contextFactory,
    DbConnection connection,
    DbTransaction transaction) : IEventStoreUnitOfWork
{
    private readonly List<Func<CancellationToken, ValueTask>> _onCommitted = [];
    private EventStoreDbContext? _context;

    /// <inheritdoc />
    public bool IsActive { get; private set; } = true;

    /// <summary>
    /// The context every enlisted write shares, created on first use and bound to the borrowed transaction.
    /// One context per frame is enough: all domains map to the same model, differing only by a column.
    /// </summary>
    public EventStoreDbContext Context
    {
        get
        {
            ObjectDisposedException.ThrowIf(!IsActive, this);
            if (_context is not null)
                return _context;

            _context = contextFactory.Create(connection);
            _context.Database.UseTransaction(transaction);
            return _context;
        }
    }

    /// <inheritdoc />
    public void OnCommitted(Func<CancellationToken, ValueTask> callback)
    {
        ObjectDisposedException.ThrowIf(!IsActive, this);
        _onCommitted.Add(callback);
    }

    /// <inheritdoc />
    public async ValueTask PrepareAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(!IsActive, this);
        if (_context is not null)
            await _context.AssignCommitSequenceAsync(ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(!IsActive, this);
        await PrepareAsync(ct).ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
        await CompleteAsync(ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask CompleteAsync(CancellationToken ct = default)
    {
        if (!IsActive)
            return;

        // Events committed without a sequence never show in $all; the owner skipped PrepareAsync.
        var unprepared = _context?.Unsequenced.Count ?? 0;

        // The frame is done the moment the transaction has committed: drop the ambient slot before running
        // the signals, so a signal that itself appends does so on its own, not against this spent transaction.
        Deactivate();

        foreach (var callback in _onCommitted)
        {
            try
            {
                await callback(ct).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Post-commit signalling is best-effort — a failed notify only costs a reader its poll tick,
                // and must never turn an already-committed unit of work into a fault.
            }
        }

        _onCommitted.Clear();

        if (unprepared > 0)
            throw new InvalidOperationException(
                $"The transaction was committed with {unprepared} event(s) that were never prepared for it; they are "
                + "stored but not in $all. Call PrepareAsync on the unit of work right before committing its transaction.");
    }

    /// <inheritdoc />
    public async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        if (!IsActive)
            return;

        _onCommitted.Clear();
        try
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            Deactivate();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        // A frame disposed without an explicit commit is an abort; leave the borrowed transaction to the
        // caller (it may still roll it back) but stop being ambient and release our own context. The
        // connection and transaction are the caller's — never closed or disposed here.
        Deactivate();
        if (_context is not null)
        {
            await _context.DisposeAsync().ConfigureAwait(false);
            _context = null;
        }
    }

    private void Deactivate()
    {
        if (!IsActive)
            return;

        // Clear the ambient slot while still active, so Current (which filters on IsActive) still resolves
        // to this frame and the reference check matches. Even if the async-local clear does not flow back to
        // the caller, IsActive going false makes Current report absent — the real leak guard.
        if (ReferenceEquals(AmbientEventStoreUnitOfWork.Current, this))
            AmbientEventStoreUnitOfWork.SetCurrent(null);
        IsActive = false;
    }
}
