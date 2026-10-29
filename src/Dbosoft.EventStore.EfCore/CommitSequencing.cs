using System.Data.Common;
using Microsoft.EntityFrameworkCore;

namespace Dbosoft.EventStore.EfCore;

/// <summary>
/// Gives events their place in <c>$all</c> as the last step before their transaction commits.
/// </summary>
/// <remarks>
/// A row id is taken at insert, and an insert can stay uncommitted for as long as its caller's transaction runs —
/// minutes, for an append enlisted in a message that goes on to call other systems. Ordered by row id, a reader
/// would pass an id still in flight and never come back for it. The commit sequence is taken from one counter
/// row whose lock the transaction keeps until it commits, so the next transaction takes its numbers only after
/// this one is visible: sequence order is commit order, and <c>$all</c> read by it has no hole to skip. The lock
/// is held only from this step to the commit, not for the transaction's life. A rollback rolls the counter back
/// with it, so no number is lost.
/// </remarks>
internal static class CommitSequencing
{
    /// <summary>
    /// Takes sequences for the event rows this context inserted under a caller's transaction and writes them
    /// into it. Call it last, right before the commit: the counter stays locked until then.
    /// </summary>
    public static async Task AssignCommitSequenceAsync(this EventStoreDbContext ctx, CancellationToken ct)
    {
        var rows = ctx.Unsequenced.OrderBy(r => r.Position).ToList();
        if (rows.Count == 0)
            return;

        var next = await ReserveAsync(ctx, rows.Count, ct).ConfigureAwait(false);
        foreach (var row in rows)
        {
            var sequence = next++;
            var position = row.Position;
            await ctx.Events
                .Where(e => e.Position == position)
                .ExecuteUpdateAsync(s => s.SetProperty(x => x.CommitSequence, sequence), ct)
                .ConfigureAwait(false);
            row.CommitSequence = sequence;
        }

        ctx.ForgetUnsequenced();
    }

    /// <summary>
    /// Saves the context's changes with sequences on the event rows it inserts, in one transaction: the context's
    /// own when it has one (the caller commits it right after), else a new one committed here. The sequences are
    /// taken just before the insert, so the counter is held only for the insert and the commit. Under a retrying
    /// execution strategy the whole of it is retried as a unit and takes its sequences anew.
    /// </summary>
    public static async Task SaveSequencedAsync(this EventStoreDbContext ctx, CancellationToken ct)
    {
        if (ctx.Database.CurrentTransaction is not null)
        {
            await SequenceInsertsAsync(ctx, ct).ConfigureAwait(false);
            await ctx.SaveChangesAsync(ct).ConfigureAwait(false);
            return;
        }

        var strategy = ctx.Database.CreateExecutionStrategy();
        await strategy.ExecuteAsync(async token =>
        {
            await using var tx = await ctx.Database.BeginTransactionAsync(token).ConfigureAwait(false);
            await SequenceInsertsAsync(ctx, token).ConfigureAwait(false);
            await ctx.SaveChangesAsync(acceptAllChangesOnSuccess: false, token).ConfigureAwait(false);
            await tx.CommitAsync(token).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
        ctx.ChangeTracker.AcceptAllChanges();
    }

    // Every event row still to be inserted takes a sequence, in the order it was added; one that holds a
    // sequence from an attempt that rolled back takes a new one.
    private static async Task SequenceInsertsAsync(EventStoreDbContext ctx, CancellationToken ct)
    {
        var rows = ctx.ChangeTracker.Entries<EventRow>()
            .Where(e => e.State == EntityState.Added)
            .Select(e => e.Entity)
            .ToList();
        if (rows.Count == 0)
            return;

        var next = await ReserveAsync(ctx, rows.Count, ct).ConfigureAwait(false);
        foreach (var row in rows)
            row.CommitSequence = next++;
    }

    // Moves the counter on by count and returns the first sequence of the reserved run. The update locks the
    // counter row until the transaction ends.
    private static async Task<long> ReserveAsync(EventStoreDbContext ctx, int count, CancellationToken ct)
    {
        try
        {
            await ctx.Sequence
                .Where(s => s.Id == EventStoreDbContext.SequenceId)
                .ExecuteUpdateAsync(s => s.SetProperty(x => x.Last, x => x.Last + count), ct)
                .ConfigureAwait(false);
            var last = await ctx.Sequence.AsNoTracking()
                .Where(s => s.Id == EventStoreDbContext.SequenceId)
                .Select(s => s.Last)
                .SingleAsync(ct).ConfigureAwait(false);
            return last - count + 1;
        }
        catch (DbException ex)
        {
            // Taking the counter is part of the save: a lock conflict or deadlock on it is reported the way a
            // conflicting save is, so the writers that re-read and retry a failed save do the same here.
            throw new DbUpdateException("Taking the commit sequence for the save failed.", ex);
        }
    }
}
