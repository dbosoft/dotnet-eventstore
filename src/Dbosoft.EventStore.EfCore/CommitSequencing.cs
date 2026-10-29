using System.Data.Common;
using System.Linq.Expressions;
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
/// this one is visible: sequence order is commit order, and <c>$all</c> read by it has no hole to skip.
/// <para>
/// The counter is always the last lock a transaction takes, after its inserts and stream heads: a transaction
/// holding the counter then waits on nothing but its own commit, so no two writers can each hold what the other
/// waits for. A rollback rolls the counter back with it, so no number is lost.
/// </para>
/// </remarks>
internal static class CommitSequencing
{
    // Rows stamped per statement: one statement per event would hold the counter for a round trip each.
    private const int StampBatch = 64;

    /// <summary>
    /// Takes sequences for the event rows this context has inserted and not yet sequenced, and writes them into
    /// the context's current transaction. Call it last, right before the commit: the counter stays locked until
    /// then, and every other committing writer waits for it.
    /// </summary>
    public static async Task AssignCommitSequenceAsync(this EventStoreDbContext ctx, CancellationToken ct)
    {
        var rows = ctx.Unsequenced;
        if (rows.Count == 0)
            return;

        var first = await ReserveAsync(ctx, rows.Count, ct).ConfigureAwait(false);
        var stamped = rows.Select((row, i) => (Row: row, Sequence: first + i)).ToList();
        foreach (var batch in stamped.Chunk(StampBatch))
            await StampAsync(ctx, batch, ct).ConfigureAwait(false);
        foreach (var (row, sequence) in stamped)
            row.CommitSequence = sequence;

        ctx.ForgetUnsequenced();
    }

    /// <summary>
    /// Saves the context's changes and sequences the event rows they insert, in one transaction: the context's
    /// own when it has one (the caller commits it right after), else a new one committed here. Under a retrying
    /// execution strategy the whole of it is retried as a unit and takes its sequences anew.
    /// </summary>
    public static async Task SaveSequencedAsync(this EventStoreDbContext ctx, CancellationToken ct)
    {
        if (ctx.Database.CurrentTransaction is not null)
        {
            await ctx.SaveChangesAsync(ct).ConfigureAwait(false);
            await ctx.AssignCommitSequenceAsync(ct).ConfigureAwait(false);
            return;
        }

        var strategy = ctx.Database.CreateExecutionStrategy();
        await strategy.ExecuteAsync(async token =>
        {
            await using var tx = await ctx.Database.BeginTransactionAsync(token).ConfigureAwait(false);
            await ctx.SaveChangesAsync(acceptAllChangesOnSuccess: false, token).ConfigureAwait(false);
            await ctx.AssignCommitSequenceAsync(token).ConfigureAwait(false);
            await tx.CommitAsync(token).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
        ctx.ChangeTracker.AcceptAllChanges();
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

    // Writes the sequences of a batch in one statement. The statement must find its rows by an index seek: a scan
    // would lock-wait on other transactions' uncommitted rows (SQL Server takes update locks while it reads), and
    // those transactions wait for the counter this one holds. Rows whose insert read back its row id are found by
    // it; rows of an insert whose ids were not read back (a save that defers accepting its changes, so a retry can
    // run it again) are found by stream and event id, on the unique (domain, stream, event id) index.
    private static async Task StampAsync(EventStoreDbContext ctx, (EventRow Row, long Sequence)[] batch, CancellationToken ct)
    {
        if (batch.All(b => b.Row.Position > 0))
        {
            Expression ByPosition(ParameterExpression row, EventRow stamped) =>
                Expression.Equal(Expression.Property(row, nameof(EventRow.Position)), Expression.Constant(stamped.Position));
            await ctx.Events
                .Where(AnyOf(batch, ByPosition))
                .ExecuteUpdateAsync(s => s.SetProperty(e => e.CommitSequence, SequenceOf(batch, ByPosition)), ct)
                .ConfigureAwait(false);
            return;
        }

        foreach (var stream in batch.GroupBy(b => (b.Row.Domain, b.Row.StreamId)))
        {
            var (domain, streamId) = stream.Key;
            var rows = stream.ToArray();
            Expression ByEventId(ParameterExpression row, EventRow stamped) =>
                Expression.Equal(Expression.Property(row, nameof(EventRow.EventId)), Expression.Constant(stamped.EventId));
            await ctx.Events
                .Where(e => e.Domain == domain && e.StreamId == streamId)
                .Where(AnyOf(rows, ByEventId))
                .ExecuteUpdateAsync(s => s.SetProperty(e => e.CommitSequence, SequenceOf(rows, ByEventId)), ct)
                .ConfigureAwait(false);
        }
    }

    // match 1 OR match 2 OR …, as literals: SQL Server resolves a list of literal keys with index seeks, where a
    // parameterized list becomes a join it may answer with a scan.
    private static Expression<Func<EventRow, bool>> AnyOf(
        (EventRow Row, long Sequence)[] rows, Func<ParameterExpression, EventRow, Expression> match)
    {
        var row = Expression.Parameter(typeof(EventRow), "e");
        var any = rows.Select(r => match(row, r.Row)).Aggregate(Expression.OrElse);
        return Expression.Lambda<Func<EventRow, bool>>(any, row);
    }

    // CASE WHEN <match row 1> THEN <sequence 1> WHEN … END, for the rows of one statement.
    private static Expression<Func<EventRow, long?>> SequenceOf(
        (EventRow Row, long Sequence)[] rows, Func<ParameterExpression, EventRow, Expression> match)
    {
        var row = Expression.Parameter(typeof(EventRow), "e");
        Expression value = Expression.Constant(null, typeof(long?));
        foreach (var (stamped, sequence) in rows.Reverse())
            value = Expression.Condition(match(row, stamped), Expression.Constant(sequence, typeof(long?)), value);
        return Expression.Lambda<Func<EventRow, long?>>(value, row);
    }
}
