using Microsoft.EntityFrameworkCore;

namespace Dbosoft.EventStore.EfCore;

/// <summary>
/// Durable checkpoint store backed by EF Core. Runs on the caller's borrowed context when a unit-of-work
/// frame is ambient — so advancing a checkpoint commits, or rolls back, with the projection write beside it
/// and shares the one connection — else it opens a context per operation.
/// </summary>
internal sealed class EfCoreCheckpointStore(EventStoreContextProvider context) : ICheckpointStore
{
    public Task<GlobalPosition?> GetAsync(string name, CancellationToken ct = default) =>
        context.ExecuteAsync<GlobalPosition?>(async ctx =>
        {
            var token = await ctx.Checkpoints.AsNoTracking()
                .Where(c => c.Name == name)
                .Select(c => c.Token)
                .FirstOrDefaultAsync(ct).ConfigureAwait(false);

            return token is null ? null : new GlobalPosition(token);
        }, ct);

    public async Task SetAsync(string name, GlobalPosition position, CancellationToken ct = default)
    {
        // Under a frame the checkpoint joins the caller's transaction; a conflict propagates and the whole
        // frame rolls back (no retry against a doomed transaction).
        if (context.Enlisted is { } enlisted)
        {
            await UpsertAsync(enlisted, name, position, ct).ConfigureAwait(false);
            await enlisted.SaveEnlistedChangesAsync(ct).ConfigureAwait(false);
            return;
        }

        // Autonomous find-then-upsert. If two consumers first-write the same checkpoint concurrently the
        // loser's insert violates the primary key; retry, where it now finds the row and updates it (last wins).
        const int maxAttempts = 3;
        for (var attempt = 1; ; attempt++)
        {
            await using var lease = await context.LeaseAsync(ct).ConfigureAwait(false);
            await UpsertAsync(lease.Context, name, position, ct).ConfigureAwait(false);
            try
            {
                await lease.Context.SaveChangesAsync(ct).ConfigureAwait(false);
                return;
            }
            catch (DbUpdateException) when (attempt < maxAttempts)
            {
                // A concurrent writer inserted the row first — retry as an update.
            }
        }
    }

    private static async Task UpsertAsync(
        EventStoreDbContext ctx, string name, GlobalPosition position, CancellationToken ct)
    {
        var existing = await ctx.Checkpoints.FindAsync([name], ct).ConfigureAwait(false);
        if (existing is null)
            ctx.Checkpoints.Add(new CheckpointRow { Name = name, Token = position.Token });
        else
            existing.Token = position.Token;
    }
}
