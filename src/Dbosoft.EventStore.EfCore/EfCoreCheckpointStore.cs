using Microsoft.EntityFrameworkCore;

namespace Dbosoft.EventStore.EfCore;

/// <summary>Durable checkpoint store backed by EF Core. Opens a context per operation.</summary>
internal sealed class EfCoreCheckpointStore(IDbContextFactory<EventStoreDbContext> factory) : ICheckpointStore
{
    public async Task<GlobalPosition?> GetAsync(string name, CancellationToken ct = default)
    {
        await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
        var token = await ctx.Checkpoints.AsNoTracking()
            .Where(c => c.Name == name)
            .Select(c => c.Token)
            .FirstOrDefaultAsync(ct).ConfigureAwait(false);

        return token is null ? null : new GlobalPosition(token);
    }

    public async Task SetAsync(string name, GlobalPosition position, CancellationToken ct = default)
    {
        // Find-then-upsert. If two consumers first-write the same checkpoint concurrently the loser's
        // insert violates the primary key; retry, where it now finds the row and updates it (last wins).
        const int maxAttempts = 3;
        for (var attempt = 1; ; attempt++)
        {
            await using var ctx = await factory.CreateDbContextAsync(ct).ConfigureAwait(false);
            var existing = await ctx.Checkpoints.FindAsync([name], ct).ConfigureAwait(false);
            if (existing is null)
                ctx.Checkpoints.Add(new CheckpointRow { Name = name, Token = position.Token });
            else
                existing.Token = position.Token;

            try
            {
                await ctx.SaveChangesAsync(ct).ConfigureAwait(false);
                return;
            }
            catch (DbUpdateException) when (attempt < maxAttempts)
            {
                // A concurrent writer inserted the row first — retry as an update.
            }
        }
    }
}
