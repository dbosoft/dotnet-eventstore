using Microsoft.EntityFrameworkCore;

namespace Dbosoft.EventStore.EfCore;

/// <summary>
/// The single source of the context every EF Core store runs on. When a unit-of-work frame is ambient it
/// hands out the caller's borrowed context (shared — never disposed here); otherwise a fresh context the
/// caller owns and disposes. Routing all context acquisition through one provider is what keeps enlistment
/// uniform across the event store, the snapshot store and the checkpoint store: no store can accidentally
/// open a second connection that would self-lock against the frame's pending write, or commit/read
/// inconsistently with the frame's transaction.
/// </summary>
internal sealed class EventStoreContextProvider(IDbContextFactory<EventStoreDbContext> factory)
{
    /// <summary>
    /// The ambient frame's context, or null when running autonomously. A frame of a foreign kind is a
    /// misconfiguration (only this provider issues frames), so it is rejected rather than silently ignored —
    /// which would quietly break the atomicity the caller asked for.
    /// </summary>
    public EventStoreDbContext? Enlisted =>
        AmbientEventStoreUnitOfWork.Current switch
        {
            null => null,
            EfCoreEventStoreUnitOfWork efCore => efCore.Context,
            _ => throw new InvalidOperationException(
                "An event store unit of work is active that this EF Core store cannot enlist onto.")
        };

    /// <summary>
    /// Leases the context for one operation: the ambient frame's (shared) when enlisted, else a fresh one.
    /// Always consume with <c>await using</c> — disposing the lease disposes only a fresh context, never the
    /// frame's. Branch on <see cref="ContextLease.Shared"/> only where the enlisted and autonomous control
    /// flow genuinely differ (a retry loop, or buffering a streaming read off the shared connection).
    /// </summary>
    public async ValueTask<ContextLease> LeaseAsync(CancellationToken ct)
    {
        if (Enlisted is { } enlisted)
            return new ContextLease(enlisted, owned: false);
        return new ContextLease(await factory.CreateDbContextAsync(ct).ConfigureAwait(false), owned: true);
    }

    /// <summary>
    /// Leases a context, runs <paramref name="work"/> on it, and releases the lease — for reads and
    /// single-shot writes with no enlisted/autonomous control-flow difference.
    /// </summary>
    public async Task<T> ExecuteAsync<T>(Func<EventStoreDbContext, Task<T>> work, CancellationToken ct)
    {
        await using var lease = await LeaseAsync(ct).ConfigureAwait(false);
        return await work(lease.Context).ConfigureAwait(false);
    }

    /// <summary>Result-less overload of <see cref="ExecuteAsync{T}"/>.</summary>
    public async Task ExecuteAsync(Func<EventStoreDbContext, Task> work, CancellationToken ct)
    {
        await using var lease = await LeaseAsync(ct).ConfigureAwait(false);
        await work(lease.Context).ConfigureAwait(false);
    }
}

/// <summary>
/// Discipline for saving on the frame's shared context: flushes the operation's staged changes into the
/// borrowed transaction, and on failure clears the change tracker before the conflict propagates. The frame
/// is doomed either way and rolls back as a whole — but the failed operation's entities must not linger on
/// the shared context, where a later save (by any store, or a contract-violating caller that swallowed the
/// failure) would re-flush them.
/// </summary>
internal static class SharedContextSaveExtensions
{
    public static async Task SaveEnlistedChangesAsync(this EventStoreDbContext ctx, CancellationToken ct)
    {
        try
        {
            await ctx.SaveChangesAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            ctx.ChangeTracker.Clear();
            throw;
        }
    }
}

/// <summary>
/// A leased context and who owns its lifetime. A fresh (autonomous) context is disposed when the lease is;
/// the frame's shared context is left alone. <see cref="Shared"/> is true only for the frame's context.
/// </summary>
internal readonly struct ContextLease(EventStoreDbContext context, bool owned) : IAsyncDisposable
{
    public EventStoreDbContext Context { get; } = context;

    /// <summary>True when the context is the ambient frame's shared one (not owned by this lease).</summary>
    public bool Shared => !owned;

    public ValueTask DisposeAsync() => owned ? Context.DisposeAsync() : ValueTask.CompletedTask;
}
