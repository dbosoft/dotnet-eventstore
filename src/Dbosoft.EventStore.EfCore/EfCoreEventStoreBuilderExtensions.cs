using System.Data.Common;
using Dbosoft.EventStore.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Dbosoft.EventStore.EfCore;

/// <summary>Registers the EF Core provider on an <see cref="EventStoreBuilder"/>.</summary>
[PublicAPI]
public static class EfCoreEventStoreBuilderExtensions
{
    /// <summary>
    /// Backs the event store with EF Core. You steer the whole registration: <paramref name="configureDbContext"/>
    /// picks the database provider, connection and options, and the optional <paramref name="configureModel"/>
    /// applies provider-specific model tweaks (concurrency token, converters — see <see cref="EventStoreModelBuilderExtensions"/>)
    /// that stay out of the provider-agnostic context. The context is never held globally — it is created per
    /// operation from an <see cref="IDbContextFactory{TContext}"/>, so the store is safe to share.
    /// </summary>
    /// <param name="builder">The event store builder being configured.</param>
    /// <param name="configureDbContext">Picks the database provider, connection, and context options.</param>
    /// <param name="configureModel">Optional provider-specific model tweaks (concurrency token, converters).</param>
    /// <param name="configureEnlistedDbContext">
    /// Optional: binds the same database dialect to a caller-supplied, already-open <see cref="DbConnection"/>,
    /// enabling transactional enlistment (<see cref="IEventStoreUnitOfWorkFactory"/>). The <c>UseXxx</c> dialect
    /// wrappers supply it; a bare <c>UseEfCore</c> caller passes one to opt in. It must force a non-retrying
    /// execution strategy, since a retrying strategy refuses the user-initiated transaction the frame borrows.
    /// When null, no unit-of-work factory is registered and enlistment is unavailable.
    /// </param>
    public static EventStoreBuilder UseEfCore(
        this EventStoreBuilder builder,
        Action<DbContextOptionsBuilder> configureDbContext,
        Action<ModelBuilder>? configureModel = null,
        Action<DbContextOptionsBuilder, DbConnection>? configureEnlistedDbContext = null)
    {
        ArgumentNullException.ThrowIfNull(configureDbContext);

        var optionsBuilder = new DbContextOptionsBuilder<EventStoreDbContext>();
        configureDbContext(optionsBuilder);

        builder.Services.TryAddSingleton<IDbContextFactory<EventStoreDbContext>>(
            new EventStoreDbContextFactory(optionsBuilder.Options, configureModel));
        builder.Services.TryAddSingleton(TimeProvider.System);
        builder.Services.TryAddSingleton<IEventStoreProvider>(sp => new EfCoreEventStoreProvider(
            sp.GetRequiredService<IDbContextFactory<EventStoreDbContext>>(),
            sp.GetServices<RegisteredDomain>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetService<IChangeNotifier>())); // optional — only wired when notifications are registered
        builder.Services.TryAddSingleton<ICheckpointStore>(sp => new EfCoreCheckpointStore(
            new EventStoreContextProvider(sp.GetRequiredService<IDbContextFactory<EventStoreDbContext>>())));

        // Enlistment is opt-in: only when the registration can rebind its dialect onto a borrowed connection
        // do we expose a unit-of-work factory. Its presence in the container is the "can enlist" capability.
        if (configureEnlistedDbContext is not null)
        {
            builder.Services.TryAddSingleton(new EnlistedContextFactory(configureEnlistedDbContext, configureModel));
            builder.Services.TryAddSingleton<IEventStoreUnitOfWorkFactory>(sp =>
                new EventStoreUnitOfWorkFactory(sp.GetRequiredService<EnlistedContextFactory>()));
        }

        return builder;
    }
}
