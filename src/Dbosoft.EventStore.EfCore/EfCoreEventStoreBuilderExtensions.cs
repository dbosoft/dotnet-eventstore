using Dbosoft.EventStore.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Dbosoft.EventStore.EfCore;

/// <summary>Registers the EF Core provider on an <see cref="EventStoreBuilder"/>.</summary>
[PublicAPI]
public static class EfCoreEventStoreBuilderExtensions
{
    /// <summary>
    /// Backs the event store with EF Core. You steer the whole registration: <paramref name="configureDbContext"/>
    /// picks the database provider, connection and options, and the optional <paramref name="configureModel"/>
    /// applies provider-specific model tweaks (concurrency token, converters — see <see cref="EventStoreModel"/>)
    /// that stay out of the provider-agnostic context. The context is never held globally — it is created per
    /// operation from an <see cref="IDbContextFactory{TContext}"/>, so the store is safe to share.
    /// </summary>
    public static EventStoreBuilder UseEfCore(
        this EventStoreBuilder builder,
        Action<DbContextOptionsBuilder> configureDbContext,
        Action<ModelBuilder>? configureModel = null)
    {
        ArgumentNullException.ThrowIfNull(configureDbContext);

        var optionsBuilder = new DbContextOptionsBuilder<EventStoreDbContext>();
        configureDbContext(optionsBuilder);

        builder.Services.TryAddSingleton<IDbContextFactory<EventStoreDbContext>>(
            new EventStoreDbContextFactory(optionsBuilder.Options, configureModel));
        builder.Services.TryAddSingleton(TimeProvider.System);
        builder.Services.TryAddSingleton<IEventStoreProvider, EfCoreEventStoreProvider>();
        builder.Services.TryAddSingleton<ICheckpointStore, EfCoreCheckpointStore>();
        return builder;
    }
}
