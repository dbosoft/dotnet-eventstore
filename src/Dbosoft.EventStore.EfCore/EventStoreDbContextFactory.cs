using Microsoft.EntityFrameworkCore;

namespace Dbosoft.EventStore.EfCore;

/// <summary>
/// Creates a fresh <see cref="EventStoreDbContext"/> per call, carrying the registration's model hook.
/// Nothing is cached or shared, so the store never holds a global context.
/// </summary>
internal sealed class EventStoreDbContextFactory(
    DbContextOptions<EventStoreDbContext> options,
    Action<ModelBuilder>? configureModel) : IDbContextFactory<EventStoreDbContext>
{
    public EventStoreDbContext CreateDbContext() => new(options, configureModel);
}
