using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Dbosoft.EventStore.InMemory;

/// <summary>Registers the in-memory provider with dependency injection.</summary>
[PublicAPI]
public static class InMemoryServiceCollectionExtensions
{
    /// <summary>
    /// Registers a singleton <see cref="InMemoryEventStore"/> as the <see cref="IEventStore"/>, its
    /// optional interfaces and its snapshot store. Intended for tests, samples and single-process use.
    /// </summary>
    public static IServiceCollection AddInMemoryEventStore(this IServiceCollection services)
    {
        services.TryAddSingleton<InMemoryEventStore>();
        services.TryAddSingleton<IEventStore>(sp => sp.GetRequiredService<InMemoryEventStore>());
        services.TryAddSingleton<IAllStreamReader>(sp => sp.GetRequiredService<InMemoryEventStore>());
        services.TryAddSingleton<ITimeNavigable>(sp => sp.GetRequiredService<InMemoryEventStore>());
        services.TryAddSingleton<ISubscriptionSource>(sp => sp.GetRequiredService<InMemoryEventStore>());
        services.TryAddSingleton<IStreamMaintenance>(sp => sp.GetRequiredService<InMemoryEventStore>());
        services.TryAddSingleton<ISnapshotStore>(sp => sp.GetRequiredService<InMemoryEventStore>().Snapshots);
        return services;
    }
}
