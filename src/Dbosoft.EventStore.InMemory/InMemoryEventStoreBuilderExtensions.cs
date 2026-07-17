using Dbosoft.EventStore.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Dbosoft.EventStore.InMemory;

/// <summary>Registers the in-memory provider on an <see cref="EventStoreBuilder"/>.</summary>
public static class InMemoryEventStoreBuilderExtensions
{
    /// <summary>
    /// Uses a singleton <see cref="InMemoryEventStore"/> as the store, wiring up its optional
    /// interfaces and its snapshot store. Intended for tests, samples and single-process use.
    /// </summary>
    public static EventStoreBuilder UseInMemory(this EventStoreBuilder builder)
    {
        var services = builder.Services;
        services.TryAddSingleton<InMemoryEventStore>();
        services.TryAddSingleton<IEventStore>(sp => sp.GetRequiredService<InMemoryEventStore>());
        services.TryAddSingleton<IAllStreamReader>(sp => sp.GetRequiredService<InMemoryEventStore>());
        services.TryAddSingleton<ITimeNavigable>(sp => sp.GetRequiredService<InMemoryEventStore>());
        services.TryAddSingleton<ISubscriptionSource>(sp => sp.GetRequiredService<InMemoryEventStore>());
        services.TryAddSingleton<IStreamMaintenance>(sp => sp.GetRequiredService<InMemoryEventStore>());
        services.TryAddSingleton<ISnapshotStore>(sp => sp.GetRequiredService<InMemoryEventStore>().Snapshots);
        return builder;
    }
}
