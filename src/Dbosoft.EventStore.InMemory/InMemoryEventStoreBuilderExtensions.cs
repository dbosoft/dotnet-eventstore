using Dbosoft.EventStore.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Dbosoft.EventStore.InMemory;

/// <summary>Registers the in-memory provider on an <see cref="EventStoreBuilder"/>.</summary>
[PublicAPI]
public static class InMemoryEventStoreBuilderExtensions
{
    /// <summary>
    /// Backs the event store with in-memory storage: each domain gets its own
    /// <see cref="InMemoryEventStore"/>. Intended for tests, samples and single-process use.
    /// </summary>
    public static EventStoreBuilder UseInMemory(this EventStoreBuilder builder)
    {
        builder.Services.TryAddSingleton(TimeProvider.System);
        builder.Services.TryAddSingleton<IEventStoreProvider>(sp => new InMemoryEventStoreProvider(
            sp.GetRequiredService<IEnumerable<RegisteredDomain>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetService<IChangeNotifier>())); // optional — only wired when UseInProcessNotifications (or a bridge) registered one
        builder.Services.TryAddSingleton<ICheckpointStore, InMemoryCheckpointStore>();
        return builder;
    }
}
