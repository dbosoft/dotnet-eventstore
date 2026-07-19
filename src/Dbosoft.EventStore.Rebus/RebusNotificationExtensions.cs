using Dbosoft.EventStore.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Rebus.Bus;
using Rebus.Handlers;

namespace Dbosoft.EventStore.Rebus;

/// <summary>Registers the Rebus change-notification bridge so subscriptions wake on writes across processes.</summary>
[PublicAPI]
public static class RebusNotificationExtensions
{
    /// <summary>
    /// Registers <see cref="RebusChangeNotifier"/> as the <see cref="IChangeNotifier"/> and its message
    /// handler. The app owns the Rebus setup (transport, and a container-resolved handler activator such as
    /// Rebus.ServiceProvider's <c>AddRebus</c>); a process that runs a subscription over an out-of-process
    /// store must also call <see cref="SubscribeToEventStoreChangesAsync"/> once at startup so the ping is
    /// delivered to it.
    /// </summary>
    public static EventStoreBuilder UseRebusNotifications(this EventStoreBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.TryAddSingleton<RebusChangeNotifier>();
        builder.Services.TryAddSingleton<IChangeNotifier>(sp => sp.GetRequiredService<RebusChangeNotifier>());
        builder.Services.AddTransient<IHandleMessages<EventStoreChanged>, EventStoreChangedHandler>();
        return builder;
    }

    /// <summary>
    /// Subscribes the bus to <see cref="EventStoreChanged"/> pings — call once at startup on any process that
    /// runs a subscription over an out-of-process store, so its <see cref="EventStoreChangedHandler"/>
    /// receives them.
    /// </summary>
    public static Task SubscribeToEventStoreChangesAsync(this IBus bus)
    {
        ArgumentNullException.ThrowIfNull(bus);
        return bus.Subscribe<EventStoreChanged>();
    }
}
