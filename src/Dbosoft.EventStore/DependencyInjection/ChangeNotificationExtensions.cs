using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Dbosoft.EventStore.DependencyInjection;

/// <summary>Registers a change-notification transport so subscriptions wake on writes instead of only polling.</summary>
[PublicAPI]
public static class ChangeNotificationExtensions
{
    /// <summary>
    /// Registers the in-process <see cref="InProcessChangeNotifier"/> as the <see cref="IChangeNotifier"/>.
    /// A provider that supports it then signals on write, so an in-process subscription wakes immediately.
    /// For cross-process readers, register a bridge over your own ping transport instead.
    /// </summary>
    public static EventStoreBuilder UseInProcessNotifications(this EventStoreBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.TryAddSingleton<IChangeNotifier, InProcessChangeNotifier>();
        return builder;
    }
}
