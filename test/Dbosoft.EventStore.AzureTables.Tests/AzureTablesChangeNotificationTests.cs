using Dbosoft.EventStore.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.AzureTables.Tests;

/// <summary>
/// Proves the change signal is wired for the Azure Tables provider, both on the core append path and on a
/// maintenance op — the latter is the regression the review caught: a delete used to signal nothing.
/// </summary>
[UsedImplicitly]
[Collection(AzuriteCollection.Name)]
public sealed class AzureTablesChangeNotificationTests(AzuriteContainerFixture container)
{
    [Fact]
    public async Task Append_and_maintenance_both_signal_the_changed_stream()
    {
        var domain = "d" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services.AddEventStore(es => es
            .UseInProcessNotifications()
            .UseAzureTables(container.ConnectionString)
            .AddDomain(domain, _ => { }));
        await using var sp = services.BuildServiceProvider();
        await sp.GetRequiredService<AzureTablesEventStoreProvider>().EnsureCreatedAsync();

        var notifier = sp.GetRequiredService<IChangeNotifier>();
        var store = sp.GetRequiredKeyedService<IEventStore>(domain);
        using var watch = notifier.Watch(domain);

        await store.AppendToStreamAsync("order-1", ExpectedVersion.NoStream, Events(1));
        Assert.Equal("order-1", await watch.WaitAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(3)));

        // A maintenance op signals the affected stream too — it used to signal nothing on this provider.
        await ((IStreamMaintenance)store).DeleteStreamAsync("order-1", DeleteMode.Soft);
        Assert.Equal("order-1", await watch.WaitAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(3)));
    }
}
