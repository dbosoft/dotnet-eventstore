using Dbosoft.EventStore.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

namespace Dbosoft.EventStore.AzureTables.Tests;

/// <summary>
/// A provider over a fresh, uniquely-named domain on the shared Azurite container — one per test, so
/// each test gets its own tables and blob container. Wired exactly as an app would
/// (<c>AddEventStore(es =&gt; es.UseAzureTables(...))</c>) and created up front.
/// </summary>
internal sealed class AzureTablesTestStore : IAsyncDisposable
{
    private ServiceProvider _services = null!;

    public IEventStoreProvider Provider { get; private set; } = null!;

    public string Domain { get; private set; } = null!;

    public static async Task<AzureTablesTestStore> CreateAsync(string connectionString)
    {
        // A unique, table-name-safe domain gives each test its own set of Azure resources.
        var domain = "d" + Guid.NewGuid().ToString("N");

        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseAzureTables(connectionString).AddDomain(domain, _ => { }));

        var store = new AzureTablesTestStore { _services = services.BuildServiceProvider(), Domain = domain };
        var provider = store._services.GetRequiredService<AzureTablesEventStoreProvider>();
        await provider.EnsureCreatedAsync();
        store.Provider = provider;
        return store;
    }

    public async ValueTask DisposeAsync() => await _services.DisposeAsync();
}
