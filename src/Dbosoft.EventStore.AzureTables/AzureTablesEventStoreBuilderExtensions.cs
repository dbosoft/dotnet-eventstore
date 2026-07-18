using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Dbosoft.EventStore.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Dbosoft.EventStore.AzureTables;

/// <summary>Registers the Azure Table Storage provider on an <see cref="EventStoreBuilder"/>.</summary>
[PublicAPI]
public static class AzureTablesEventStoreBuilderExtensions
{
    /// <summary>
    /// Backs the event store with Azure Table Storage (plus blobs for large payloads), reached through
    /// one connection string. Each domain gets its own tables and blob container, optionally sharing a
    /// <paramref name="tablePrefix"/> so several stores can live in one storage account. Call
    /// <see cref="AzureTablesEventStoreProvider.EnsureCreatedAsync"/> once at startup to create them.
    /// </summary>
    public static EventStoreBuilder UseAzureTables(
        this EventStoreBuilder builder,
        string connectionString,
        string? tablePrefix = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(connectionString);
        var prefix = tablePrefix ?? "";
        var tables = new TableServiceClient(connectionString);
        var blobs = new BlobServiceClient(connectionString);

        builder.Services.TryAddSingleton(TimeProvider.System);
        builder.Services.TryAddSingleton(sp => new AzureTablesEventStoreProvider(
            tables, blobs, sp.GetServices<RegisteredDomain>(), sp.GetRequiredService<TimeProvider>(), prefix));
        builder.Services.TryAddSingleton<IEventStoreProvider>(sp => sp.GetRequiredService<AzureTablesEventStoreProvider>());
        builder.Services.TryAddSingleton(sp => sp.GetRequiredService<AzureTablesEventStoreProvider>().Checkpoints);
        return builder;
    }
}
