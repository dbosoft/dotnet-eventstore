using Testcontainers.Azurite;
using Xunit;

namespace Dbosoft.EventStore.AzureTables.Tests;

/// <summary>
/// Starts one Azurite (Azure Storage emulator) container for the whole collection. Each test gets its
/// own domain — and therefore its own tables and blob container — inside it, so tests are isolated.
/// </summary>
[UsedImplicitly]
public sealed class AzuriteContainerFixture : IAsyncLifetime
{
    // A current Azurite image: the one Testcontainers pins by default is too old for the API version
    // the Azure Storage SDK sends.
    private readonly AzuriteContainer _container =
        new AzuriteBuilder("mcr.microsoft.com/azure-storage/azurite:latest").Build();

    public string ConnectionString => _container.GetConnectionString();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

/// <summary>Binds the Azure Tables test classes to one shared Azurite container.</summary>
[CollectionDefinition(Name)]
public sealed class AzuriteCollection : ICollectionFixture<AzuriteContainerFixture>
{
    public const string Name = "Azurite";
}
