using Dbosoft.EventStore.Testing;
using Xunit;

namespace Dbosoft.EventStore.AzureTables.Tests;

/// <summary>Runs the full event-store conformance suite against the Azure Table Storage provider.</summary>
[UsedImplicitly]
[Collection(AzuriteCollection.Name)]
public sealed class AzureTablesConformanceTests(AzuriteContainerFixture container) : EventStoreConformanceTests, IAsyncLifetime
{
    private AzureTablesTestStore _store = null!;

    public async Task InitializeAsync() => _store = await AzureTablesTestStore.CreateAsync(container.ConnectionString);

    public async Task DisposeAsync() => await _store.DisposeAsync();

    protected override IEventStore CreateStore() => _store.Provider.GetStore(_store.Domain);

    protected override ISnapshotStore? GetSnapshotStore(IEventStore store) => _store.Provider.GetSnapshotStore(_store.Domain);
}

/// <summary>Runs the snapshot conformance suite against the Azure Table Storage provider.</summary>
[UsedImplicitly]
[Collection(AzuriteCollection.Name)]
public sealed class AzureTablesSnapshotConformanceTests(AzuriteContainerFixture container)
    : SnapshotStoreConformanceTests, IAsyncLifetime
{
    private AzureTablesTestStore _store = null!;

    public async Task InitializeAsync() => _store = await AzureTablesTestStore.CreateAsync(container.ConnectionString);

    public async Task DisposeAsync() => await _store.DisposeAsync();

    protected override (ISnapshotStore Snapshots, StoreCapabilities Capabilities) CreateSnapshotStore() =>
        (_store.Provider.GetSnapshotStore(_store.Domain)!, _store.Provider.GetStore(_store.Domain).Capabilities);
}
