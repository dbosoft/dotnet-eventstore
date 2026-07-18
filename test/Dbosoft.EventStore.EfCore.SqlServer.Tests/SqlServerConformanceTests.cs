using Dbosoft.EventStore.Testing;
using Xunit;

namespace Dbosoft.EventStore.EfCore.SqlServer.Tests;

/// <summary>Runs the full event-store conformance suite against the EF Core provider on SQL Server.</summary>
[UsedImplicitly]
[Collection(SqlServerCollection.Name)]
public sealed class SqlServerConformanceTests(SqlServerContainerFixture container) : EventStoreConformanceTests, IAsyncLifetime
{
    private SqlServerTestStore _store = null!;

    public async Task InitializeAsync() => _store = await SqlServerTestStore.CreateAsync(container.ConnectionString);

    public async Task DisposeAsync() => await _store.DisposeAsync();

    protected override IEventStore CreateStore() => _store.Provider.GetStore("test");

    protected override ISnapshotStore? GetSnapshotStore(IEventStore store) => _store.Provider.GetSnapshotStore("test");
}

/// <summary>Runs the snapshot conformance suite against the EF Core provider on SQL Server.</summary>
[UsedImplicitly]
[Collection(SqlServerCollection.Name)]
public sealed class SqlServerSnapshotConformanceTests(SqlServerContainerFixture container)
    : SnapshotStoreConformanceTests, IAsyncLifetime
{
    private SqlServerTestStore _store = null!;

    public async Task InitializeAsync() => _store = await SqlServerTestStore.CreateAsync(container.ConnectionString);

    public async Task DisposeAsync() => await _store.DisposeAsync();

    protected override (ISnapshotStore Snapshots, StoreCapabilities Capabilities) CreateSnapshotStore() =>
        (_store.Provider.GetSnapshotStore("test")!, _store.Provider.GetStore("test").Capabilities);
}
