using Dbosoft.EventStore.Testing;
using Xunit;

namespace Dbosoft.EventStore.EfCore.PostgreSql.Tests;

/// <summary>Runs the full event-store conformance suite against the EF Core provider on PostgreSQL.</summary>
[UsedImplicitly]
[Collection(PostgreSqlCollection.Name)]
public sealed class PostgreSqlConformanceTests(PostgreSqlContainerFixture container)
    : EventStoreConformanceTests, IAsyncLifetime
{
    private PostgreSqlTestStore _store = null!;

    public async Task InitializeAsync() => _store = await PostgreSqlTestStore.CreateAsync(container.ConnectionString);

    public async Task DisposeAsync() => await _store.DisposeAsync();

    protected override IEventStore CreateStore() => _store.Provider.GetStore("test");

    protected override ISnapshotStore? GetSnapshotStore(IEventStore store) => _store.Provider.GetSnapshotStore("test");
}

/// <summary>Runs the snapshot conformance suite against the EF Core provider on PostgreSQL.</summary>
[UsedImplicitly]
[Collection(PostgreSqlCollection.Name)]
public sealed class PostgreSqlSnapshotConformanceTests(PostgreSqlContainerFixture container)
    : SnapshotStoreConformanceTests, IAsyncLifetime
{
    private PostgreSqlTestStore _store = null!;

    public async Task InitializeAsync() => _store = await PostgreSqlTestStore.CreateAsync(container.ConnectionString);

    public async Task DisposeAsync() => await _store.DisposeAsync();

    protected override (ISnapshotStore Snapshots, StoreCapabilities Capabilities) CreateSnapshotStore() =>
        (_store.Provider.GetSnapshotStore("test")!, _store.Provider.GetStore("test").Capabilities);
}
