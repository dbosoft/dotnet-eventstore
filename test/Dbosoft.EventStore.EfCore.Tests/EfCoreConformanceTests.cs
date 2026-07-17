using Dbosoft.EventStore.Testing;

namespace Dbosoft.EventStore.EfCore.Tests;

/// <summary>Runs the full event-store conformance suite against the EF Core provider on SQLite.</summary>
public sealed class EfCoreConformanceTests : EventStoreConformanceTests, IDisposable
{
    private readonly SqliteEventStoreFixture _fixture = new();

    protected override IEventStore CreateStore() => _fixture.Provider.GetStore("test");

    protected override ISnapshotStore? GetSnapshotStore(IEventStore store) =>
        _fixture.Provider.GetSnapshotStore("test");

    public void Dispose() => _fixture.Dispose();
}

/// <summary>Runs the snapshot conformance suite against the EF Core provider on SQLite.</summary>
public sealed class EfCoreSnapshotConformanceTests : SnapshotStoreConformanceTests, IDisposable
{
    private readonly SqliteEventStoreFixture _fixture = new();

    protected override (ISnapshotStore Snapshots, StoreCapabilities Capabilities) CreateSnapshotStore() =>
        (_fixture.Provider.GetSnapshotStore("test")!, _fixture.Provider.GetStore("test").Capabilities);

    public void Dispose() => _fixture.Dispose();
}
