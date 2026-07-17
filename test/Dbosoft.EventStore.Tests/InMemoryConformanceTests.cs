using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Testing;

namespace Dbosoft.EventStore.Tests;

/// <summary>Runs the full event-store conformance suite against the in-memory reference provider.</summary>
public sealed class InMemoryConformanceTests : EventStoreConformanceTests
{
    protected override IEventStore CreateStore() => new InMemoryEventStore();

    protected override ISnapshotStore GetSnapshotStore(IEventStore store) =>
        ((InMemoryEventStore)store).Snapshots;
}

/// <summary>Runs the snapshot conformance suite against the in-memory reference provider.</summary>
public sealed class InMemorySnapshotConformanceTests : SnapshotStoreConformanceTests
{
    protected override (ISnapshotStore Snapshots, StoreCapabilities Capabilities) CreateSnapshotStore()
    {
        var store = new InMemoryEventStore();
        return (store.Snapshots, store.Capabilities);
    }
}
