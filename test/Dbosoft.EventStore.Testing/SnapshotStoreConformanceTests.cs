using AwesomeAssertions;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.Testing;

/// <summary>The contract for a snapshot store. Providers with snapshots derive this suite.</summary>
public abstract class SnapshotStoreConformanceTests
{
    /// <summary>Creates a fresh, empty snapshot store, paired with its event store's capabilities.</summary>
    protected abstract (ISnapshotStore Snapshots, StoreCapabilities Capabilities) CreateSnapshotStore();

    [Fact]
    public async Task Save_and_load_latest_round_trips()
    {
        var (snapshots, _) = CreateSnapshotStore();

        await snapshots.SaveAsync("s1", 5, Event("Snap", "{\"v\":5}").Data);
        var loaded = await snapshots.LoadLatestAsync("s1");

        loaded.Should().NotBeNull();
        loaded.StreamVersion.Should().Be(5);
    }

    [Fact]
    public async Task Load_latest_is_null_when_absent()
    {
        var (snapshots, _) = CreateSnapshotStore();
        (await snapshots.LoadLatestAsync("missing")).Should().BeNull();
    }

    [Fact]
    public async Task Load_at_or_before_returns_the_newest_within_bound()
    {
        var (snapshots, capabilities) = CreateSnapshotStore();
        if (!capabilities.HasFlag(StoreCapabilities.SnapshotHistory))
            return;

        await snapshots.SaveAsync("s1", 3, Event("Snap", "{\"v\":3}").Data);
        await snapshots.SaveAsync("s1", 7, Event("Snap", "{\"v\":7}").Data);

        var loaded = await snapshots.LoadAtOrBeforeAsync("s1", 5);

        loaded.Should().NotBeNull();
        loaded.StreamVersion.Should().Be(3);
    }

    [Fact]
    public async Task Prune_keeps_only_the_newest_snapshots()
    {
        var (snapshots, capabilities) = CreateSnapshotStore();
        if (!capabilities.HasFlag(StoreCapabilities.SnapshotHistory))
            return;

        await snapshots.SaveAsync("s1", 1, Event("Snap").Data);
        await snapshots.SaveAsync("s1", 2, Event("Snap").Data);
        await snapshots.SaveAsync("s1", 3, Event("Snap").Data);

        await snapshots.PruneAsync("s1", 1);

        (await snapshots.LoadAtOrBeforeAsync("s1", 2)).Should().BeNull();
        (await snapshots.LoadLatestAsync("s1"))!.StreamVersion.Should().Be(3);
    }

    [Fact]
    public async Task Prune_with_a_negative_keep_is_rejected()
    {
        var (snapshots, _) = CreateSnapshotStore();
        await snapshots.SaveAsync("s1", 1, Event("Snap").Data);

        // A negative keep is a caller error; it must be refused consistently, never silently wipe.
        var act = () => snapshots.PruneAsync("s1", -1);

        await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
        (await snapshots.LoadLatestAsync("s1")).Should().NotBeNull();
    }
}
