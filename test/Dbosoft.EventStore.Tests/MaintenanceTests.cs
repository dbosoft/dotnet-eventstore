using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.Tests;

public sealed class MaintenanceTests
{
    [Fact]
    public async Task In_band_domain_writes_the_marker_on_the_affected_stream()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseInMemory().AddDomain("logs", d => d.EmitMarkersInBand()));
        await using var sp = services.BuildServiceProvider();
        var provider = sp.GetRequiredService<IEventStoreProvider>();
        var store = provider.GetStore("logs");
        var maintenance = (IStreamMaintenance)store;

        await store.AppendToStreamAsync("log-1", ExpectedVersion.NoStream, Events(3));
        await provider.GetSnapshotStore("logs")!.SaveAsync("log-1", 2, "{}"u8.ToArray());
        await maintenance.TruncateStreamAsync("log-1", 2);

        // In-band: reading the stream itself shows the truncation marker.
        var streamEvents = await store.ReadAllOf("log-1");
        streamEvents.Should().Contain(e => e.EventType == SystemEventTypes.StreamTruncated);
    }

    [Fact]
    public async Task Default_domain_keeps_the_marker_off_the_stream()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseInMemory().AddDomain("acct", _ => { }));
        await using var sp = services.BuildServiceProvider();
        var provider = sp.GetRequiredService<IEventStoreProvider>();
        var store = provider.GetStore("acct");

        await store.AppendToStreamAsync("a-1", ExpectedVersion.NoStream, Events(3));
        await provider.GetSnapshotStore("acct")!.SaveAsync("a-1", 2, "{}"u8.ToArray());
        await ((IStreamMaintenance)store).TruncateStreamAsync("a-1", 2);

        // Default: the aggregate stream stays pure; the marker lives in $system, visible only in $all.
        (await store.ReadAllOf("a-1")).Should().NotContain(e => e.EventType == SystemEventTypes.StreamTruncated);
    }

    [Fact]
    public async Task Expired_system_tombstones_are_scavenged()
    {
        var clock = new TestClock(DateTimeOffset.UnixEpoch);
        var store = new InMemoryEventStore(clock, tombstoneRetention: TimeSpan.FromDays(7));

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));
        await store.Snapshots.SaveAsync("s1", 2, "{}"u8.ToArray());
        await store.TruncateStreamAsync("s1", 2);
        (await ReadAll(store)).Should().Contain(e => e.EventType == SystemEventTypes.StreamTruncated);

        clock.Advance(TimeSpan.FromDays(8));
        var report = await store.ScavengeAsync();

        report.EventsRemoved.Should().BeGreaterThan(0);
        (await ReadAll(store)).Should().NotContain(e => e.EventType == SystemEventTypes.StreamTruncated);
    }

    private static async Task<List<StoredEvent>> ReadAll(IAllStreamReader reader)
    {
        var result = new List<StoredEvent>();
        await foreach (var e in reader.ReadAllAsync(GlobalPosition.Start))
            result.Add(e);
        return result;
    }
}

file sealed class TestClock(DateTimeOffset start) : TimeProvider
{
    private DateTimeOffset _now = start;

    public override DateTimeOffset GetUtcNow() => _now;

    public void Advance(TimeSpan by) => _now += by;
}
