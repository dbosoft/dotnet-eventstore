using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.Tests;

public sealed class MaterializerAndCheckpointTests
{
    [Fact]
    public async Task A_read_only_reader_is_keyed_per_domain_and_reads_the_all_stream()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseInMemory().AddDomain("d", d => d.AddEventType<Incremented>()));
        await using var sp = services.BuildServiceProvider();

        var store = sp.GetRequiredKeyedService<IEventStore>("d");
        await store.AppendToStreamAsync("counter-1", ExpectedVersion.NoStream, Events(1));

        // A projection component would inject only this reader — it can catch up without a write handle.
        var reader = sp.GetRequiredKeyedService<IAllStreamReader>("d");
        var count = 0;
        await foreach (var _ in reader.ReadAllAsync(GlobalPosition.Start))
            count++;

        count.Should().Be(1);
        reader.Should().BeSameAs(store); // same singleton; the separation is that the component holds only the reader
    }

    [Fact]
    public async Task Materializer_round_trips_a_typed_event()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseInMemory().AddDomain("d", d => d.AddEventType<Incremented>()));
        await using var sp = services.BuildServiceProvider();
        var materializer = sp.GetRequiredKeyedService<IEventMaterializer>("d");

        var data = materializer.Dematerialize(new Incremented(7));
        var stored = new StoredEvent(
            "counter-1", 1, GlobalPosition.Start, data.EventId, data.EventType,
            data.Data, data.Metadata, data.ContentType, DateTimeOffset.UnixEpoch);

        var restored = materializer.Materialize(stored);

        restored.Should().BeOfType<Incremented>();
        ((Incremented)restored).By.Should().Be(7);
    }

    [Fact]
    public async Task Materializer_reports_unknown_event_types()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseInMemory().AddDomain("d", d => d.AddEventType<Incremented>()));
        await using var sp = services.BuildServiceProvider();
        var materializer = sp.GetRequiredKeyedService<IEventMaterializer>("d");

        var stored = new StoredEvent(
            "s", 1, GlobalPosition.Start, Guid.NewGuid(), "UnregisteredEvent",
            "{}"u8.ToArray(), null, "application/json", DateTimeOffset.UnixEpoch);

        materializer.TryMaterialize(stored, out var evt).Should().BeFalse();
        evt.Should().BeNull();
    }

    [Fact]
    public async Task Materializer_types_system_markers_regardless_of_registration()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseInMemory().AddDomain("d", _ => { }));
        await using var sp = services.BuildServiceProvider();
        var materializer = sp.GetRequiredKeyedService<IEventMaterializer>("d");

        var payload = "{\"streamId\":\"account-1\",\"beforeVersion\":5}"u8.ToArray();
        var stored = new StoredEvent(
            SystemEventTypes.SystemStream, 1, GlobalPosition.Start, Guid.NewGuid(),
            SystemEventTypes.StreamTruncated, payload, null, "application/json", DateTimeOffset.UnixEpoch);

        var marker = materializer.Materialize(stored);

        marker.Should().BeOfType<StreamTruncatedMarker>();
        ((StreamTruncatedMarker)marker).BeforeVersion.Should().Be(5);
    }

    [Fact]
    public async Task Checkpoint_store_round_trips()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseInMemory());
        await using var sp = services.BuildServiceProvider();
        var checkpoints = sp.GetRequiredService<ICheckpointStore>();

        (await checkpoints.GetAsync("relay")).Should().BeNull();
        await checkpoints.SetAsync("relay", new GlobalPosition("0000000000000000042"));

        (await checkpoints.GetAsync("relay"))!.Value.Token.Should().Be("0000000000000000042");
    }
}
