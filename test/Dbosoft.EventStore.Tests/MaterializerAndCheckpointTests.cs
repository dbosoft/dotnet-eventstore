using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Dbosoft.EventStore.Tests;

public sealed class MaterializerAndCheckpointTests
{
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
