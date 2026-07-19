using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Rebus.Bus;
using Xunit;

namespace Dbosoft.EventStore.Rebus.Tests;

internal sealed record ThingHappened(int N);

public sealed class RebusEventRelayTests
{
    [Fact]
    public async Task Publishes_each_event_with_ordering_headers_and_resumes_from_the_checkpoint()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseInMemory().AddDomain("d", d => d.AddEventType<ThingHappened>()));
        await using var sp = services.BuildServiceProvider();

        var store = sp.GetRequiredKeyedService<IEventStore>("d");
        var materializer = sp.GetRequiredKeyedService<IEventMaterializer>("d");
        var checkpoints = sp.GetRequiredService<ICheckpointStore>();
        var bus = Substitute.For<IBus>();

        var eventId = Guid.NewGuid();
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream,
            [materializer.Dematerialize(new ThingHappened(7), eventId)]);

        var relay = new RebusEventRelay((IAllStreamReader)store, materializer, checkpoints, bus, "d->rebus");
        var first = await relay.PumpAsync();
        var second = await relay.PumpAsync(); // nothing new — resumes from the checkpoint

        first.Should().Be(1);
        second.Should().Be(0);
        await bus.Received(1).Publish(
            Arg.Is<object>(m => m is ThingHappened && ((ThingHappened)m).N == 7),
            Arg.Is<IDictionary<string, string>>(h =>
                h != null
                && h[EventStoreHeaders.StreamId] == "s1"
                && h[EventStoreHeaders.StreamVersion] == "1"
                && h[EventStoreHeaders.EventId] == eventId.ToString("N")
                && h[EventStoreHeaders.EventType] == "ThingHappened"));
    }

    [Fact]
    public async Task Does_not_forward_system_markers_when_disabled()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseInMemory().AddDomain("d", d => d.AddEventType<ThingHappened>()));
        await using var sp = services.BuildServiceProvider();

        var store = sp.GetRequiredKeyedService<IEventStore>("d");
        var materializer = sp.GetRequiredKeyedService<IEventMaterializer>("d");
        var checkpoints = sp.GetRequiredService<ICheckpointStore>();
        var bus = Substitute.For<IBus>();

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream,
            [materializer.Dematerialize(new ThingHappened(1)), materializer.Dematerialize(new ThingHappened(2))]);
        await ((IStreamMaintenance)store).TruncateStreamAsync("s1", 2, force: true); // drops v1, emits a marker; v2 stays

        var relay = new RebusEventRelay((IAllStreamReader)store, materializer, checkpoints, bus, "d->rebus",
            new RebusRelayOptions { PublishSystemMarkers = false });
        var published = await relay.PumpAsync();

        // Only the surviving domain event is forwarded; the $stream-truncated marker is skipped.
        published.Should().Be(1);
        await bus.Received(1).Publish(
            Arg.Is<object>(m => m is ThingHappened && ((ThingHappened)m).N == 2),
            Arg.Any<IDictionary<string, string>>());
    }

    [Fact]
    public void Metadata_round_trips_through_the_headers()
    {
        var headers = new Dictionary<string, string>
        {
            [EventStoreHeaders.StreamId] = "s1",
            [EventStoreHeaders.StreamVersion] = "42",
            [EventStoreHeaders.GlobalPosition] = "000000000000000000042",
            [EventStoreHeaders.EventId] = "0123456789abcdef0123456789abcdef",
            [EventStoreHeaders.EventType] = "ThingHappened"
        };

        EventStoreMetadata.TryRead(headers, out var meta).Should().BeTrue();
        meta!.StreamId.Should().Be("s1");
        meta.StreamVersion.Should().Be(42);
        meta.EventType.Should().Be("ThingHappened");

        EventStoreMetadata.TryRead(new Dictionary<string, string>(), out _).Should().BeFalse();
    }
}
