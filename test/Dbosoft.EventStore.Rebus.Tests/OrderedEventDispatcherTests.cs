using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Dbosoft.EventStore.Rebus.Tests;

public sealed class OrderedEventDispatcherTests
{
    private static async Task<(IEventStore Store, IEventMaterializer Materializer)> StoreWithTwoStreamsAsync()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseInMemory().AddDomain("d", d => d.AddEventType<ThingHappened>()));
        var sp = services.BuildServiceProvider();

        var store = sp.GetRequiredKeyedService<IEventStore>("d");
        var materializer = sp.GetRequiredKeyedService<IEventMaterializer>("d");
        await store.AppendToStreamAsync("a", ExpectedVersion.NoStream,
            [materializer.Dematerialize(new ThingHappened(1)), materializer.Dematerialize(new ThingHappened(2)), materializer.Dematerialize(new ThingHappened(3))]);
        await store.AppendToStreamAsync("b", ExpectedVersion.NoStream,
            [materializer.Dematerialize(new ThingHappened(10)), materializer.Dematerialize(new ThingHappened(20))]);
        return (store, materializer);
    }

    private static EventStoreMetadata Signal(string streamId, long version) =>
        new(streamId, version, GlobalPosition.Start, Guid.Empty, nameof(ThingHappened));

    [Fact]
    public async Task Applies_each_stream_in_version_order_despite_scrambled_duplicated_and_gapped_signals()
    {
        var (store, materializer) = await StoreWithTwoStreamsAsync();
        var applied = new List<string>();
        var dispatcher = new OrderedEventDispatcher(store, materializer, (_, meta, _) =>
        {
            applied.Add($"{meta.StreamId}:{meta.StreamVersion}");
            return Task.CompletedTask;
        });

        // Signals arrive out of order, with a jump-ahead gap (a:3 before a:1/a:2) and duplicates.
        EventStoreMetadata[] signals =
        [
            Signal("a", 3), // gap: fills a:1, a:2, a:3 from the store
            Signal("a", 1), // duplicate — already applied
            Signal("b", 20), // gap: fills b:1, b:2
            Signal("a", 1), // duplicate
            Signal("a", 2), // duplicate
            Signal("b", 10), // duplicate
            Signal("a", 3) // duplicate
        ];
        foreach (var signal in signals)
            await dispatcher.DispatchAsync(signal);

        // Each stream applied exactly once, in ascending version order — regardless of signal order.
        applied.Where(x => x.StartsWith('a')).Should().Equal("a:1", "a:2", "a:3");
        applied.Where(x => x.StartsWith('b')).Should().Equal("b:1", "b:2");
        applied.Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public async Task Delivers_the_typed_event_and_resumes_from_a_seeded_watermark()
    {
        var (store, materializer) = await StoreWithTwoStreamsAsync();
        var values = new List<int>();
        var dispatcher = new OrderedEventDispatcher(store, materializer, (evt, _, _) =>
        {
            values.Add(((ThingHappened)evt).N);
            return Task.CompletedTask;
        });

        // A restart seeded past a:1 must not re-apply it; it resumes at a:2.
        dispatcher.Seed("a", 1);
        await dispatcher.DispatchAsync(Signal("a", 3));

        values.Should().Equal(2, 3); // a:1 skipped by the seed, a:2 and a:3 delivered as typed events
        dispatcher.LastApplied("a").Should().Be(3);
    }
}
