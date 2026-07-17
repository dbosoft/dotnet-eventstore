using AwesomeAssertions;
using Dbosoft.EventStore.Serialization;
using Xunit;

namespace Dbosoft.EventStore.Tests;

public sealed class SerializationTests
{
    private sealed record SampleEvent(string Name, int Count);

    [Fact]
    public void Serializer_round_trips_an_event()
    {
        var serializer = new SystemTextJsonEventSerializer();
        var original = new SampleEvent("hello", 42);

        var bytes = serializer.Serialize(original);
        var restored = (SampleEvent)serializer.Deserialize(typeof(SampleEvent), bytes);

        restored.Name.Should().Be("hello");
        restored.Count.Should().Be(42);
    }

    [Fact]
    public void Registry_maps_type_to_name_and_back()
    {
        var registry = new EventTypeRegistry();
        registry.Register(typeof(SampleEvent), "sample");

        registry.GetEventType(typeof(SampleEvent)).Should().Be("sample");
        registry.Resolve("sample").Should().Be(typeof(SampleEvent));
    }

    [Fact]
    public void Registry_rejects_a_name_collision()
    {
        var registry = new EventTypeRegistry();
        registry.Register(typeof(SampleEvent), "sample");

        var act = () => registry.Register(typeof(SerializationTests), "sample");

        act.Should().Throw<InvalidOperationException>();
    }
}

public sealed class UpcasterChainTests
{
    private sealed record V1(int Amount);

    private sealed record V2(int Amount, string Currency);

    private sealed class V1ToV2 : IEventUpcaster
    {
        public bool CanUpcast(object @event) => @event is V1;
        public object Upcast(object @event) => new V2(((V1)@event).Amount, "EUR");
    }

    [Fact]
    public void Chain_upgrades_an_old_event()
    {
        var chain = new UpcasterChain([new V1ToV2()]);

        var result = chain.Upcast(new V1(100));

        result.Should().BeOfType<V2>();
        var v2 = (V2)result;
        v2.Amount.Should().Be(100);
        v2.Currency.Should().Be("EUR");
    }

    [Fact]
    public void Chain_leaves_current_events_untouched()
    {
        var chain = new UpcasterChain([new V1ToV2()]);
        var current = new V2(100, "USD");

        chain.Upcast(current).Should().BeSameAs(current);
    }

    [Fact]
    public void Empty_chain_is_identity()
    {
        var chain = new UpcasterChain([]);
        var evt = new V1(1);

        chain.Upcast(evt).Should().BeSameAs(evt);
    }
}
