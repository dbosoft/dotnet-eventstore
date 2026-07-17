using AwesomeAssertions;
using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Dbosoft.EventStore.Tests;

// A second aggregate type, in its own domain, sharing the counter events by marker interface.
file interface ICountingEvent;

file sealed record Counted(int By) : ICountingEvent;

public sealed class DomainTests
{
    [Fact]
    public async Task Domains_are_isolated_even_for_the_same_stream_id()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es
            .UseInMemory()
            .AddDomain("a", d => d.AddEventType<Incremented>())
            .AddDomain("b", d => d.AddEventType<Incremented>()));

        await using var sp = services.BuildServiceProvider();
        var provider = sp.GetRequiredService<IEventStoreProvider>();

        var evt = ConformanceEvent();
        await provider.GetStore("a").AppendToStreamAsync("shared", ExpectedVersion.NoStream, [evt]);

        // Domain "b" has never seen the stream, even though "a" holds it under the same id.
        (await provider.GetStore("b").GetStreamInfoAsync("shared")).Should().BeNull();
        (await provider.GetStore("a").GetStreamInfoAsync("shared")).Should().NotBeNull();
    }

    [Fact]
    public async Task Marker_interface_registers_all_events_in_a_domain()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es
            .UseInMemory()
            .AddDomain("counting", d => d
                .AddEventTypesImplementing<ICountingEvent>()
                .AddAggregateRepository("counter", id => new MarkerCounter(id))));

        await using var sp = services.BuildServiceProvider();
        var repo = sp.GetRequiredService<IAggregateRepository<MarkerCounter>>();

        var counter = new MarkerCounter("c1");
        counter.Add(5);
        await repo.SaveAsync(counter);

        var loaded = await repo.LoadAsync("c1");

        loaded.Should().NotBeNull();
        loaded.Total.Should().Be(5);
    }

    private static EventData ConformanceEvent() =>
        new(Guid.NewGuid(), nameof(Incremented), "{}"u8.ToArray());
}

internal sealed class MarkerCounter(string id) : AggregateBase(id)
{
    public int Total { get; private set; }

    public void Add(int by) => Raise(new Counted(by));

    protected override void When(object @event)
    {
        if (@event is Counted counted)
            Total += counted.By;
    }
}
