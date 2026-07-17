using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Dbosoft.EventStore.Tests;

public sealed class RegistrationTests
{
    [Fact]
    public void In_band_markers_combined_with_an_aggregate_repository_is_rejected()
    {
        var services = new ServiceCollection();

        // In-band markers take a version slot on the affected stream, which would break an aggregate's
        // optimistic concurrency. Configuring both on one domain is a silent trap, so it must be refused.
        var act = () => services.AddEventStore(es => es
            .UseInMemory()
            .AddDomain("logs", d => d
                .EmitMarkersInBand()
                .AddAggregateRepository("counter", id => new Counter(id))));

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void In_band_markers_combined_with_an_aggregate_repository_is_rejected_regardless_of_order()
    {
        var services = new ServiceCollection();

        var act = () => services.AddEventStore(es => es
            .UseInMemory()
            .AddDomain("logs", d => d
                .AddAggregateRepository("counter", id => new Counter(id))
                .EmitMarkersInBand()));

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void Registering_the_same_domain_twice_is_rejected()
    {
        var services = new ServiceCollection();

        var act = () => services.AddEventStore(es => es
            .UseInMemory()
            .AddDomain("dup", _ => { })
            .AddDomain("dup", _ => { }));

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void Resolving_a_store_for_an_unregistered_domain_throws()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseInMemory().AddDomain("known", _ => { }));
        using var sp = services.BuildServiceProvider();
        var provider = sp.GetRequiredService<IEventStoreProvider>();

        var act = () => provider.GetStore("typo");

        act.Should().Throw<ArgumentException>();
    }
}
