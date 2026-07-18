using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Dbosoft.EventStore.Tests;

// A second event type used to prove the default domain keeps every registration.
file sealed record Escalated;

public sealed class RegistrationTests
{
    [Fact]
    public void Default_domain_keeps_types_from_add_domain_and_convenience_methods_together()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es
            .UseInMemory()
            .AddDomain(EventStoreBuilder.DefaultDomainName, d => d.AddEventType<Incremented>())
            .AddEventType<Escalated>());

        using var sp = services.BuildServiceProvider();
        var materializer = sp.GetRequiredKeyedService<IEventMaterializer>(EventStoreBuilder.DefaultDomainName);

        // Both the AddDomain-registered type and the convenience-registered type must be known: the
        // convenience method must extend the same default domain, not replace its materializer.
        var fromAddDomain = () => materializer.Dematerialize(new Incremented(1));
        var fromConvenience = () => materializer.Dematerialize(new Escalated());
        fromAddDomain.Should().NotThrow();
        fromConvenience.Should().NotThrow();
    }

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
