using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Dbosoft.EventStore.Tests;

public sealed class BusRelayTests
{
    [Fact]
    public async Task Relay_publishes_materialized_events_and_resumes_from_its_checkpoint()
    {
        var services = new ServiceCollection();
        services.AddEventStore(es => es.UseInMemory().AddDomain("d", d => d.AddEventType<Incremented>()));
        await using var sp = services.BuildServiceProvider();

        var provider = sp.GetRequiredService<IEventStoreProvider>();
        var store = provider.GetStore("d");
        var materializer = sp.GetRequiredKeyedService<IEventMaterializer>("d");
        var bus = new InMemoryBus();
        var received = new List<object>();
        using var _ = bus.Subscribe((m, _) =>
        {
            received.Add(m.Payload);
            return Task.CompletedTask;
        });

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, [materializer.Dematerialize(new Incremented(3))]);

        var relay = new StoreToBusRelay(
            (IAllStreamReader)store, materializer, sp.GetRequiredService<ICheckpointStore>(), bus, "d->bus");

        var first = await relay.PumpAsync();
        var second = await relay.PumpAsync(); // nothing new — resumes from the checkpoint

        first.Should().Be(1);
        second.Should().Be(0);
        received.Should().ContainSingle().Which.Should().BeOfType<Incremented>()
            .Which.By.Should().Be(3);
    }
}
