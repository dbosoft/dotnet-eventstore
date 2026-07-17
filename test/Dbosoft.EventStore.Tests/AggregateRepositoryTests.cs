using AwesomeAssertions;
using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Dbosoft.EventStore.Tests;

public sealed class AggregateRepositoryTests
{
    private static ServiceProvider BuildProvider(ISnapshotPolicy? policy = null)
    {
        var services = new ServiceCollection();
        services.AddEventStore(b => b
            .UseInMemory()
            .AddEventType<Incremented>()
            .AddAggregateRepository("counter", id => new Counter(id), policy));
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task Load_returns_null_for_unknown_aggregate()
    {
        await using var sp = BuildProvider();
        var repo = sp.GetRequiredService<IAggregateRepository<Counter>>();

        (await repo.LoadAsync("nope")).Should().BeNull();
    }

    [Fact]
    public async Task Save_then_load_reconstructs_state_and_version()
    {
        await using var sp = BuildProvider();
        var repo = sp.GetRequiredService<IAggregateRepository<Counter>>();

        var counter = new Counter("c1");
        counter.Increment(3);
        counter.Increment(4);
        await repo.SaveAsync(counter);

        var loaded = await repo.LoadAsync("c1");

        loaded.Should().NotBeNull();
        loaded.Value.Should().Be(7);
        loaded.Version.Should().Be(2);
        loaded.UncommittedEvents.Should().BeEmpty();
    }

    [Fact]
    public async Task Save_clears_uncommitted_events()
    {
        await using var sp = BuildProvider();
        var repo = sp.GetRequiredService<IAggregateRepository<Counter>>();

        var counter = new Counter("c1");
        counter.Increment(1);
        await repo.SaveAsync(counter);

        counter.UncommittedEvents.Should().BeEmpty();
    }

    [Fact]
    public async Task Concurrent_save_of_stale_aggregate_conflicts()
    {
        await using var sp = BuildProvider();
        var repo = sp.GetRequiredService<IAggregateRepository<Counter>>();

        var seed = new Counter("c1");
        seed.Increment(1);
        await repo.SaveAsync(seed);

        var a = await repo.LoadAsync("c1");
        var b = await repo.LoadAsync("c1");
        a!.Increment(1);
        b!.Increment(1);

        await repo.SaveAsync(a);
        var act = () => repo.SaveAsync(b);

        await act.Should().ThrowAsync<WrongExpectedVersionException>();
    }

    [Fact]
    public async Task Load_uses_snapshot_when_earlier_events_are_truncated()
    {
        await using var sp = BuildProvider(new EveryNEventsSnapshotPolicy(5));
        var repo = sp.GetRequiredService<IAggregateRepository<Counter>>();
        var maintenance = (IStreamMaintenance)sp.GetRequiredService<IEventStoreProvider>()
            .GetStore(EventStoreBuilder.DefaultDomainName);

        var counter = new Counter("c1");
        for (var i = 0; i < 6; i++)
            counter.Increment(1);
        await repo.SaveAsync(counter);

        // Crossing 5 events wrote a snapshot at version 6. Truncating away versions 1..5 leaves the
        // stream reconstructable only via that snapshot — so a correct load must use it.
        await maintenance.TruncateStreamAsync("counter-c1", 6);

        var loaded = await repo.LoadAsync("c1");

        loaded.Should().NotBeNull();
        loaded.Value.Should().Be(6);
        loaded.Version.Should().Be(6);
    }

    [Fact]
    public async Task Load_falls_back_to_replay_when_a_snapshot_cannot_be_deserialized()
    {
        await using var sp = BuildProvider(new EveryNEventsSnapshotPolicy(5));
        var repo = sp.GetRequiredService<IAggregateRepository<Counter>>();
        var snapshots = sp.GetRequiredService<IEventStoreProvider>()
            .GetSnapshotStore(EventStoreBuilder.DefaultDomainName)!;

        var counter = new Counter("c1");
        for (var i = 0; i < 6; i++)
            counter.Increment(1);
        await repo.SaveAsync(counter);

        // Simulate a snapshot whose schema no longer deserializes into the current state type.
        await snapshots.SaveAsync("counter-c1", 6, "\"not an object\""u8.ToArray());

        var loaded = await repo.LoadAsync("c1");

        // The unusable snapshot is ignored and the aggregate is rebuilt from the events.
        loaded.Should().NotBeNull();
        loaded.Value.Should().Be(6);
        loaded.Version.Should().Be(6);
    }

    [Fact]
    public async Task Load_fails_loudly_when_truncated_and_the_covering_snapshot_is_unusable()
    {
        await using var sp = BuildProvider(new EveryNEventsSnapshotPolicy(5));
        var repo = sp.GetRequiredService<IAggregateRepository<Counter>>();
        var provider = sp.GetRequiredService<IEventStoreProvider>();
        var maintenance = (IStreamMaintenance)provider.GetStore(EventStoreBuilder.DefaultDomainName);
        var snapshots = provider.GetSnapshotStore(EventStoreBuilder.DefaultDomainName)!;

        var counter = new Counter("c1");
        for (var i = 0; i < 6; i++)
            counter.Increment(1);
        await repo.SaveAsync(counter);

        // Truncate behind the snapshot at v6, then corrupt that snapshot: the prefix events are gone
        // and the only thing that could reconstruct them no longer deserializes.
        await maintenance.TruncateStreamAsync("counter-c1", 6);
        await snapshots.SaveAsync("counter-c1", 6, "\"not an object\""u8.ToArray());

        var act = () => repo.LoadAsync("c1");

        await act.Should().ThrowAsync<SnapshotRequiredException>();
    }
}
