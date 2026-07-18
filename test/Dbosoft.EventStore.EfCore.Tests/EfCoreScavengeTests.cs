using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.EfCore.Tests;

/// <summary>
/// Exercises the EF Core scavenge cutoff against a controllable clock — the tombstone-retention path
/// and the SQLite ticks converter for <c>CreatedAt &lt; cutoff</c>, which no other test reaches.
/// </summary>
public sealed class EfCoreScavengeTests
{
    [Fact]
    public async Task Scavenge_removes_system_tombstones_older_than_the_retention_window()
    {
        var clock = new TestClock(DateTimeOffset.UnixEpoch);
        var connectionString = $"DataSource=file:memdb-{Guid.NewGuid():N}?mode=memory&cache=shared";
        await using var keepAlive = new SqliteConnection(connectionString);
        keepAlive.Open();

        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock); // wins over UseEfCore's TryAddSingleton(TimeProvider.System)
        services.AddEventStore(es => es
            .UseSqlite(connectionString)
            .AddDomain("test", d => d.WithTombstoneRetention(TimeSpan.FromDays(7))));
        await using var sp = services.BuildServiceProvider();

        await using (var ctx = await sp.GetRequiredService<IDbContextFactory<EventStoreDbContext>>().CreateDbContextAsync())
            await ctx.Database.EnsureCreatedAsync();

        var provider = sp.GetRequiredService<IEventStoreProvider>();
        var store = provider.GetStore("test");
        var maintenance = (IStreamMaintenance)store;

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));
        await provider.GetSnapshotStore("test")!.SaveAsync("s1", 2, "{}"u8.ToArray());
        await maintenance.TruncateStreamAsync("s1", 2); // emits a $system tombstone at the clock's "now"

        var beforeExpiry = await maintenance.ScavengeAsync();
        beforeExpiry.EventsRemoved.Should().Be(0);

        clock.Advance(TimeSpan.FromDays(8));
        var afterExpiry = await maintenance.ScavengeAsync();

        afterExpiry.EventsRemoved.Should().BeGreaterThan(0);
    }
}

file sealed class TestClock(DateTimeOffset start) : TimeProvider
{
    private DateTimeOffset _now = start;

    public override DateTimeOffset GetUtcNow() => _now;

    public void Advance(TimeSpan by) => _now += by;
}
