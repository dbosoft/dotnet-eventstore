using AwesomeAssertions;
using Dbosoft.EventStore.InMemory;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.Tests;

public sealed class RetentionTests
{
    [Fact]
    public async Task Retention_by_max_age_truncates_events_older_than_the_window()
    {
        var clock = new MutableClock(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var store = new InMemoryEventStore(clock);
        await store.SetStreamMetadataAsync("s1", new StreamMetadata { MaxAge = TimeSpan.FromHours(1) });

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1)); // v1 at t0
        clock.Advance(TimeSpan.FromHours(2));
        await store.AppendToStreamAsync("s1", 1, Events(1)); // v2 at t0 + 2h

        // At t0 + 2h the one-hour window keeps only v2; v1 is older and is truncated away.
        await store.ScavengeAsync();

        (await store.ReadAllOf("s1")).Select(e => e.StreamVersion).Should().Equal(2);
    }

    [Fact]
    public async Task Retention_leaves_a_stream_within_its_limits_untouched()
    {
        var store = new InMemoryEventStore();
        await store.SetStreamMetadataAsync("s1", new StreamMetadata { MaxCount = 10 });
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));

        await store.ScavengeAsync();

        (await store.ReadAllOf("s1")).Select(e => e.StreamVersion).Should().Equal(1, 2, 3);
        (await store.GetStreamInfoAsync("s1"))!.TruncatedBefore.Should().Be(1); // nothing truncated
    }
}
