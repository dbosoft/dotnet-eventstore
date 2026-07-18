using AwesomeAssertions;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.AzureTables.Tests;

/// <summary>
/// Exercises the per-stream descending index and its projector: a backward read served by the index
/// plus the not-yet-indexed tail, truncation pruning the index, and a redaction reflected through it.
/// </summary>
[UsedImplicitly]
[Collection(AzuriteCollection.Name)]
public sealed class AzureTablesIndexTests(AzuriteContainerFixture container) : IAsyncLifetime
{
    private AzureTablesTestStore _store = null!;

    public async Task InitializeAsync() => _store = await AzureTablesTestStore.CreateAsync(container.ConnectionString);

    public async Task DisposeAsync() => await _store.DisposeAsync();

    private AzureTablesEventStoreProvider Provider => (AzureTablesEventStoreProvider)_store.Provider;

    [Fact]
    public async Task Backward_read_combines_the_index_body_with_the_un_indexed_tail()
    {
        var store = Provider.GetStore(_store.Domain);
        var projector = Provider.CreateIndexProjector(_store.Domain);

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(5));
        (await projector.DrainAsync()).Should().Be(5); // index v1..5
        await store.AppendToStreamAsync("s1", 5, Events(3)); // v6..8 remain in the tail

        var read = await ReadBackwards(store, "s1");

        read.Select(e => e.StreamVersion).Should().Equal(8, 7, 6, 5, 4, 3, 2, 1);
    }

    [Fact]
    public async Task Truncation_prunes_the_index_so_a_backward_read_stays_correct()
    {
        var store = Provider.GetStore(_store.Domain);
        var maintenance = (IStreamMaintenance)store;
        var projector = Provider.CreateIndexProjector(_store.Domain);

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(5));
        await projector.DrainAsync(); // index v1..5
        await maintenance.TruncateStreamAsync("s1", 3, force: true); // remove v1, v2
        await projector.DrainAsync(); // consume the marker → prune index rows below v3

        var read = await ReadBackwards(store, "s1");

        read.Select(e => e.StreamVersion).Should().Equal(5, 4, 3);
    }

    [Fact]
    public async Task Backward_read_through_the_index_reflects_a_redaction()
    {
        var store = Provider.GetStore(_store.Domain);
        var maintenance = (IStreamMaintenance)store;
        var projector = Provider.CreateIndexProjector(_store.Domain);

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream,
            [Event("Registered", "{\"pii\":\"secret\"}"), Event(), Event()]);
        await projector.DrainAsync();

        var scrubbed = "{\"pii\":\"[redacted]\"}"u8.ToArray();
        await maintenance.RedactEventAsync("s1", 1, scrubbed);

        var read = await ReadBackwards(store, "s1");

        read.Select(e => e.StreamVersion).Should().Equal(3, 2, 1);
        // The index points to the row; dereferencing it returns the current (redacted) payload.
        read[^1].Data.ToArray().Should().Equal(scrubbed);
    }

    [Fact]
    public async Task All_read_is_index_backed_and_complete_after_projection()
    {
        var store = Provider.GetStore(_store.Domain);
        var reader = (IAllStreamReader)store;
        var projector = Provider.CreateIndexProjector(_store.Domain);

        await store.AppendToStreamAsync("a", ExpectedVersion.NoStream, Events(2));
        await store.AppendToStreamAsync("b", ExpectedVersion.NoStream, Events(2));

        // Eventually consistent: nothing is in $all until the projector indexes it.
        (await ReadAll(reader)).Should().BeEmpty();

        await projector.DrainAsync();

        var all = await ReadAll(reader);
        all.Select(e => e.StreamId).Should().Equal("a", "a", "b", "b");
        all.Select(e => e.Position).Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task Truncation_prunes_the_all_index_rows()
    {
        var store = Provider.GetStore(_store.Domain);
        var reader = (IAllStreamReader)store;
        var maintenance = (IStreamMaintenance)store;
        var projector = Provider.CreateIndexProjector(_store.Domain);

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(5));
        await projector.DrainAsync();
        await maintenance.TruncateStreamAsync("s1", 3, force: true);
        await projector.DrainAsync(); // consume the marker → prune the $all rows below v3, not just skip them

        // $all reflects the truncation (the pruned rows are gone from the index, exercised without error).
        (await ReadAll(reader)).Where(e => e.StreamId == "s1").Select(e => e.StreamVersion).Should().Equal(3, 4, 5);
    }

    [Fact]
    public async Task The_safety_window_holds_fresh_events_until_they_age_out()
    {
        var store = Provider.GetStore(_store.Domain);
        var reader = (IAllStreamReader)store;
        // A large window so freshly-appended events are always "too fresh" for a windowed pump — the same
        // hold that stops the projector from checkpointing past a position whose lower sibling is still in
        // flight, which is what would otherwise skip that sibling permanently.
        var projector = Provider.CreateIndexProjector(_store.Domain, safetyWindow: TimeSpan.FromHours(1));

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(3));

        (await projector.PumpAsync()).Should().Be(0); // within the window → held, checkpoint not advanced
        (await ReadAll(reader)).Should().BeEmpty();

        // Draining (or, in production, the window elapsing) indexes them once no lower sibling can be pending.
        (await projector.DrainAsync()).Should().Be(3);
        (await ReadAll(reader)).Select(e => e.StreamVersion).Should().Equal(1, 2, 3);
    }

    private static async Task<List<StoredEvent>> ReadAll(IAllStreamReader reader)
    {
        var result = new List<StoredEvent>();
        await foreach (var stored in reader.ReadAllAsync(GlobalPosition.Start))
            result.Add(stored);
        return result;
    }

    private static async Task<List<StoredEvent>> ReadBackwards(IEventStore store, string streamId)
    {
        var result = new List<StoredEvent>();
        await foreach (var stored in store.ReadStreamAsync(streamId, ReadDirection.Backwards, long.MaxValue))
            result.Add(stored);
        return result;
    }
}
