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
        (await projector.PumpAsync()).Should().Be(5); // index v1..5
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
        await projector.PumpAsync(); // index v1..5
        await maintenance.TruncateStreamAsync("s1", 3, force: true); // remove v1, v2
        await projector.PumpAsync(); // consume the marker → prune index rows below v3

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
        await projector.PumpAsync();

        var scrubbed = "{\"pii\":\"[redacted]\"}"u8.ToArray();
        await maintenance.RedactEventAsync("s1", 1, scrubbed);

        var read = await ReadBackwards(store, "s1");

        read.Select(e => e.StreamVersion).Should().Equal(3, 2, 1);
        // The index points to the row; dereferencing it returns the current (redacted) payload.
        read[^1].Data.ToArray().Should().Equal(scrubbed);
    }

    private static async Task<List<StoredEvent>> ReadBackwards(IEventStore store, string streamId)
    {
        var result = new List<StoredEvent>();
        await foreach (var stored in store.ReadStreamAsync(streamId, ReadDirection.Backwards, long.MaxValue))
            result.Add(stored);
        return result;
    }
}
