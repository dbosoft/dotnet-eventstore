using AwesomeAssertions;
using Xunit;

namespace Dbosoft.EventStore.AzureTables.Tests;

/// <summary>
/// Exercises the large-payload path the conformance suite never reaches: payloads over the inline
/// limit spill to blobs and must round-trip transparently, and a redaction that shrinks a payload
/// back to inline must read the small value.
/// </summary>
[UsedImplicitly]
[Collection(AzuriteCollection.Name)]
public sealed class AzureTablesLargePayloadTests(AzuriteContainerFixture container) : IAsyncLifetime
{
    private AzureTablesTestStore _store = null!;

    public async Task InitializeAsync() => _store = await AzureTablesTestStore.CreateAsync(container.ConnectionString);

    public async Task DisposeAsync() => await _store.DisposeAsync();

    [Fact]
    public async Task Large_event_payload_and_metadata_round_trip_through_blobs()
    {
        var store = _store.Provider.GetStore(_store.Domain);
        var data = RandomBytes(250_000);
        var metadata = RandomBytes(120_000);

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, [new EventData(Guid.NewGuid(), "Big", data, metadata)]);

        var stream = await ReadStream(store, "s1");
        stream.Should().ContainSingle();
        stream[0].Data.ToArray().Should().Equal(data);
        stream[0].Metadata!.Value.ToArray().Should().Equal(metadata);
    }

    [Fact]
    public async Task Large_snapshot_payload_round_trips_through_a_blob()
    {
        var snapshots = _store.Provider.GetSnapshotStore(_store.Domain)!;
        var payload = RandomBytes(300_000);

        await snapshots.SaveAsync("s1", 5, payload);
        var loaded = await snapshots.LoadLatestAsync("s1");

        loaded.Should().NotBeNull();
        loaded.Payload.ToArray().Should().Equal(payload);
    }

    [Fact]
    public async Task Redacting_a_large_event_to_a_small_one_reads_back_the_small_payload()
    {
        var store = _store.Provider.GetStore(_store.Domain);
        var maintenance = (IStreamMaintenance)store;
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, [new EventData(Guid.NewGuid(), "Big", RandomBytes(250_000))]);

        var scrubbed = "[redacted]"u8.ToArray();
        await maintenance.RedactEventAsync("s1", 1, scrubbed);

        var stream = await ReadStream(store, "s1");
        stream.Should().ContainSingle();
        stream[0].Data.ToArray().Should().Equal(scrubbed);
    }

    private static async Task<List<StoredEvent>> ReadStream(IEventStore store, string streamId)
    {
        var result = new List<StoredEvent>();
        await foreach (var stored in store.ReadStreamAsync(streamId))
            result.Add(stored);
        return result;
    }

    private static byte[] RandomBytes(int length)
    {
        var bytes = new byte[length];
        Random.Shared.NextBytes(bytes);
        return bytes;
    }
}
