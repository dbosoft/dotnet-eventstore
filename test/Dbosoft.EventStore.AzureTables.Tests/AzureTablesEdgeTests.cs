using AwesomeAssertions;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.AzureTables.Tests;

/// <summary>
/// Provider-specific edge cases the conformance suite does not reach: transactions that exceed the
/// 100-entity entity-group limit, keys Azure rejects, and the concurrent large-payload race the
/// EventId-keyed blob naming exists to make safe.
/// </summary>
[UsedImplicitly]
[Collection(AzuriteCollection.Name)]
public sealed class AzureTablesEdgeTests(AzuriteContainerFixture container) : IAsyncLifetime
{
    private AzureTablesTestStore _store = null!;

    public async Task InitializeAsync() => _store = await AzureTablesTestStore.CreateAsync(container.ConnectionString);

    public async Task DisposeAsync() => await _store.DisposeAsync();

    [Fact]
    public async Task Truncating_more_than_one_transaction_of_events_works()
    {
        var store = _store.Provider.GetStore(_store.Domain);
        var maintenance = (IStreamMaintenance)store;

        // 150 events, so truncating 119 of them spans more than one 100-entity transaction.
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(99));
        await store.AppendToStreamAsync("s1", 99, Events(51));

        await maintenance.TruncateStreamAsync("s1", 120, force: true);

        (await ReadStream(store, "s1")).Select(e => e.StreamVersion).Should().Equal(Enumerable.Range(120, 31).Select(i => (long)i));
        var info = await store.GetStreamInfoAsync("s1");
        info.Should().NotBeNull();
        info.Version.Should().Be(150);
        info.TruncatedBefore.Should().Be(120);
    }

    [Fact]
    public async Task A_stream_id_with_a_reserved_key_character_is_rejected_clearly()
    {
        var store = _store.Provider.GetStore(_store.Domain);
        var maintenance = (IStreamMaintenance)store;
        const string bad = "orders/42";

        // Every entry point that takes a stream id rejects an id Azure cannot key on, with the clear
        // provider exception rather than a raw storage/filter error — reads and maintenance, not just append.
        await ((Func<Task>)(() => store.AppendToStreamAsync(bad, ExpectedVersion.NoStream, Events(1))))
            .Should().ThrowAsync<NotSupportedByProviderException>();
        await ((Func<Task>)(() => ReadStream(store, bad))).Should().ThrowAsync<NotSupportedByProviderException>();
        await ((Func<Task>)(() => store.GetStreamInfoAsync(bad))).Should().ThrowAsync<NotSupportedByProviderException>();
        await ((Func<Task>)(() => maintenance.TruncateStreamAsync(bad, 1, force: true)))
            .Should().ThrowAsync<NotSupportedByProviderException>();
        await ((Func<Task>)(() => maintenance.DeleteStreamAsync(bad, DeleteMode.Soft)))
            .Should().ThrowAsync<NotSupportedByProviderException>();
    }

    [Fact]
    public async Task Concurrent_large_appends_to_the_same_version_do_not_corrupt_the_winner()
    {
        var store = _store.Provider.GetStore(_store.Domain);
        var payloadA = RandomBytes(200_000);
        var payloadB = RandomBytes(200_000);

        // Both race to become version 1 with distinct large payloads. With version-keyed blob names the
        // loser's upload could clobber the winner's blob; keying by EventId keeps them separate.
        byte[]? winner = null;
        await Task.WhenAll(
            Append(store, "s1", payloadA, () => winner = payloadA),
            Append(store, "s1", payloadB, () => winner = payloadB));

        winner.Should().NotBeNull();
        var read = await ReadStream(store, "s1");
        read.Should().ContainSingle();
        read[0].Data.ToArray().Should().Equal(winner);
    }

    private static Task Append(IEventStore store, string streamId, byte[] payload, Action onWin) =>
        Task.Run(async () =>
        {
            try
            {
                await store.AppendToStreamAsync(streamId, ExpectedVersion.NoStream, [new EventData(Guid.NewGuid(), "Big", payload)]);
                onWin();
            }
            catch (WrongExpectedVersionException)
            {
            }
        });

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
