using Azure;
using Azure.Core;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.AzureTables.Tests;

/// <summary>
/// Adversarial tests for the ways Azure Table Storage is not one consistent unit: a destructive op and
/// its <c>$system</c> marker are in different partitions (not atomic), and a payload blob is separate
/// from its table row. These force the in-between states and assert the store recovers cleanly.
/// </summary>
[UsedImplicitly]
[Collection(AzuriteCollection.Name)]
public sealed class AzureTablesFaultInjectionTests(AzuriteContainerFixture container)
{
    [Fact]
    public async Task Truncation_that_loses_its_marker_re_emits_it_on_retry()
    {
        var (provider, domain, tablePolicy, _) = Build(container.ConnectionString);
        await provider.EnsureCreatedAsync();
        var store = provider.GetStore(domain);
        var maintenance = (IStreamMaintenance)store;
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(5));

        // Fail the marker's batch — the second batch write from here (the first is the truncation delete).
        var batches = new int[1];
        tablePolicy.ShouldFail = req => FaultInjectionPolicy.IsBatch(req) && Interlocked.Increment(ref batches[0]) > 1;

        var act = () => maintenance.TruncateStreamAsync("s1", 3, force: true);
        await act.Should().ThrowAsync<RequestFailedException>();

        // The data was truncated but the marker was lost.
        (await store.ReadAllOf("s1")).Select(e => e.StreamVersion).Should().Equal(3, 4, 5);
        (await ReadAll(store)).Should().NotContain(e => e.EventType == SystemEventTypes.StreamTruncated);

        // Retrying the same truncation recovers the missing marker.
        tablePolicy.ShouldFail = null;
        await maintenance.TruncateStreamAsync("s1", 3, force: true);
        (await ReadAll(store)).Should().ContainSingle(e => e.EventType == SystemEventTypes.StreamTruncated);
    }

    [Fact]
    public async Task Deletion_that_loses_its_marker_re_emits_it_on_retry()
    {
        var (provider, domain, tablePolicy, _) = Build(container.ConnectionString);
        await provider.EnsureCreatedAsync();
        var store = provider.GetStore(domain);
        var maintenance = (IStreamMaintenance)store;
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(5));

        var batches = new int[1];
        tablePolicy.ShouldFail = req => FaultInjectionPolicy.IsBatch(req) && Interlocked.Increment(ref batches[0]) > 1;

        var act = () => maintenance.DeleteStreamAsync("s1", DeleteMode.Soft);
        await act.Should().ThrowAsync<RequestFailedException>();

        // The events were removed but the tombstone was lost.
        (await store.ReadAllOf("s1")).Should().BeEmpty();
        (await ReadAll(store)).Should().NotContain(e => e.EventType == SystemEventTypes.StreamDeleted);

        tablePolicy.ShouldFail = null;
        await maintenance.DeleteStreamAsync("s1", DeleteMode.Soft);
        (await ReadAll(store)).Should().ContainSingle(e => e.EventType == SystemEventTypes.StreamDeleted);
    }

    [Fact]
    public async Task A_failed_blob_cleanup_during_truncation_recovers_on_retry()
    {
        var (provider, domain, _, blobPolicy) = Build(container.ConnectionString);
        await provider.EnsureCreatedAsync();
        var store = provider.GetStore(domain);
        var maintenance = (IStreamMaintenance)store;
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, [new EventData(Guid.NewGuid(), "Big", RandomBytes(200_000))]);
        await store.AppendToStreamAsync("s1", 1, Events(4));

        // Truncation deletes the payload blobs before the rows; fail that delete.
        blobPolicy.ShouldFail = req => req.Method == RequestMethod.Delete;
        var act = () => maintenance.TruncateStreamAsync("s1", 3, force: true);
        await act.Should().ThrowAsync<RequestFailedException>();

        // Nothing was committed (the boundary was not advanced), so a retry completes it cleanly — no
        // orphaned blob, no partial state.
        blobPolicy.ShouldFail = null;
        await maintenance.TruncateStreamAsync("s1", 3, force: true);
        (await store.ReadAllOf("s1")).Select(e => e.StreamVersion).Should().Equal(3, 4, 5);
    }

    [Fact]
    public async Task A_vanished_payload_blob_is_skipped_rather_than_crashing_the_read()
    {
        var (provider, domain, _, blobPolicy) = Build(container.ConnectionString);
        await provider.EnsureCreatedAsync();
        var store = provider.GetStore(domain);
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, [new EventData(Guid.NewGuid(), "Big", RandomBytes(200_000))]);
        await store.AppendToStreamAsync("s1", 1, [new EventData(Guid.NewGuid(), "Small", "kept"u8.ToArray())]);

        // Make the blob download fail as if a concurrent delete removed it mid-read.
        blobPolicy.Status = 404;
        blobPolicy.ShouldFail = req => req.Method == RequestMethod.Get;

        var read = await store.ReadAllOf("s1");

        // The blob-backed event is dropped from the read; the inline one survives, and nothing throws.
        read.Should().ContainSingle();
        read[0].StreamVersion.Should().Be(2);
    }

    [Fact]
    public async Task An_append_over_the_batch_limit_is_rejected_with_a_clear_error()
    {
        var (provider, domain, _, _) = Build(container.ConnectionString);
        await provider.EnsureCreatedAsync();
        var store = provider.GetStore(domain);

        var act = () => store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(100));

        await act.Should().ThrowAsync<NotSupportedByProviderException>();
    }

    private static (AzureTablesEventStoreProvider Provider, string Domain, FaultInjectionPolicy Tables, FaultInjectionPolicy Blobs)
        Build(string connectionString)
    {
        var tables = new FaultInjectionPolicy();
        var blobs = new FaultInjectionPolicy();

        var tableOptions = new TableClientOptions();
        tableOptions.AddPolicy(tables, HttpPipelinePosition.PerCall);
        var blobOptions = new BlobClientOptions();
        blobOptions.AddPolicy(blobs, HttpPipelinePosition.PerCall);

        var domain = "d" + Guid.NewGuid().ToString("N");
        var provider = new AzureTablesEventStoreProvider(
            new TableServiceClient(connectionString, tableOptions),
            new BlobServiceClient(connectionString, blobOptions),
            [new RegisteredDomain(domain, new DomainOptions())],
            TimeProvider.System,
            "");
        return (provider, domain, tables, blobs);
    }

    private static async Task<List<StoredEvent>> ReadAll(IEventStore store)
    {
        var result = new List<StoredEvent>();
        await foreach (var e in ((IAllStreamReader)store).ReadAllAsync(GlobalPosition.Start))
            result.Add(e);
        return result;
    }

    private static byte[] RandomBytes(int length)
    {
        var bytes = new byte[length];
        Random.Shared.NextBytes(bytes);
        return bytes;
    }
}
