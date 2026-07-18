using System.Collections.Concurrent;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Dbosoft.EventStore.DependencyInjection;

namespace Dbosoft.EventStore.AzureTables;

/// <summary>
/// Backs each domain with its own set of Azure Storage resources — an events table, a snapshots table
/// and a blob container for large payloads — plus one shared checkpoints table. A domain is therefore
/// a self-contained unit that can be created or dropped on its own. The stores hold no per-operation
/// state, so the provider is safe as a singleton.
/// </summary>
[PublicAPI]
public sealed class AzureTablesEventStoreProvider : IEventStoreProvider
{
    private readonly TableServiceClient _tables;
    private readonly BlobServiceClient _blobs;
    private readonly TimeProvider _timeProvider;
    private readonly string _prefix;
    private readonly Dictionary<string, DomainOptions> _options;
    private readonly ConcurrentDictionary<string, AzureTablesEventStore> _stores = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, AzureTablesSnapshotStore> _snapshotStores = new(StringComparer.Ordinal);

    /// <summary>Creates the provider for the registered domains.</summary>
    public AzureTablesEventStoreProvider(
        TableServiceClient tables,
        BlobServiceClient blobs,
        IEnumerable<RegisteredDomain> domains,
        TimeProvider timeProvider,
        string tablePrefix)
    {
        _tables = tables;
        _blobs = blobs;
        _timeProvider = timeProvider;
        _prefix = tablePrefix;
        _options = domains
            .GroupBy(d => d.Name, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First().Options, StringComparer.Ordinal);
        Domains = _options.Keys.ToArray();
        Checkpoints = new AzureTablesCheckpointStore(_tables.GetTableClient(AzureTableNames.Checkpoints(_prefix)));
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Domains { get; }

    /// <summary>The durable checkpoint store, shared across domains.</summary>
    public ICheckpointStore Checkpoints { get; }

    /// <inheritdoc />
    public IEventStore GetStore(string domain) =>
        _stores.GetOrAdd(domain, key =>
        {
            var options = OptionsFor(key);
            return new AzureTablesEventStore(
                EventsTable(key), SnapshotsTable(key), IndexTable(key), Payloads(key), _timeProvider, options.InBandMarkers, options.TombstoneRetention);
        });

    /// <summary>
    /// Creates an index projector for a domain — run it (e.g. as a hosted service) to keep the domain's
    /// per-stream descending index current, which makes backward stream reads a server-side range scan.
    /// </summary>
    public AzureTablesIndexProjector CreateIndexProjector(string domain, string checkpointName = "azure-tables-index")
    {
        OptionsFor(domain); // validate the domain is registered
        return new AzureTablesIndexProjector(GetStore(domain), IndexTable(domain), Checkpoints, $"{domain}:{checkpointName}");
    }

    /// <inheritdoc />
    public ISnapshotStore GetSnapshotStore(string domain)
    {
        OptionsFor(domain); // validate the domain is registered
        return _snapshotStores.GetOrAdd(domain, key => new AzureTablesSnapshotStore(SnapshotsTable(key), Payloads(key), _timeProvider));
    }

    /// <summary>Creates the tables and blob containers for every registered domain if they are absent.</summary>
    public async Task EnsureCreatedAsync(CancellationToken ct = default)
    {
        await _tables.GetTableClient(AzureTableNames.Checkpoints(_prefix)).CreateIfNotExistsAsync(ct).ConfigureAwait(false);
        foreach (var domain in Domains)
        {
            await EventsTable(domain).CreateIfNotExistsAsync(ct).ConfigureAwait(false);
            await SnapshotsTable(domain).CreateIfNotExistsAsync(ct).ConfigureAwait(false);
            await IndexTable(domain).CreateIfNotExistsAsync(ct).ConfigureAwait(false);
            await Container(domain).CreateIfNotExistsAsync(cancellationToken: ct).ConfigureAwait(false);
        }
    }

    private DomainOptions OptionsFor(string domain) =>
        _options.TryGetValue(domain, out var options)
            ? options
            : throw new ArgumentException($"Domain '{domain}' is not registered.", nameof(domain));

    private TableClient EventsTable(string domain) => _tables.GetTableClient(AzureTableNames.Events(_prefix, domain));

    private TableClient SnapshotsTable(string domain) => _tables.GetTableClient(AzureTableNames.Snapshots(_prefix, domain));

    private TableClient IndexTable(string domain) => _tables.GetTableClient(AzureTableNames.Index(_prefix, domain));

    private BlobContainerClient Container(string domain) => _blobs.GetBlobContainerClient(AzureTableNames.Container(_prefix, domain));

    private PayloadStore Payloads(string domain) => new(Container(domain));
}
