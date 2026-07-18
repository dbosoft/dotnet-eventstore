using Azure.Data.Tables;

namespace Dbosoft.EventStore.AzureTables;

/// <summary>
/// Durable checkpoint store backed by one shared table (the checkpoint store is a single
/// domain-agnostic service). Each named checkpoint is a row keyed by its name.
/// </summary>
internal sealed class AzureTablesCheckpointStore(TableClient checkpoints) : ICheckpointStore
{
    private const string Partition = "checkpoint";

    public async Task<GlobalPosition?> GetAsync(string name, CancellationToken ct = default)
    {
        var response = await checkpoints
            .GetEntityIfExistsAsync<TableEntity>(Partition, name, cancellationToken: ct)
            .ConfigureAwait(false);
        return response is { HasValue: true, Value: { } entity } && entity.GetString("Token") is { } token
            ? new GlobalPosition(token)
            : null;
    }

    public async Task SetAsync(string name, GlobalPosition position, CancellationToken ct = default)
    {
        var entity = new TableEntity(Partition, name) { ["Token"] = position.Token };
        await checkpoints.UpsertEntityAsync(entity, TableUpdateMode.Replace, ct).ConfigureAwait(false);
    }
}
