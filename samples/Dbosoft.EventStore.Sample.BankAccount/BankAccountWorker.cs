using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Dbosoft.EventStore.Sample.BankAccount;

/// <summary>
/// Drives the sample as a hosted service: it runs a few account commands under optimistic
/// concurrency, then rebuilds a read model by replaying the domain's global stream through the
/// materializer. In production, live events reach read models over a message bus — the store does
/// not deliver — so replay is how you build and rebuild a read model. Aggregate work runs inside a
/// DI scope, as it would per request/message in a real application.
/// </summary>
internal sealed class BankAccountWorker(
    IServiceScopeFactory scopeFactory,
    [FromKeyedServices(BankAccountWorker.Domain)] IEventStore bankingStore,
    [FromKeyedServices(BankAccountWorker.Domain)] IEventMaterializer materializer,
    ICheckpointStore checkpoints,
    ILoggerFactory loggerFactory,
    ILogger<BankAccountWorker> logger,
    IHostApplicationLifetime lifetime) : BackgroundService
{
    /// <summary>The domain the banking events live in.</summary>
    public const string Domain = "banking";

    private const string ReadModelCheckpoint = "banking/account-read-model";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await RunScenarioAsync(stoppingToken);
        await RebuildReadModelAsync(stoppingToken);
        lifetime.StopApplication();
    }

    private async Task RunScenarioAsync(CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IAggregateRepository<BankAccount>>();

        var account = BankAccount.Open("0001", "Ada Lovelace");
        account.Deposit(1000m);
        account.Withdraw(250m);
        await repository.SaveAsync(account, ct);

        // A handful of further deposits; crossing five events triggers a snapshot.
        for (var i = 0; i < 4; i++)
        {
            var reloaded = await Load(repository, "0001", ct);
            reloaded.Deposit(100m);
            await repository.SaveAsync(reloaded, ct);
        }

        var current = await Load(repository, "0001", ct);
        logger.LogInformation(
            "Account {Id} owned by {Owner}: balance {Balance:C} at version {Version}.",
            current.Id, current.Owner, current.Balance, current.Version);

        // Optimistic concurrency: two callers load the same version, both write, one loses.
        var first = await Load(repository, "0001", ct);
        var second = await Load(repository, "0001", ct);
        first.Deposit(10m);
        second.Deposit(20m);
        await repository.SaveAsync(first, ct);
        try
        {
            await repository.SaveAsync(second, ct);
        }
        catch (WrongExpectedVersionException ex)
        {
            logger.LogWarning(
                "Concurrency conflict as expected: expected v{Expected}, stream was at v{Actual}.",
                ex.ExpectedVersion, ex.ActualVersion);
        }
    }

    private async Task RebuildReadModelAsync(CancellationToken ct)
    {
        var allStream = (IAllStreamReader)bankingStore;
        var readModel = new AccountReadModel(loggerFactory.CreateLogger<AccountReadModel>());

        var checkpoint = await checkpoints.GetAsync(ReadModelCheckpoint, ct) ?? GlobalPosition.Start;
        var last = checkpoint;
        await foreach (var stored in allStream.ReadAllAsync(checkpoint, ct: ct))
        {
            readModel.Apply(materializer.Materialize(stored));
            last = stored.Position;
        }

        await checkpoints.SetAsync(ReadModelCheckpoint, last, ct);
    }

    private static async Task<BankAccount> Load(IAggregateRepository<BankAccount> repository, string id, CancellationToken ct) =>
        await repository.LoadAsync(id, ct) ?? throw new InvalidOperationException($"Account '{id}' was not found.");
}
