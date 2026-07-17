using Dbosoft.EventStore.Aggregates;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Dbosoft.EventStore.Sample.BankAccount;

/// <summary>
/// Drives the sample scenario as a hosted service: it starts a projection over the global stream,
/// runs a few account commands under optimistic concurrency, then stops the host. Aggregate work
/// runs inside a DI scope, as it would per request/message in a real application.
/// </summary>
internal sealed class BankAccountWorker(
    IServiceScopeFactory scopeFactory,
    ISubscriptionSource subscriptions,
    IAllStreamReader allStream,
    ILoggerFactory loggerFactory,
    ILogger<BankAccountWorker> logger,
    IHostApplicationLifetime lifetime) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var projection = new ConsoleProjection(loggerFactory.CreateLogger<ConsoleProjection>());
        await using var subscription = subscriptions.SubscribeToAll(GlobalPosition.Start, projection);

        await RunScenarioAsync(stoppingToken);

        // Let the asynchronous subscription drain before shutting the host down.
        await Task.Delay(TimeSpan.FromMilliseconds(200), stoppingToken);

        var total = 0;
        await foreach (var _ in allStream.ReadAllAsync(GlobalPosition.Start, ct: stoppingToken))
            total++;
        logger.LogInformation("Global stream holds {Count} events in total.", total);

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

    private static async Task<BankAccount> Load(IAggregateRepository<BankAccount> repository, string id, CancellationToken ct) =>
        await repository.LoadAsync(id, ct) ?? throw new InvalidOperationException($"Account '{id}' was not found.");
}
