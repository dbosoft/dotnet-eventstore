using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Dbosoft.EventStore.Sample.BankAccount;

/// <summary>
/// Drives the sample as a hosted service. It shows the presupposed-bus flow: events are appended to
/// two domains, a relay pumps each domain's <c>$all</c> onto the demo bus, and read models are fed
/// from the bus — including deletion propagation, where a retention truncate on the access log prunes
/// the log read model. Aggregate work runs inside a DI scope, as it would per message in a real app.
/// </summary>
internal sealed class SampleWorker(
    IServiceScopeFactory scopeFactory,
    [FromKeyedServices(SampleWorker.Banking)] IEventStore bankingStore,
    [FromKeyedServices(SampleWorker.Banking)] IEventMaterializer bankingMaterializer,
    [FromKeyedServices(SampleWorker.AccessLog)] IEventStore accessLogStore,
    [FromKeyedServices(SampleWorker.AccessLog)] IEventMaterializer accessLogMaterializer,
    ICheckpointStore checkpoints,
    ILoggerFactory loggerFactory,
    ILogger<SampleWorker> logger,
    IHostApplicationLifetime lifetime) : BackgroundService
{
    public const string Banking = "banking";
    public const string AccessLog = "access-log";

    private const string LogStream = "resource-42";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bus = new InMemoryBus();
        var accounts = new AccountReadModel(loggerFactory.CreateLogger<AccountReadModel>());
        var accessLog = new AccessLogReadModel(loggerFactory.CreateLogger<AccessLogReadModel>());
        using var accountsSub = bus.Subscribe(accounts.Handle);
        using var accessLogSub = bus.Subscribe(accessLog.Handle);

        var bankingRelay = new StoreToBusRelay(
            (IAllStreamReader)bankingStore, bankingMaterializer, checkpoints, bus, "banking->bus");
        var accessLogRelay = new StoreToBusRelay(
            (IAllStreamReader)accessLogStore, accessLogMaterializer, checkpoints, bus, "access-log->bus");

        // Banking: aggregate commands, then relay to the bus so the read model catches up.
        await RunBankingScenarioAsync(stoppingToken);
        await bankingRelay.PumpAsync(stoppingToken);
        logger.LogInformation("account balance is {Balance:C}", accounts.Balance);

        // Access log: append entries, relay them, then apply retention and relay the deletion.
        await AppendAccessEntriesAsync(stoppingToken);
        await accessLogRelay.PumpAsync(stoppingToken);
        logger.LogInformation("access-log entries after appends: {Count}", accessLog.LiveEntries);

        await ApplyRetentionAsync(stoppingToken);
        await accessLogRelay.PumpAsync(stoppingToken);
        logger.LogInformation("access-log entries after retention: {Count}", accessLog.LiveEntries);

        lifetime.StopApplication();
    }

    private async Task RunBankingScenarioAsync(CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IAggregateRepository<BankAccount>>();

        var account = BankAccount.Open("0001", "Ada Lovelace");
        account.Deposit(1000m);
        account.Withdraw(250m);
        await repository.SaveAsync(account, ct);

        // Reload to show the write-side aggregate state (distinct from the bus-fed read model).
        var reloaded = await repository.LoadAsync("0001", ct)
                       ?? throw new InvalidOperationException("Account '0001' was not found.");
        logger.LogInformation(
            "aggregate {Owner}: balance {Balance:C} at version {Version}",
            reloaded.Owner, reloaded.Balance, reloaded.Version);
    }

    private async Task AppendAccessEntriesAsync(CancellationToken ct)
    {
        var entries = Enumerable.Range(1, 5)
            .Select(i => accessLogMaterializer.Dematerialize(new AccessLogged(LogStream, $"user{i}")))
            .ToArray();

        await accessLogStore.AppendToStreamAsync(LogStream, ExpectedVersion.NoStream, entries, ct);
    }

    private async Task ApplyRetentionAsync(CancellationToken ct)
    {
        // Keep only the last two entries. A log is read, not folded, so there is no snapshot to
        // reconstruct from — truncation is forced. The store emits an in-band $stream-truncated marker.
        var maintenance = (IStreamMaintenance)accessLogStore;
        await maintenance.TruncateStreamAsync(LogStream, beforeVersion: 4, force: true, ct);
    }
}
