using Dbosoft.EventStore.InMemory;
using Microsoft.Extensions.Logging;

namespace Dbosoft.EventStore.Sample.BankAccount;

/// <summary>
/// A read model fed from the bus. It folds *typed* payloads — the relay materialized the stored
/// bytes back into <see cref="AccountOpened"/> / <see cref="Deposited"/> / <see cref="Withdrawn"/>.
/// </summary>
internal sealed class AccountReadModel(ILogger<AccountReadModel> logger)
{
    public decimal Balance { get; private set; }

    public Task Handle(BusMessage message, CancellationToken ct = default)
    {
        switch (message.Payload)
        {
            case AccountOpened opened:
                logger.LogInformation("read model: account opened by {Owner}", opened.Owner);
                break;
            case Deposited deposited:
                Balance += deposited.Amount;
                break;
            case Withdrawn withdrawn:
                Balance -= withdrawn.Amount;
                break;
        }

        return Task.CompletedTask;
    }
}
