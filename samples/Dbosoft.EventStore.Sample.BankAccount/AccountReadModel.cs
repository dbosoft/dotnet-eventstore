using Microsoft.Extensions.Logging;

namespace Dbosoft.EventStore.Sample.BankAccount;

/// <summary>
/// A read model rebuilt by replaying the domain's events. It folds *typed* events — the materializer
/// turned the stored bytes back into <see cref="AccountOpened"/> / <see cref="Deposited"/> /
/// <see cref="Withdrawn"/> — which is exactly what a projection or a bus handler consumes.
/// </summary>
internal sealed class AccountReadModel(ILogger<AccountReadModel> logger)
{
    private decimal _balance;

    public void Apply(object @event)
    {
        switch (@event)
        {
            case AccountOpened opened:
                logger.LogInformation("read model: account opened by {Owner}", opened.Owner);
                break;
            case Deposited deposited:
                _balance += deposited.Amount;
                logger.LogInformation("read model: +{Amount:C} → balance {Balance:C}", deposited.Amount, _balance);
                break;
            case Withdrawn withdrawn:
                _balance -= withdrawn.Amount;
                logger.LogInformation("read model: -{Amount:C} → balance {Balance:C}", withdrawn.Amount, _balance);
                break;
        }
    }
}
