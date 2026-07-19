using Dbosoft.EventStore.Aggregates;

namespace Dbosoft.EventStore.Sample.Projection;

/// <summary>Marker for every event in the banking domain — registered in one call with AddEventTypesImplementing.</summary>
public interface IAccountEvent;

public sealed record AccountOpened(string Owner) : IAccountEvent;

public sealed record MoneyDeposited(decimal Amount) : IAccountEvent;

public sealed record MoneyWithdrawn(decimal Amount) : IAccountEvent;

/// <summary>A tiny account aggregate: it raises delta events and folds them into a balance. It never touches the store.</summary>
public sealed class Account(string id) : AggregateBase(id)
{
    /// <summary>Stream-id prefix (category) for account streams — a stream id is "account-{id}".</summary>
    public const string Category = "account";

    public string Owner { get; private set; } = "";

    public decimal Balance { get; private set; }

    public static Account Open(string id, string owner)
    {
        var account = new Account(id);
        account.Raise(new AccountOpened(owner));
        return account;
    }

    public void Deposit(decimal amount) => Raise(new MoneyDeposited(amount));

    public void Withdraw(decimal amount)
    {
        if (amount > Balance)
            throw new InvalidOperationException($"Account '{Id}' cannot withdraw {amount}; balance is {Balance}.");
        Raise(new MoneyWithdrawn(amount));
    }

    protected override void When(object @event)
    {
        switch (@event)
        {
            case AccountOpened opened: Owner = opened.Owner; break;
            case MoneyDeposited deposited: Balance += deposited.Amount; break;
            case MoneyWithdrawn withdrawn: Balance -= withdrawn.Amount; break;
        }
    }
}
