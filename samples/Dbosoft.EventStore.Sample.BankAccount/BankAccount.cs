using Dbosoft.EventStore.Aggregates;

namespace Dbosoft.EventStore.Sample.BankAccount;

// The events — the only things ever persisted. Payloads are plain records the serializer turns
// into JSON; the type registry maps each to a stable logical name.
public sealed record AccountOpened(string Owner);

public sealed record Deposited(decimal Amount);

public sealed record Withdrawn(decimal Amount);

// The snapshot state, so a long-lived account need not replay from the first event.
public sealed record AccountSnapshot(string Owner, decimal Balance);

/// <summary>
/// A bank account aggregate: it decides what may happen (the public methods), folds events into
/// state (<see cref="When"/>), and can summarize itself into a snapshot. It never touches the store.
/// </summary>
public sealed class BankAccount : AggregateBase, ISnapshotable
{
    public BankAccount(string id) : base(id)
    {
    }

    public string Owner { get; private set; } = "";
    public decimal Balance { get; private set; }

    public static BankAccount Open(string id, string owner)
    {
        var account = new BankAccount(id);
        account.Raise(new AccountOpened(owner));
        return account;
    }

    public void Deposit(decimal amount)
    {
        if (amount <= 0)
            throw new ArgumentOutOfRangeException(nameof(amount), "Deposit must be positive.");
        Raise(new Deposited(amount));
    }

    public void Withdraw(decimal amount)
    {
        if (amount <= 0)
            throw new ArgumentOutOfRangeException(nameof(amount), "Withdrawal must be positive.");
        if (amount > Balance)
            throw new InvalidOperationException($"Cannot withdraw {amount:C}; balance is {Balance:C}.");
        Raise(new Withdrawn(amount));
    }

    protected override void When(object @event)
    {
        switch (@event)
        {
            case AccountOpened opened:
                Owner = opened.Owner;
                break;
            case Deposited deposited:
                Balance += deposited.Amount;
                break;
            case Withdrawn withdrawn:
                Balance -= withdrawn.Amount;
                break;
        }
    }

    public Type SnapshotStateType => typeof(AccountSnapshot);

    public object CreateSnapshotState() => new AccountSnapshot(Owner, Balance);

    public void RestoreFromSnapshot(object state, long version)
    {
        var snapshot = (AccountSnapshot)state;
        Owner = snapshot.Owner;
        Balance = snapshot.Balance;
        SeedVersion(version);
    }
}
