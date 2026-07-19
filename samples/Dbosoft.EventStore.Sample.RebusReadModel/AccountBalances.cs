using System.Collections.Concurrent;
using Dbosoft.EventStore.Rebus;

namespace Dbosoft.EventStore.Sample.RebusReadModel;

/// <summary>
/// The read model: a current balance per account, derived purely from events applied in order. The same
/// <see cref="Apply"/> is used whether an event arrives live (via the ordered dispatcher) or during a rebuild
/// (replayed from the store), so the model is a pure function of the event stream — which is what makes it
/// rebuildable.
/// </summary>
public sealed class AccountBalances
{
    private readonly ConcurrentDictionary<string, decimal> _balances = new(StringComparer.Ordinal);

    public void Apply(object @event, EventStoreMetadata meta) =>
        _balances.AddOrUpdate(meta.StreamId, _ => Delta(@event), (_, current) => current + Delta(@event));

    public void Clear() => _balances.Clear();

    public IEnumerable<KeyValuePair<string, decimal>> Snapshot() => _balances.OrderBy(entry => entry.Key);

    private static decimal Delta(object @event) =>
        @event switch
        {
            MoneyDeposited deposited => deposited.Amount,
            MoneyWithdrawn withdrawn => -withdrawn.Amount,
            _ => 0m // AccountOpened and anything else does not move the balance
        };
}
