using System.Collections.Concurrent;
using System.Globalization;

namespace Dbosoft.EventStore.Sample.Projection;

/// <summary>
/// The read model: a full <b>statement</b> per account — every transaction with its running balance. This
/// is the point of keeping deltas: the read model chooses to build history, which a latest-state feed
/// could never reconstruct. It is a pure fold of the ordered event stream, so it can be rebuilt from zero.
/// </summary>
public sealed class AccountStatements
{
    private readonly ConcurrentDictionary<string, List<string>> _lines = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, decimal> _balance = new(StringComparer.Ordinal);

    public void Apply(string streamId, object @event)
    {
        switch (@event)
        {
            case AccountOpened opened:
                _balance[streamId] = 0m;
                Line(streamId, $"opened for {opened.Owner}", 0m);
                break;
            case MoneyDeposited deposited:
                Line(streamId, $"deposit  {deposited.Amount,7}", Move(streamId, deposited.Amount));
                break;
            case MoneyWithdrawn withdrawn:
                Line(streamId, $"withdraw {-withdrawn.Amount,7}", Move(streamId, -withdrawn.Amount));
                break;
        }
    }

    public void Clear()
    {
        _lines.Clear();
        _balance.Clear();
    }

    public IEnumerable<KeyValuePair<string, List<string>>> Statements() => _lines.OrderBy(entry => entry.Key);

    private decimal Move(string streamId, decimal delta) =>
        _balance.AddOrUpdate(streamId, delta, (_, current) => current + delta);

    private void Line(string streamId, string description, decimal runningBalance) =>
        _lines.GetOrAdd(streamId, _ => []).Add(
            $"{description}   → {runningBalance.ToString("0.00", CultureInfo.InvariantCulture)}");
}
