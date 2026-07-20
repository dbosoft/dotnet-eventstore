using System.Collections.Concurrent;

namespace AuctionHouse.Contracts;

/// <summary>
/// The shared read model. The projection module writes into it as it folds the event stream; the UI module
/// reads it and re-renders when <see cref="Changed"/> fires. It is a cross-cutting resource registered once
/// on the shared host container — the analogue of a shared read database, kept in memory for the sample.
/// </summary>
public interface ILotReadStore
{
    /// <summary>Raised after any change, so the UI can re-render live.</summary>
    event Action? Changed;

    IReadOnlyList<LotView> All();
    LotView? Get(string lotId);

    /// <summary>Insert or replace a lot's view (called by the projection).</summary>
    void Upsert(LotView lot);

    /// <summary>Drop everything — used when the projection rebuilds from the start of the stream.</summary>
    void Clear();
}

/// <summary>An in-memory <see cref="ILotReadStore"/>. Thread-safe; raises <see cref="Changed"/> on every mutation.</summary>
public sealed class InMemoryLotReadStore : ILotReadStore
{
    private readonly ConcurrentDictionary<string, LotView> _lots = new(StringComparer.Ordinal);

    public event Action? Changed;

    public IReadOnlyList<LotView> All() =>
        _lots.Values.OrderBy(l => l.LotId, StringComparer.Ordinal).ToArray();

    public LotView? Get(string lotId) => _lots.GetValueOrDefault(lotId);

    public void Upsert(LotView lot)
    {
        _lots[lot.LotId] = lot;
        Changed?.Invoke();
    }

    public void Clear()
    {
        _lots.Clear();
        Changed?.Invoke();
    }
}
