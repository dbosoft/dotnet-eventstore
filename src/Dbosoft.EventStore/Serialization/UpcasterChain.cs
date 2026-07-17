namespace Dbosoft.EventStore.Serialization;

/// <summary>
/// Applies a set of <see cref="IEventUpcaster"/> to an event, repeating until no upcaster matches so
/// multi-step upgrades (v1→v2→v3) compose. A hop limit guards against upcasters that keep matching.
/// </summary>
public sealed class UpcasterChain
{
    private readonly IReadOnlyList<IEventUpcaster> _upcasters;
    private const int MaxHops = 32;

    /// <summary>Creates a chain from the given upcasters, applied in order each pass.</summary>
    public UpcasterChain(IEnumerable<IEventUpcaster> upcasters) =>
        _upcasters = upcasters.ToArray();

    /// <summary>Upgrades an event to its current shape.</summary>
    public object Upcast(object @event)
    {
        if (_upcasters.Count == 0)
            return @event;

        for (var hop = 0; hop < MaxHops; hop++)
        {
            var upcaster = _upcasters.FirstOrDefault(u => u.CanUpcast(@event));
            if (upcaster is null)
                return @event;

            @event = upcaster.Upcast(@event);
        }

        throw new InvalidOperationException(
            $"Upcasting did not converge after {MaxHops} hops; an upcaster likely keeps matching its own output.");
    }
}
