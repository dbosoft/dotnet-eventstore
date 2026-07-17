using System.Globalization;

namespace Dbosoft.EventStore;

/// <summary>
/// An opaque, comparable position in the store's global ordering of events. The token's internal
/// format is defined by the provider that produced it (a dense sequence on a relational store, a
/// tick value on a key/value store); callers treat it only as an ordered, serializable value and
/// must not parse it.
/// </summary>
[PublicAPI]
public readonly record struct GlobalPosition(string Token) : IComparable<GlobalPosition>
{
    /// <summary>The position before the first event. Reading from here yields the whole store.</summary>
    public static GlobalPosition Start => new(string.Empty);

    /// <summary>The position after the last event. Nothing is ordered at or after it.</summary>
    public static GlobalPosition End => new("￿");

    /// <summary>
    /// Compares by ordinal token order. Providers emit tokens that sort lexicographically in the
    /// same order events were written, so a plain ordinal comparison reflects global order.
    /// </summary>
    public int CompareTo(GlobalPosition other) =>
        string.CompareOrdinal(Token, other.Token);

    /// <summary>Whether <paramref name="left"/> orders before <paramref name="right"/>.</summary>
    public static bool operator <(GlobalPosition left, GlobalPosition right) => left.CompareTo(right) < 0;

    /// <summary>Whether <paramref name="left"/> orders after <paramref name="right"/>.</summary>
    public static bool operator >(GlobalPosition left, GlobalPosition right) => left.CompareTo(right) > 0;

    /// <summary>Whether <paramref name="left"/> orders at or before <paramref name="right"/>.</summary>
    public static bool operator <=(GlobalPosition left, GlobalPosition right) => left.CompareTo(right) <= 0;

    /// <summary>Whether <paramref name="left"/> orders at or after <paramref name="right"/>.</summary>
    public static bool operator >=(GlobalPosition left, GlobalPosition right) => left.CompareTo(right) >= 0;

    /// <summary>Returns the underlying token.</summary>
    public override string ToString() => Token;

    /// <summary>
    /// Builds a position from a monotonic sequence number, zero-padded so ordinal string order
    /// matches numeric order. Used by providers whose global order is a dense counter.
    /// </summary>
    public static GlobalPosition FromSequence(long sequence) =>
        new(sequence.ToString("D19", CultureInfo.InvariantCulture));
}
