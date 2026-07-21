namespace Dbosoft.EventStore;

/// <summary>
/// Marks a store that can enlist its writes onto a caller-owned transaction — an
/// <see cref="AmbientEventStoreUnitOfWork.Current"/> frame — so its appends commit atomically with the
/// caller's own writes. Availability is a type, as with the other optional capabilities: a store supports
/// enlistment <em>iff</em> it implements this interface, discoverable with an <c>is</c>-cast. Enlistment
/// itself is transparent (a write under an ambient frame joins it automatically); this marker exists so the
/// capability can be probed and the conformance suite gated on it. Only the EF Core provider implements it.
/// </summary>
[PublicAPI]
public interface IEnlistableStore;
