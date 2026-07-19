namespace Dbosoft.EventStore;

/// <summary>
/// A best-effort "something changed" signal, so a catch-up subscription can wake immediately instead of
/// waiting out its poll interval — without a message bus. It carries no event content and makes <b>no</b>
/// delivery or ordering guarantee: the store stays the source of truth and the subscription's polling is
/// the backstop, so a missed notification only costs a little latency, never an event. That is exactly why
/// it is not a bus, and why any transport will do.
/// </summary>
/// <remarks>
/// Optional and pluggable. The in-process notifier covers a writer and reader in the same process; a
/// cross-process notifier is a thin bridge over whatever ping transport you already run (a message bus used
/// only for the ping, Redis pub/sub, Postgres <c>LISTEN/NOTIFY</c>, …) — one of many possible
/// implementations, provided so it need not be reinvented.
/// </remarks>
[PublicAPI]
public interface IChangeNotifier
{
    /// <summary>Signals that a domain's streams changed. Best-effort — a dropped signal is not an error.</summary>
    ValueTask NotifyAsync(string domain, CancellationToken ct = default);

    /// <summary>Completes when a change is signalled for the domain, or when the token cancels.</summary>
    Task WaitForChangeAsync(string domain, CancellationToken ct = default);
}
