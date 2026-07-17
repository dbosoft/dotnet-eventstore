namespace Dbosoft.EventStore;

/// <summary>
/// Gives access to a store per domain. A domain is an independent, ordered log mapped to its own
/// storage unit (a table, a partition, an in-memory area). There is deliberately no ordering across
/// domains: each has its own version and <see cref="GlobalPosition"/> sequence, its own global read
/// and its own subscriptions. Group events that belong together into one domain.
/// </summary>
[PublicAPI]
public interface IEventStoreProvider
{
    /// <summary>The domains registered with this provider.</summary>
    IReadOnlyCollection<string> Domains { get; }

    /// <summary>Returns the event store for a domain.</summary>
    IEventStore GetStore(string domain);

    /// <summary>Returns the snapshot store for a domain, or <c>null</c> if the provider has none.</summary>
    ISnapshotStore? GetSnapshotStore(string domain);
}
