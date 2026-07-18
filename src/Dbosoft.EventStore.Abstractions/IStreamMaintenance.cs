namespace Dbosoft.EventStore;

/// <summary>
/// The full set of destructive operations, aggregating the individual role interfaces
/// (<see cref="ITruncatable"/>, <see cref="IStreamDeletable"/>, <see cref="IRedactable"/>,
/// <see cref="IScavengeable"/>). A provider that supports every destructive operation implements this;
/// one that supports only some implements just those role interfaces. Callers that need a single
/// operation should depend on its role interface — availability is the presence of that interface, not
/// a capability flag. Each destructive operation emits a system marker event into <c>$all</c> (see
/// <see cref="SystemEventTypes"/>) so read models over the bus learn to forget or reproject.
/// </summary>
[PublicAPI]
public interface IStreamMaintenance : ITruncatable, IStreamDeletable, IRedactable, IScavengeable;
