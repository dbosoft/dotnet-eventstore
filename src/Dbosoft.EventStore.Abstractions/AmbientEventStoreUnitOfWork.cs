namespace Dbosoft.EventStore;

/// <summary>
/// The ambient <see cref="IEventStoreUnitOfWork"/> for the current async flow. An enlisting store reads
/// <see cref="Current"/> to decide whether to join a caller's transaction instead of opening its own. The
/// slot flows with the async context (an <see cref="System.Threading.AsyncLocal{T}"/>), mirroring how Rebus
/// carries its transaction context, so a caller opens a frame once and every append underneath it enlists
/// without threading it through call signatures.
/// </summary>
[PublicAPI]
public static class AmbientEventStoreUnitOfWork
{
    private static readonly AsyncLocal<IEventStoreUnitOfWork?> Slot = new();

    /// <summary>
    /// The open frame on this async flow, or <c>null</c> if there is none. A frame that has committed,
    /// rolled back, or been disposed reports <see cref="IEventStoreUnitOfWork.IsActive"/> false and is
    /// treated as absent here — so a stale reference left in the async-local slot never re-enlists a write
    /// onto a spent transaction.
    /// </summary>
    public static IEventStoreUnitOfWork? Current => Slot.Value is { IsActive: true } uow ? uow : null;

    /// <summary>
    /// Sets (or clears, with <c>null</c>) the ambient frame. Infrastructure use — a
    /// <see cref="IEventStoreUnitOfWorkFactory"/> sets it when it enlists and clears it when the frame ends.
    /// </summary>
    public static void SetCurrent(IEventStoreUnitOfWork? unitOfWork) => Slot.Value = unitOfWork;
}
