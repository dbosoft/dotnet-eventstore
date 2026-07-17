namespace Dbosoft.EventStore;

/// <summary>The outcome of an append.</summary>
/// <param name="NextExpectedVersion">
/// The stream's version after the append — the value to pass as <c>expectedVersion</c> next time.
/// </param>
/// <param name="Position">
/// The global position of the last appended event, or <c>null</c> when the append was deferred to a
/// caller-owned transaction and the position is not yet assigned. See <c>CommitMode.Deferred</c>.
/// </param>
public sealed record AppendResult(long NextExpectedVersion, GlobalPosition? Position);
