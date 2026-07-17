namespace Dbosoft.EventStore;

/// <summary>The outcome of an append.</summary>
/// <param name="NextExpectedVersion">
/// The stream's version after the append operation — the value to pass as <c>expectedVersion</c> next time.
/// </param>
/// <param name="Position">
/// The global position of the last appended event. May be <c>null</c> for a provider that assigns
/// the position only on commit, when the append operation participates in a caller-owned transaction
/// that has not committed yet; providers that commit eagerly always set it.
/// </param>
[PublicAPI]
public sealed record AppendResult(long NextExpectedVersion, GlobalPosition? Position);
