namespace Dbosoft.EventStore;

/// <summary>The outcome of an append.</summary>
/// <param name="NextExpectedVersion">
/// The stream's version after the append operation — the value to pass as <c>expectedVersion</c> next time.
/// </param>
/// <param name="Position">
/// The global position of the last appended event, or <c>null</c> when the append wrote no event —
/// an empty batch, which is a checked no-op. It is also <c>null</c> for a provider that assigns the
/// position only on commit while participating in a caller-owned transaction that has not committed
/// yet; a provider that commits eagerly otherwise always sets it.
/// </param>
[PublicAPI]
public sealed record AppendResult(long NextExpectedVersion, GlobalPosition? Position);
