namespace Dbosoft.EventStore.Aggregates;

/// <summary>Decides whether the repository writes a snapshot after an append.</summary>
public interface ISnapshotPolicy
{
    /// <summary>
    /// Returns whether to snapshot, given the stream version before the append and after it.
    /// </summary>
    bool ShouldSnapshot(long versionBefore, long versionAfter);
}

/// <summary>Never snapshots. The default when no snapshot store is configured.</summary>
public sealed class NoSnapshotPolicy : ISnapshotPolicy
{
    /// <summary>A shared instance.</summary>
    public static NoSnapshotPolicy Instance { get; } = new();

    /// <inheritdoc />
    public bool ShouldSnapshot(long versionBefore, long versionAfter) => false;
}

/// <summary>Snapshots whenever the version crosses a multiple of <c>n</c> events.</summary>
public sealed class EveryNEventsSnapshotPolicy : ISnapshotPolicy
{
    private readonly long _n;

    /// <summary>Creates the policy with an interval of <paramref name="n"/> events.</summary>
    public EveryNEventsSnapshotPolicy(long n)
    {
        if (n <= 0)
            throw new ArgumentOutOfRangeException(nameof(n), n, "The snapshot interval must be positive.");
        _n = n;
    }

    /// <inheritdoc />
    public bool ShouldSnapshot(long versionBefore, long versionAfter) =>
        versionAfter / _n > versionBefore / _n;
}
