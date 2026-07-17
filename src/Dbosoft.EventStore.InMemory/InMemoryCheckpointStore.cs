using System.Collections.Concurrent;

namespace Dbosoft.EventStore.InMemory;

/// <summary>In-memory <see cref="ICheckpointStore"/>. Checkpoints are lost when the process ends.</summary>
[PublicAPI]
public sealed class InMemoryCheckpointStore : ICheckpointStore
{
    private readonly ConcurrentDictionary<string, GlobalPosition> _checkpoints = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public Task<GlobalPosition?> GetAsync(string name, CancellationToken ct = default)
    {
        GlobalPosition? position = _checkpoints.TryGetValue(name, out var stored) ? stored : null;
        return Task.FromResult(position);
    }

    /// <inheritdoc />
    public Task SetAsync(string name, GlobalPosition position, CancellationToken ct = default)
    {
        _checkpoints[name] = position;
        return Task.CompletedTask;
    }
}
