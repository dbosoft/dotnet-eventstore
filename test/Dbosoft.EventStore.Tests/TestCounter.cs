using Dbosoft.EventStore.Aggregates;

namespace Dbosoft.EventStore.Tests;

public sealed record Incremented(int By);

public sealed record CounterSnapshot(int Value);

/// <summary>A minimal snapshotable aggregate used by the repository and DI tests.</summary>
public sealed class Counter : AggregateBase, ISnapshotable
{
    public Counter(string id) : base(id)
    {
    }

    public int Value { get; private set; }

    public void Increment(int by) => Raise(new Incremented(by));

    protected override void When(object @event)
    {
        if (@event is Incremented incremented)
            Value += incremented.By;
    }

    public Type SnapshotStateType => typeof(CounterSnapshot);

    public object CreateSnapshotState() => new CounterSnapshot(Value);

    public void RestoreFromSnapshot(object state, long version)
    {
        Value = ((CounterSnapshot)state).Value;
        SeedVersion(version);
    }
}
