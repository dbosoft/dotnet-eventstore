namespace Dbosoft.EventStore.Tests;

/// <summary>A hand-advanced clock, so a test can give successive events distinct, known timestamps.</summary>
internal sealed class MutableClock(DateTimeOffset now) : TimeProvider
{
    public DateTimeOffset Now { get; private set; } = now;

    public override DateTimeOffset GetUtcNow() => Now;

    public void Advance(TimeSpan by) => Now += by;
}
