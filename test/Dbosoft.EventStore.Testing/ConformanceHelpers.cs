using System.Text;

namespace Dbosoft.EventStore.Testing;

/// <summary>Shared helpers for the conformance tests.</summary>
public static class ConformanceHelpers
{
    /// <summary>Builds a test event with a fresh id and a small JSON payload.</summary>
    public static EventData Event(string eventType = "TestEvent", string json = "{}") =>
        new(Guid.NewGuid(), eventType, Encoding.UTF8.GetBytes(json));

    /// <summary>Builds a batch of test events.</summary>
    public static IReadOnlyList<EventData> Events(int count, string eventType = "TestEvent") =>
        Enumerable.Range(0, count).Select(i => Event(eventType, $"{{\"i\":{i}}}")).ToList();

    /// <summary>Reads a whole stream forwards into a list.</summary>
    public static async Task<List<StoredEvent>> ReadAllOf(this IEventStore store, string streamId)
    {
        var result = new List<StoredEvent>();
        await foreach (var e in store.ReadStreamAsync(streamId))
            result.Add(e);
        return result;
    }

    /// <summary>Polls until a condition holds or the timeout elapses.</summary>
    public static async Task WaitUntil(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition was not met within the timeout.");
            await Task.Delay(10);
        }
    }

    /// <summary>An event handler that records everything it receives, in order.</summary>
    public sealed class RecordingHandler : IEventHandler
    {
        private readonly List<StoredEvent> _received = [];
        private readonly Lock _lock = new();

        public IReadOnlyList<StoredEvent> Received
        {
            get
            {
                lock (_lock)
                    return _received.ToList();
            }
        }

        public Task HandleAsync(StoredEvent @event, CancellationToken ct = default)
        {
            lock (_lock)
                _received.Add(@event);
            return Task.CompletedTask;
        }
    }
}
