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
}
