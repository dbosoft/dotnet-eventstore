using AwesomeAssertions;
using Xunit;

namespace Dbosoft.EventStore.Rebus.Tests;

public sealed class StreamOrderingTrackerTests
{
    [Fact]
    public void Classifies_in_order_duplicate_and_gapped_deliveries()
    {
        var tracker = new StreamOrderingTracker();

        // First expected version is 1.
        tracker.Classify("s1", 1).Should().Be(StreamMessageOrder.Next);

        tracker.MarkApplied("s1", 1);
        tracker.Classify("s1", 2).Should().Be(StreamMessageOrder.Next);   // the next one
        tracker.Classify("s1", 1).Should().Be(StreamMessageOrder.Duplicate); // Rebus redelivered v1
        tracker.Classify("s1", 4).Should().Be(StreamMessageOrder.Gap);     // v3 (and v2) not applied yet

        // Streams are independent.
        tracker.Classify("s2", 1).Should().Be(StreamMessageOrder.Next);
    }

    [Fact]
    public void Applied_version_never_moves_backward_and_can_be_seeded()
    {
        var tracker = new StreamOrderingTracker();

        tracker.MarkApplied("s1", 5);
        tracker.MarkApplied("s1", 3); // an out-of-order redelivery must not rewind progress
        tracker.LastApplied("s1").Should().Be(5);
        tracker.Classify("s1", 4).Should().Be(StreamMessageOrder.Duplicate);
        tracker.Classify("s1", 6).Should().Be(StreamMessageOrder.Next);

        // A durable read model seeds from its persisted watermark on startup.
        var restored = new StreamOrderingTracker();
        restored.Seed("s1", 10);
        restored.Classify("s1", 11).Should().Be(StreamMessageOrder.Next);
        restored.Classify("s1", 8).Should().Be(StreamMessageOrder.Duplicate);
    }
}
