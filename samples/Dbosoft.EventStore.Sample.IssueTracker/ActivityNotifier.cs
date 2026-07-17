using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.Logging;

namespace Dbosoft.EventStore.Sample.IssueTracker;

/// <summary>
/// A durable catch-up consumer: it "notifies" once per new event and remembers how far it got in an
/// <see cref="ICheckpointStore"/>. Because the checkpoint is persisted by the EF Core provider, a
/// second process run resumes from where the first left off and only sees the events added since — the
/// at-least-once delivery position survives restarts, which is the whole point of a durable checkpoint.
/// </summary>
internal sealed class ActivityNotifier(
    IAllStreamReader source,
    IEventMaterializer materializer,
    ICheckpointStore checkpoints,
    ILogger<ActivityNotifier> logger)
{
    private const string CheckpointName = "issue-activity-notifier";

    public async Task RunAsync(CancellationToken ct = default)
    {
        var from = await checkpoints.GetAsync(CheckpointName, ct).ConfigureAwait(false) ?? GlobalPosition.Start;
        var last = from;
        var count = 0;

        await foreach (var stored in source.ReadAllAsync(from, ct: ct).ConfigureAwait(false))
        {
            if (materializer.TryMaterialize(stored, out var payload))
                logger.LogInformation("  notify: {Event} on {Stream}", payload.GetType().Name, Display(stored.StreamId));
            last = stored.Position;
            count++;
        }

        if (count > 0)
            await checkpoints.SetAsync(CheckpointName, last, ct).ConfigureAwait(false);

        logger.LogInformation("notifier processed {Count} new event(s) since its last run", count);
    }

    private static string Display(string streamId) =>
        streamId.StartsWith($"{Issue.Category}-", StringComparison.Ordinal)
            ? streamId[(Issue.Category.Length + 1)..]
            : streamId;
}
