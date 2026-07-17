using System.Text;
using Dbosoft.EventStore.Serialization;

namespace Dbosoft.EventStore.Sample.IssueTracker;

/// <summary>What a notifier pass delivered — enough for the presenter to narrate the resume story.</summary>
/// <param name="Resumed">Whether it started from a previously saved checkpoint.</param>
/// <param name="Count">How many events were delivered this pass.</param>
/// <param name="Lines">One human-readable line per stream, summarizing the events delivered.</param>
internal sealed record NotifyResult(bool Resumed, int Count, IReadOnlyList<string> Lines);

/// <summary>
/// A durable catch-up consumer: it "notifies" once per new event and remembers how far it got in an
/// <see cref="ICheckpointStore"/>. Because the checkpoint is persisted by the EF Core provider, a
/// second process run resumes from where the first left off and only sees the events added since — the
/// delivery position survives restarts, which is the whole point of a durable checkpoint.
/// </summary>
internal sealed class ActivityNotifier(
    IAllStreamReader source,
    IEventMaterializer materializer,
    ICheckpointStore checkpoints)
{
    private const string CheckpointName = "issue-activity-notifier";

    public async Task<NotifyResult> RunAsync(CancellationToken ct = default)
    {
        var saved = await checkpoints.GetAsync(CheckpointName, ct).ConfigureAwait(false);
        var from = saved ?? GlobalPosition.Start;
        var last = from;
        var count = 0;

        var streams = new List<string>();
        var byStream = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        await foreach (var stored in source.ReadAllAsync(from, ct: ct).ConfigureAwait(false))
        {
            count++;
            last = stored.Position;
            if (!materializer.TryMaterialize(stored, out var payload))
                continue;

            var stream = Display(stored.StreamId);
            if (!byStream.TryGetValue(stream, out var events))
            {
                events = [];
                byStream[stream] = events;
                streams.Add(stream);
            }

            events.Add(payload.GetType().Name);
        }

        if (count > 0)
            await checkpoints.SetAsync(CheckpointName, last, ct).ConfigureAwait(false);

        var lines = streams.Select(s => $"{s} · {Summarize(byStream[s])}").ToList();
        return new NotifyResult(saved is not null, count, lines);
    }

    // Collapses runs of the same event type into "Type ×N", keeping order: e.g. nine comments in a row
    // read as "IssueCommented ×9" instead of nine identical entries.
    private static string Summarize(IReadOnlyList<string> events)
    {
        var result = new StringBuilder();
        for (var i = 0; i < events.Count;)
        {
            var run = 1;
            while (i + run < events.Count && events[i + run] == events[i])
                run++;

            if (result.Length > 0)
                result.Append(", ");
            result.Append(events[i]);
            if (run > 1)
                result.Append(" ×").Append(run);
            i += run;
        }

        return result.ToString();
    }

    private static string Display(string streamId) =>
        streamId.StartsWith($"{Issue.Category}-", StringComparison.Ordinal)
            ? streamId[(Issue.Category.Length + 1)..]
            : streamId;
}
