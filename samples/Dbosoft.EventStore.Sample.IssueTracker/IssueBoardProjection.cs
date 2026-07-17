using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.Logging;

namespace Dbosoft.EventStore.Sample.IssueTracker;

/// <summary>
/// A read model that rebuilds the whole board from the domain's <c>$all</c> stream, folding typed
/// issue events into a per-issue summary. It reads from <see cref="GlobalPosition.Start"/> every time,
/// so on a later process run it shows the issues that a previous run persisted to SQLite — that is the
/// durability the in-memory provider cannot give. It also reacts to the store's
/// <see cref="EventRedactedMarker"/> so a value it had cached is dropped when the payload is scrubbed.
/// </summary>
internal sealed class IssueBoardProjection(
    IAllStreamReader source,
    IEventMaterializer materializer,
    ILogger<IssueBoardProjection> logger)
{
    public async Task RebuildAndPrintAsync(string label, CancellationToken ct = default)
    {
        var board = new Dictionary<string, Entry>(StringComparer.Ordinal);

        await foreach (var stored in source.ReadAllAsync(GlobalPosition.Start, ct: ct).ConfigureAwait(false))
        {
            if (materializer.TryMaterialize(stored, out var payload))
                Apply(board, stored, payload);
        }

        logger.LogInformation("── issue board ({Label}) ──", label);
        foreach (var (id, e) in board.OrderBy(kv => kv.Key, StringComparer.Ordinal))
            logger.LogInformation(
                "  {Id}: \"{Title}\" [{Status}{Assignee}] · reported by {Reporter} · {Comments} comment(s) · last: {Last}",
                id, e.Title, e.Status, e.Assignee is null ? "" : $" → {e.Assignee}", e.Reporter, e.Comments,
                e.LastActivity ?? "—");
    }

    private void Apply(Dictionary<string, Entry> board, StoredEvent stored, object payload)
    {
        switch (payload)
        {
            case IssueOpened opened:
                board[Display(stored.StreamId)] = new Entry { Title = opened.Title, Reporter = opened.Reporter };
                break;
            case IssueCommented commented when board.TryGetValue(Display(stored.StreamId), out var commentedOn):
                commentedOn.Comments++;
                commentedOn.LastActivity = $"{commented.Author}: {commented.Text}";
                break;
            case IssueAssigned assigned when board.TryGetValue(Display(stored.StreamId), out var assignedOn):
                assignedOn.Assignee = assigned.Assignee;
                assignedOn.Status = Issue.Assigned;
                break;
            case IssueResolved resolved when board.TryGetValue(Display(stored.StreamId), out var resolvedOn):
                resolvedOn.Status = Issue.Resolved;
                resolvedOn.LastActivity = $"resolved: {resolved.Resolution}";
                break;
            case IssueReopened reopened when board.TryGetValue(Display(stored.StreamId), out var reopenedOn):
                reopenedOn.Status = Issue.Open;
                reopenedOn.LastActivity = $"reopened: {reopened.Reason}";
                break;
            case EventRedactedMarker redacted when board.TryGetValue(Display(redacted.StreamId), out var redactedOn):
                // The payload is already scrubbed on disk; the marker is how a consumer that cached the
                // old value learns to forget it.
                logger.LogInformation(
                    "  (redaction propagated for {Stream} v{Version} — dropping the cached comment)",
                    Display(redacted.StreamId), redacted.Version);
                redactedOn.LastActivity = "[redacted]";
                break;
        }
    }

    private static string Display(string streamId) =>
        streamId.StartsWith($"{Issue.Category}-", StringComparison.Ordinal)
            ? streamId[(Issue.Category.Length + 1)..]
            : streamId;

    private sealed class Entry
    {
        public string Title { get; init; } = "";
        public string Reporter { get; init; } = "";
        public string Status { get; set; } = Issue.Open;
        public string? Assignee { get; set; }
        public int Comments { get; set; }
        public string? LastActivity { get; set; }
    }
}
