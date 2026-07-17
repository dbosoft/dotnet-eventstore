using Dbosoft.EventStore.Serialization;

namespace Dbosoft.EventStore.Sample.IssueTracker;

/// <summary>One row of the rendered board — a per-issue summary the presenter turns into a table.</summary>
internal sealed record BoardRow(
    string Id,
    string Title,
    string Status,
    string Reporter,
    string? Assignee,
    int Comments,
    string LastActivity);

/// <summary>
/// A read model that rebuilds the whole board from the domain's <c>$all</c> stream, folding typed
/// issue events into a per-issue summary. It reads from <see cref="GlobalPosition.Start"/> every time,
/// so on a later process run it shows the issues a previous run persisted to SQLite — the durability
/// the in-memory provider cannot give. It also reacts to the store's <see cref="EventRedactedMarker"/>,
/// so a value it had cached is dropped when the payload is scrubbed. It returns data; the presenter
/// decides how to render it.
/// </summary>
internal sealed class IssueBoardProjection(IAllStreamReader source, IEventMaterializer materializer)
{
    public async Task<IReadOnlyList<BoardRow>> RebuildAsync(CancellationToken ct = default)
    {
        var board = new Dictionary<string, Entry>(StringComparer.Ordinal);
        var order = new List<string>();

        await foreach (var stored in source.ReadAllAsync(GlobalPosition.Start, ct: ct).ConfigureAwait(false))
        {
            if (materializer.TryMaterialize(stored, out var payload))
                Apply(board, order, stored, payload);
        }

        return order.Select(id =>
        {
            var e = board[id];
            return new BoardRow(id, e.Title, e.Status, e.Reporter, e.Assignee, e.Comments, e.LastActivity ?? "—");
        }).ToList();
    }

    private static void Apply(Dictionary<string, Entry> board, List<string> order, StoredEvent stored, object payload)
    {
        switch (payload)
        {
            case IssueOpened opened:
                var id = Display(stored.StreamId);
                if (board.TryAdd(id, new Entry { Title = opened.Title, Reporter = opened.Reporter }))
                    order.Add(id);
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
