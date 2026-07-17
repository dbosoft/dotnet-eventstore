using Dbosoft.EventStore.Aggregates;

namespace Dbosoft.EventStore.Sample.IssueTracker;

/// <summary>
/// Marker (contract) for every event in the tracking domain. Registering the domain with
/// <c>AddEventTypesImplementing&lt;IIssueEvent&gt;()</c> picks all of them up at once.
/// </summary>
public interface IIssueEvent;

// The events — the only things ever persisted. Plain records the serializer turns into JSON; the
// type registry maps each to a stable logical name.
public sealed record IssueOpened(string Title, string Reporter) : IIssueEvent;

public sealed record IssueCommented(string Author, string Text) : IIssueEvent;

public sealed record IssueAssigned(string Assignee) : IIssueEvent;

public sealed record IssueResolved(string Resolution) : IIssueEvent;

public sealed record IssueReopened(string Reason) : IIssueEvent;

// Snapshot state, so a long, chatty issue need not replay from its first event on every load.
public sealed record IssueSnapshot(string Title, string Status, string? Assignee, int CommentCount);

/// <summary>
/// An issue aggregate: it decides what may happen (the public methods), folds events into state
/// (<see cref="When"/>), and can summarize itself into a snapshot. It never touches the store.
/// </summary>
public sealed class Issue(string id) : AggregateBase(id), ISnapshotable
{
    /// <summary>Stream-id prefix (category) for issue aggregates within the tracking domain.</summary>
    public const string Category = "issue";

    public const string Open = "open";
    public const string Assigned = "assigned";
    public const string Resolved = "resolved";

    public string Title { get; private set; } = "";
    public string Status { get; private set; } = Open;
    public string? Assignee { get; private set; }
    public int CommentCount { get; private set; }

    public static Issue Report(string id, string title, string reporter)
    {
        var issue = new Issue(id);
        issue.Raise(new IssueOpened(title, reporter));
        return issue;
    }

    public void Comment(string author, string text) => Raise(new IssueCommented(author, text));

    public void AssignTo(string assignee) => Raise(new IssueAssigned(assignee));

    public void Resolve(string resolution)
    {
        if (Status == Resolved)
            throw new InvalidOperationException($"Issue '{Id}' is already resolved.");
        Raise(new IssueResolved(resolution));
    }

    public void Reopen(string reason)
    {
        if (Status != Resolved)
            throw new InvalidOperationException($"Issue '{Id}' is not resolved, so it cannot be reopened.");
        Raise(new IssueReopened(reason));
    }

    protected override void When(object @event)
    {
        switch (@event)
        {
            case IssueOpened opened:
                Title = opened.Title;
                Status = Open;
                break;
            case IssueCommented:
                CommentCount++;
                break;
            case IssueAssigned assigned:
                Assignee = assigned.Assignee;
                Status = Assigned;
                break;
            case IssueResolved:
                Status = Resolved;
                break;
            case IssueReopened:
                Status = Open;
                break;
        }
    }

    public Type SnapshotStateType => typeof(IssueSnapshot);

    public object CreateSnapshotState() => new IssueSnapshot(Title, Status, Assignee, CommentCount);

    public void RestoreFromSnapshot(object state, long version)
    {
        var snapshot = (IssueSnapshot)state;
        Title = snapshot.Title;
        Status = snapshot.Status;
        Assignee = snapshot.Assignee;
        CommentCount = snapshot.CommentCount;
        SeedVersion(version);
    }
}
