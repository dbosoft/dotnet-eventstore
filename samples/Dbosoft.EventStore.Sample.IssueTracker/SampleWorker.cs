using System.Globalization;
using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Dbosoft.EventStore.Sample.IssueTracker;

/// <summary>
/// Drives the sample as a hosted service against on-disk SQLite, presenting the run as a readable
/// narrative (see <see cref="Report"/>). Every run opens a couple of new issues with randomly varied
/// content and touches an existing one, so the board grows and looks different each time. State
/// persists between runs: the board is rebuilt in full (proving earlier runs survived), an issue is
/// loaded from its snapshot, a leaked email is redacted in place, and the durable notifier resumes
/// from its saved checkpoint to deliver only what is new. Aggregate work runs inside a DI scope, as it
/// would per message in a real app.
/// </summary>
internal sealed class SampleWorker(
    IServiceScopeFactory scopeFactory,
    [FromKeyedServices(SampleWorker.Tracking)] IEventStore store,
    [FromKeyedServices(SampleWorker.Tracking)] IEventMaterializer materializer,
    ICheckpointStore checkpoints,
    IHostApplicationLifetime lifetime) : BackgroundService
{
    public const string Tracking = "tracking";
    public const string DatabaseFile = "issues.db";
    public const int SnapshotInterval = 10;

    private static readonly string[] Titles =
    [
        "Login page returns 500", "Cannot reset password", "Timeout exporting large reports",
        "Search returns stale results", "Mobile layout overlaps the footer", "Webhook retries never stop",
        "CSV import silently drops rows", "Dark mode flickers on load", "Session expires too early",
        "Duplicate emails on signup", "Pagination skips the last page", "Uploads fail over 10 MB"
    ];

    private static readonly string[] People =
        ["grace", "ada", "linus", "mallory", "hedy", "alan", "edsger", "radia", "katherine"];

    private static readonly string[] CommentTexts =
    [
        "reproduced on staging", "cannot reproduce locally", "added more logging", "looks like a race",
        "needs a database migration", "waiting on review", "pushed a tentative fix", "reverted, made it worse",
        "narrowed it to the cache layer", "customer confirmed the workaround", "bisected to last week's deploy"
    ];

    private static readonly string[] Resolutions =
    [
        "deployed a hotfix", "corrected the configuration", "duplicate of an earlier report",
        "fixed in the nightly build", "resolved by the cache rewrite"
    ];

    private static readonly string[] ReopenReasons =
        ["it resurfaced after the latest deploy", "a customer hit it again", "the fix did not hold"];

    private readonly Random _rng = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var board = new IssueBoardProjection((IAllStreamReader)store, materializer);
        var notifier = new ActivityNotifier((IAllStreamReader)store, materializer, checkpoints);

        var existing = await board.RebuildAsync(stoppingToken);

        Report.Banner("Issue Tracker", "Event sourcing on the EF Core / SQLite provider");
        Report.KeyValue("Database", existing.Count == 0
            ? $"{DatabaseFile}  (fresh)"
            : $"{DatabaseFile}  (existing — {existing.Count} issue(s) from earlier runs)");

        if (existing.Count > 0)
        {
            Report.Section("Updating an existing issue");
            await UpdateRandomIssueAsync(existing, stoppingToken);
        }

        Report.Section("Opening new issues");
        var leak = await OpenRandomIssuesAsync(NextIssueNumber(existing), stoppingToken);

        Report.Board("Board", await board.RebuildAsync(stoppingToken));

        if (leak is { } spill)
        {
            Report.Warn($"{spill.Issue} / v{spill.Version} leaked an email address — redacting it in place …");
            var scrubbed = materializer.Dematerialize(new IssueCommented(spill.Author, "[redacted]")).Data;
            await ((IStreamMaintenance)store).RedactEventAsync(StreamId(spill.Issue), spill.Version, scrubbed, ct: stoppingToken);
            Report.Board("Board after redaction — the payload was scrubbed on disk", await board.RebuildAsync(stoppingToken));
        }

        var delivered = await notifier.RunAsync(stoppingToken);
        Report.Notifications(delivered.Resumed, delivered.Count, delivered.Lines);

        Report.Outro($"State is persisted in {DatabaseFile} — run the sample again to grow the board further.");

        lifetime.StopApplication();
    }

    private async Task UpdateRandomIssueAsync(IReadOnlyList<BoardRow> existing, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IAggregateRepository<Issue>>();

        var target = existing[_rng.Next(existing.Count)];
        var issue = await repository.LoadAsync(target.Id, ct);
        if (issue is null)
            return;

        var before = issue.Version;
        var assignee = issue.Assignee is null ? "" : $", assignee {issue.Assignee}";
        Report.Bullet(
            $"Loaded {issue.Id} \"{issue.Title}\" at v{issue.Version} [{issue.Status}, {issue.CommentCount} comment(s)"
            + $"{assignee}] — "
            + (before >= SnapshotInterval ? "rehydrated from its snapshot plus tail." : "replayed from its events."));

        if (issue.Status == Issue.Resolved)
        {
            issue.Reopen(Pick(ReopenReasons));
            Report.Note($"{issue.Id} was resolved → reopened it.");
        }

        var added = _rng.Next(1, 4);
        for (var i = 0; i < added; i++)
            issue.Comment(Pick(People), Pick(CommentTexts));
        await repository.SaveAsync(issue, ct);

        var snapshotted = Crossed(before, issue.Version) ? " (a snapshot was taken)" : "";
        Report.Note($"Added {added} comment(s); {issue.Id} is now at v{issue.Version}{snapshotted}.");
    }

    private async Task<Leak?> OpenRandomIssuesAsync(int startNumber, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IAggregateRepository<Issue>>();

        var count = _rng.Next(2, 4);
        var leakIndex = _rng.Next(count);
        Leak? leak = null;

        for (var i = 0; i < count; i++)
        {
            var id = $"ISSUE-{startNumber + i}";
            var reporter = Pick(People);
            var issue = Issue.Report(id, Pick(Titles), reporter);

            var comments = _rng.Next(0, 5);
            for (var c = 0; c < comments; c++)
                issue.Comment(Pick(People), Pick(CommentTexts));

            if (i == leakIndex)
            {
                var author = Pick(People);
                issue.Comment(author, $"reach me at {reporter}@example.com if you need more detail");
                leak = new Leak(id, issue.Version, author);
                comments++;
            }

            string? assignee = null;
            if (_rng.Next(2) == 0)
                issue.AssignTo(assignee = Pick(People));

            var resolved = _rng.Next(3) == 0;
            if (resolved)
                issue.Resolve(Pick(Resolutions));

            await repository.SaveAsync(issue, ct);
            Report.Bullet($"Opened {id} \"{issue.Title}\" ({reporter}) · {Describe(comments, assignee, resolved)}");
            if (issue.Version >= SnapshotInterval)
                Report.Note($"→ {id} crossed the snapshot interval; a snapshot was taken.");
        }

        return leak;
    }

    private static string Describe(int comments, string? assignee, bool resolved)
    {
        var parts = new List<string> { $"{comments} comment(s)" };
        if (assignee is not null)
            parts.Add($"assigned to {assignee}");
        parts.Add(resolved ? "resolved" : "open");
        return string.Join(" · ", parts);
    }

    // The next free issue number: one past the highest ISSUE-<n> already on the board.
    private static int NextIssueNumber(IReadOnlyList<BoardRow> existing)
    {
        var highest = 0;
        foreach (var row in existing)
        {
            var suffix = row.Id.StartsWith("ISSUE-", StringComparison.Ordinal) ? row.Id["ISSUE-".Length..] : row.Id;
            if (int.TryParse(suffix, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) && n > highest)
                highest = n;
        }

        return highest + 1;
    }

    private static bool Crossed(long before, long after) => after / SnapshotInterval > before / SnapshotInterval;

    private string Pick(string[] pool) => pool[_rng.Next(pool.Length)];

    private static string StreamId(string id) => $"{Issue.Category}-{id}";

    private readonly record struct Leak(string Issue, long Version, string Author);
}
