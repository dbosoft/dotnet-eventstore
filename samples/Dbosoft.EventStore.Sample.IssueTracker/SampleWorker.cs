using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Dbosoft.EventStore.Sample.IssueTracker;

/// <summary>
/// Drives the sample as a hosted service against on-disk SQLite, presenting the run as a readable
/// narrative (see <see cref="Report"/>). It is written to be run more than once: the first run seeds a
/// fresh database and redacts a leaked email; each later run loads an issue from its persisted
/// snapshot, appends to it, and shows both the board (rebuilt in full, proving the earlier run's data
/// survived) and the durable notifier (resuming from its saved checkpoint). Aggregate work runs inside
/// a DI scope, as it would per message in a real app.
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

    private const string PrimaryIssue = "ISSUE-1";
    private const string ContactIssue = "ISSUE-2";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var board = new IssueBoardProjection((IAllStreamReader)store, materializer);
        var notifier = new ActivityNotifier((IAllStreamReader)store, materializer, checkpoints);

        var firstRun = await LoadIssueAsync(PrimaryIssue, stoppingToken) is null;

        Report.Banner("Issue Tracker", "Event sourcing on the EF Core / SQLite provider");
        Report.KeyValue("Database", $"{DatabaseFile}  ({(firstRun ? "fresh" : "existing — written by a previous run")})");

        if (firstRun)
        {
            Report.Section("Seeding a fresh database");
            await SeedAsync(stoppingToken);
            Report.Board("Board after seeding", await board.RebuildAsync(stoppingToken));

            Report.Warn("Comment ISSUE-2 / v2 leaked an email address — redacting it in place …");
            await RedactLeakedContactAsync(stoppingToken);
            Report.Board("Board after redaction — the payload was scrubbed on disk", await board.RebuildAsync(stoppingToken));
        }
        else
        {
            Report.Section("Continuing from persisted state");
            await ContinueAsync(PrimaryIssue, stoppingToken);
            Report.Board("Board rebuilt from $all — the earlier run's issues survived the restart",
                await board.RebuildAsync(stoppingToken));
        }

        var delivered = await notifier.RunAsync(stoppingToken);
        Report.Notifications(delivered.Resumed, delivered.Count, delivered.Lines);

        Report.Outro($"State is persisted in {DatabaseFile} — run the sample again to watch the board and notifier resume.");

        lifetime.StopApplication();
    }

    private async Task SeedAsync(CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IAggregateRepository<Issue>>();

        // A chatty issue: opening plus nine comments plus an assignment and a resolution cross the
        // ten-event snapshot interval, so the repository writes a snapshot the next load rehydrates from.
        var main = Issue.Report(PrimaryIssue, "Login page returns 500", "grace");
        for (var i = 1; i <= 9; i++)
            main.Comment(i % 2 == 0 ? "grace" : "ada", $"investigation note #{i}");
        main.AssignTo("ada");
        main.Resolve("deployed a hotfix for the null configuration value");
        await repository.SaveAsync(main, ct);
        Report.Bullet($"Opened {PrimaryIssue}, discussed it (9 comments), assigned to ada, and resolved it.");
        Report.Note($"→ {main.Version} events written; a snapshot was taken at v{main.Version}.");

        var withContact = Issue.Report(ContactIssue, "Cannot reset password", "mallory");
        withContact.Comment("mallory", "reach me at mallory@example.com if you need more detail");
        await repository.SaveAsync(withContact, ct);
        Report.Bullet($"Opened {ContactIssue} with a comment that contains a personal email address.");
    }

    private async Task RedactLeakedContactAsync(CancellationToken ct)
    {
        // Overwrite the payload in place — same shape, same slot, same id — so a fresh read never sees
        // the address; the $event-redacted marker tells already-caught-up consumers to forget it.
        var scrubbed = materializer.Dematerialize(new IssueCommented("mallory", "[redacted]")).Data;
        await ((IStreamMaintenance)store).RedactEventAsync(StreamId(ContactIssue), version: 2, scrubbed, ct: ct);
    }

    private async Task ContinueAsync(string id, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IAggregateRepository<Issue>>();

        var issue = await repository.LoadAsync(id, ct)
                    ?? throw new InvalidOperationException($"Issue '{id}' was expected but not found.");
        Report.Bullet(
            $"Loaded {id} \"{issue.Title}\" at v{issue.Version} [{issue.Status}, assignee {issue.Assignee ?? "none"}, "
            + $"{issue.CommentCount} comments] — rehydrated from the snapshot, not replayed from event 1.");

        // A resolved issue is reopened before the follow-up; once open, later runs just add a comment.
        if (issue.Status == Issue.Resolved)
        {
            issue.Reopen("the problem resurfaced after the latest deploy");
            Report.Bullet($"{id} was resolved → reopened it, then added a follow-up comment.");
        }
        else
        {
            Report.Bullet($"Added a follow-up comment to {id}.");
        }

        issue.Comment("ada", "follow-up after restart");
        await repository.SaveAsync(issue, ct);
        Report.Note($"→ {id} is now at v{issue.Version}.");
    }

    private async Task<Issue?> LoadIssueAsync(string id, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IAggregateRepository<Issue>>();
        return await repository.LoadAsync(id, ct);
    }

    private static string StreamId(string id) => $"{Issue.Category}-{id}";
}
