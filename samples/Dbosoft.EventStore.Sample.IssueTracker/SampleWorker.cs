using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Dbosoft.EventStore.Sample.IssueTracker;

/// <summary>
/// Drives the sample as a hosted service against on-disk SQLite. The scenario is written to be run
/// more than once: the first run seeds a fresh database and redacts a leaked email; each later run
/// loads an issue from its persisted snapshot, appends to it, and shows both the board (rebuilt in
/// full, proving the earlier run's data survived) and the durable notifier (resuming from its saved
/// checkpoint, so it only reports the newly added events). Aggregate work runs inside a DI scope, as
/// it would per message in a real app.
/// </summary>
internal sealed class SampleWorker(
    IServiceScopeFactory scopeFactory,
    [FromKeyedServices(SampleWorker.Tracking)] IEventStore store,
    [FromKeyedServices(SampleWorker.Tracking)] IEventMaterializer materializer,
    ICheckpointStore checkpoints,
    ILoggerFactory loggerFactory,
    ILogger<SampleWorker> logger,
    IHostApplicationLifetime lifetime) : BackgroundService
{
    public const string Tracking = "tracking";

    private const string PrimaryIssue = "ISSUE-1";
    private const string ContactIssue = "ISSUE-2";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var board = new IssueBoardProjection(
            (IAllStreamReader)store, materializer, loggerFactory.CreateLogger<IssueBoardProjection>());
        var notifier = new ActivityNotifier(
            (IAllStreamReader)store, materializer, checkpoints, loggerFactory.CreateLogger<ActivityNotifier>());

        var firstRun = await LoadIssueAsync(PrimaryIssue, stoppingToken) is null;
        if (firstRun)
        {
            logger.LogInformation("First run: seeding a fresh SQLite database.");
            await SeedAsync(stoppingToken);
            await board.RebuildAndPrintAsync("after seeding", stoppingToken);

            await RedactLeakedContactAsync(stoppingToken);
            await board.RebuildAndPrintAsync("after redaction", stoppingToken);
        }
        else
        {
            logger.LogInformation("Later run: the database already holds issues from a previous run.");
            await ContinueAsync(PrimaryIssue, stoppingToken);
            await board.RebuildAndPrintAsync("current", stoppingToken);
        }

        await notifier.RunAsync(stoppingToken);
        logger.LogInformation("State is persisted in the SQLite file — run the sample again to watch it resume.");

        lifetime.StopApplication();
    }

    private async Task SeedAsync(CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IAggregateRepository<Issue>>();

        // A chatty issue: opening plus nine comments plus an assignment crosses the ten-event snapshot
        // interval, so the repository writes a snapshot the next load can rehydrate from.
        var main = Issue.Report(PrimaryIssue, "Login page returns 500", "grace");
        for (var i = 1; i <= 9; i++)
            main.Comment(i % 2 == 0 ? "grace" : "ada", $"investigation note #{i}");
        main.AssignTo("ada");
        main.Resolve("deployed a hotfix for the null configuration value");
        await repository.SaveAsync(main, ct);
        logger.LogInformation("{Id} saved at version {Version} — a snapshot was taken.", PrimaryIssue, main.Version);

        var withContact = Issue.Report(ContactIssue, "Cannot reset password", "mallory");
        withContact.Comment("mallory", "reach me at mallory@example.com if you need more detail");
        await repository.SaveAsync(withContact, ct);
        logger.LogInformation("{Id} saved with a comment that contains a personal email address.", ContactIssue);
    }

    private async Task RedactLeakedContactAsync(CancellationToken ct)
    {
        // The comment at ContactIssue v2 leaked an email. Overwrite the payload in place — same shape,
        // same slot, same id — so a fresh read never sees the address; the $event-redacted marker tells
        // already-caught-up consumers to forget the value they cached.
        var scrubbed = materializer.Dematerialize(new IssueCommented("mallory", "[redacted]")).Data;
        await ((IStreamMaintenance)store).RedactEventAsync(StreamId(ContactIssue), version: 2, scrubbed, ct: ct);
        logger.LogInformation("Redacted the leaked email in {Id} (comment v2).", ContactIssue);
    }

    private async Task ContinueAsync(string id, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IAggregateRepository<Issue>>();

        var issue = await repository.LoadAsync(id, ct)
                    ?? throw new InvalidOperationException($"Issue '{id}' was expected but not found.");
        logger.LogInformation(
            "Loaded {Id} \"{Title}\" at version {Version} [{Status}, assignee {Assignee}, {Comments} comments], "
            + "rehydrated from the persisted snapshot plus its tail events.",
            id, issue.Title, issue.Version, issue.Status, issue.Assignee ?? "unassigned", issue.CommentCount);

        // A resolved issue is reopened before the follow-up; once open, later runs just add a comment.
        if (issue.Status == Issue.Resolved)
        {
            issue.Reopen("the problem resurfaced after the latest deploy");
            logger.LogInformation("{Id} was resolved; reopened it.", id);
        }

        issue.Comment("ada", "follow-up after restart");
        await repository.SaveAsync(issue, ct);
        logger.LogInformation("{Id} is now at version {Version}.", id, issue.Version);
    }

    private async Task<Issue?> LoadIssueAsync(string id, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IAggregateRepository<Issue>>();
        return await repository.LoadAsync(id, ct);
    }

    private static string StreamId(string id) => $"{Issue.Category}-{id}";
}
