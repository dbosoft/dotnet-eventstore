namespace Dbosoft.EventStore.Sample.IssueTracker;

/// <summary>
/// A tiny console presenter so the sample reads as a narrative — a banner, headed sections, bullets
/// and a boxed table — rather than a wall of log lines. Uses ANSI colour and box-drawing when the
/// output is a real terminal (and <c>NO_COLOR</c> is unset); degrades to plain text when redirected.
/// </summary>
internal static class Report
{
    private static readonly bool UseColor =
        !Console.IsOutputRedirected && Environment.GetEnvironmentVariable("NO_COLOR") is null;

    private const string Reset = "[0m";
    private const string Bold = "[1m";
    private const string Dim = "[90m";
    private const string Cyan = "[96m";
    private const string Green = "[92m";
    private const string Yellow = "[93m";
    private const string Blue = "[94m";
    private const string Red = "[91m";

    public static void Banner(string title, string subtitle)
    {
        var inner = Math.Max(title.Length, subtitle.Length) + 2;
        Console.WriteLine();
        Console.WriteLine(Paint("╔" + new string('═', inner) + "╗", Cyan));
        Console.WriteLine(Paint("║", Cyan) + Paint(Fit(" " + title, inner), Cyan + Bold) + Paint("║", Cyan));
        Console.WriteLine(Paint("║", Cyan) + Paint(Fit(" " + subtitle, inner), Dim) + Paint("║", Cyan));
        Console.WriteLine(Paint("╚" + new string('═', inner) + "╝", Cyan));
    }

    public static void KeyValue(string label, string value)
    {
        Console.WriteLine();
        Console.WriteLine(Paint(label + ": ", Dim) + value);
    }

    public static void Section(string text)
    {
        Console.WriteLine();
        Console.WriteLine(Paint("▶ " + text, Cyan + Bold));
    }

    public static void Bullet(string text) => Console.WriteLine("  • " + text);

    public static void Note(string text) => Console.WriteLine(Paint("    " + text, Dim));

    public static void Warn(string text) => Console.WriteLine(Paint("  ⚠ " + text, Yellow));

    public static void Outro(string text)
    {
        Console.WriteLine();
        Console.WriteLine(Paint("  " + text, Dim));
        Console.WriteLine();
    }

    public static void Board(string caption, IReadOnlyList<BoardRow> rows)
    {
        Console.WriteLine();
        Console.WriteLine(Paint("  " + caption, Dim));

        var headers = new[] { "Issue", "Title", "Status", "Reporter", "#", "Last activity" };
        var caps = new[] { 9, 22, 16, 9, 3, 30 };
        var data = rows.Select(r => new[]
        {
            r.Id, r.Title, StatusText(r), r.Reporter, r.Comments.ToString(), r.LastActivity
        }).ToList();
        Func<string, string?>?[] paint =
            [null, null, StatusColor, null, null, value => value == "[redacted]" ? Red : null];

        Table(headers, data, caps, paint);
    }

    public static void Notifications(bool resumed, int count, IReadOnlyList<string> lines)
    {
        Section("Notifications · durable checkpoint");
        Bullet(resumed
            ? $"Resumed from the saved checkpoint — delivered {Emphasize(count.ToString())} new event(s)."
            : $"First delivery — sent {Emphasize(count.ToString())} event(s) and saved a checkpoint.");
        foreach (var line in lines)
            Note(line);
    }

    private static string StatusText(BoardRow row) =>
        row.Assignee is null ? row.Status : $"{row.Status} ({row.Assignee})";

    private static string StatusColor(string status) =>
        status.StartsWith(Issue.Resolved, StringComparison.Ordinal) ? Green
        : status.StartsWith(Issue.Assigned, StringComparison.Ordinal) ? Blue
        : Yellow;

    private static string Emphasize(string text) => Paint(text, Green + Bold);

    private static void Table(
        string[] headers,
        IReadOnlyList<string[]> rows,
        int[] caps,
        IReadOnlyList<Func<string, string?>?> paint)
    {
        var columns = headers.Length;
        var width = new int[columns];
        for (var c = 0; c < columns; c++)
        {
            var max = headers[c].Length;
            foreach (var row in rows)
                max = Math.Max(max, row[c].Length);
            width[c] = Math.Min(caps[c], max);
        }

        string Rule(char left, char mid, char right) =>
            left + string.Join(mid, width.Select(w => new string('─', w + 2))) + right;

        Console.WriteLine(Paint("  " + Rule('┌', '┬', '┐'), Dim));
        Console.Write("  " + Paint("│", Dim));
        for (var c = 0; c < columns; c++)
            Console.Write(" " + Paint(Fit(headers[c], width[c]), Bold) + " " + Paint("│", Dim));
        Console.WriteLine();
        Console.WriteLine(Paint("  " + Rule('├', '┼', '┤'), Dim));

        foreach (var row in rows)
        {
            Console.Write("  " + Paint("│", Dim));
            for (var c = 0; c < columns; c++)
            {
                var cell = Fit(row[c], width[c]);
                var color = paint[c]?.Invoke(row[c].Trim());
                var text = color is not null && UseColor ? color + cell + Reset : cell;
                Console.Write(" " + text + " " + Paint("│", Dim));
            }

            Console.WriteLine();
        }

        Console.WriteLine(Paint("  " + Rule('└', '┴', '┘'), Dim));
    }

    private static string Paint(string text, string color) => UseColor ? color + text + Reset : text;

    private static string Fit(string text, int width) =>
        text.Length <= width
            ? text + new string(' ', width - text.Length)
            : width <= 1
                ? text[..width]
                : text[..(width - 1)] + "…";
}
