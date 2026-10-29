using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Dbosoft.EventStore.EfCore.Tests;

/// <summary>
/// Builds a provider over a fresh SQLite database file in write-ahead-log mode, wired exactly as an app
/// would (<c>AddEventStore(es =&gt; es.UseSqlite(...))</c>). Each context opens its own connection to it, so
/// concurrent appends run against SQLite's real locking as a deployment's file does: a writer that finds the
/// database locked waits for it. A shared-cache in-memory database locks per table and fails at once instead;
/// on it the concurrent conformance tests failed about one run in ten with raw SQLite errors ("cannot start a
/// transaction within a transaction", "not an error"), on a file not in thirty.
/// </summary>
public sealed class SqliteEventStoreFixture : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"es-conformance-{Guid.NewGuid():N}.db");
    private readonly ServiceProvider _services;

    public SqliteEventStoreFixture()
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = _path, Pooling = false }.ToString();

        var services = new ServiceCollection();
        services.AddEventStore(es => es
            .UseSqlite(connectionString)
            .AddDomain("test", _ => { }));
        _services = services.BuildServiceProvider();

        using var ctx = _services.GetRequiredService<IDbContextFactory<EventStoreDbContext>>().CreateDbContext();
        ctx.Database.EnsureCreated();
        ctx.Database.ExecuteSqlRaw("PRAGMA journal_mode=WAL;");
    }

    public IEventStoreProvider Provider => _services.GetRequiredService<IEventStoreProvider>();

    public void Dispose()
    {
        _services.Dispose();
        foreach (var file in new[] { _path, _path + "-wal", _path + "-shm" })
        {
            try
            {
                File.Delete(file);
            }
            catch (IOException)
            {
                // A file still held by a closing connection is left to the temp folder's cleanup.
            }
        }
    }
}
