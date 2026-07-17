using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Dbosoft.EventStore.EfCore.Tests;

/// <summary>
/// Builds a provider over a fresh SQLite shared-cache in-memory database, wired exactly as an app
/// would (<c>AddEventStore(es =&gt; es.UseSqlite(...))</c>). Shared cache lets each context open its
/// own connection to the same in-memory database, so concurrent appends run against real relational
/// locking — a genuine test target, not a fake.
/// </summary>
public sealed class SqliteEventStoreFixture : IDisposable
{
    private readonly SqliteConnection _keepAlive;
    private readonly ServiceProvider _services;

    public SqliteEventStoreFixture()
    {
        var connectionString = $"DataSource=file:memdb-{Guid.NewGuid():N}?mode=memory&cache=shared";

        // Keep one connection open for the lifetime of the fixture, so the in-memory database survives.
        _keepAlive = new SqliteConnection(connectionString);
        _keepAlive.Open();

        var services = new ServiceCollection();
        services.AddEventStore(es => es
            .UseSqlite(connectionString)
            .AddDomain("test", _ => { }));
        _services = services.BuildServiceProvider();

        using var ctx = _services.GetRequiredService<IDbContextFactory<EventStoreDbContext>>().CreateDbContext();
        ctx.Database.EnsureCreated();
    }

    public IEventStoreProvider Provider => _services.GetRequiredService<IEventStoreProvider>();

    public void Dispose()
    {
        _services.Dispose();
        _keepAlive.Dispose();
    }
}
