using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.EfCore.Tests;

public sealed class EfCoreChangeNotificationTests
{
    // Proves the change signal is wired through the core (EventStoreBase) for a persistent provider, not
    // just in-memory: registering UseInProcessNotifications flows an IChangeNotifier into the EF Core store,
    // whose append fires it — the whole point of centralising the signal instead of per-provider wiring.
    [Fact]
    public async Task The_Ef_Core_store_signals_a_change_on_append()
    {
        var connectionString = $"DataSource=file:memdb-{Guid.NewGuid():N}?mode=memory&cache=shared";
        await using var keepAlive = new SqliteConnection(connectionString);
        keepAlive.Open();

        var services = new ServiceCollection();
        services.AddEventStore(es => es
            .UseInProcessNotifications()
            .UseSqlite(connectionString)
            .AddDomain("banking", _ => { }));
        await using var sp = services.BuildServiceProvider();

        await using (var ctx = await sp.GetRequiredService<IDbContextFactory<EventStoreDbContext>>().CreateDbContextAsync())
            await ctx.Database.EnsureCreatedAsync();

        var notifier = sp.GetRequiredService<IChangeNotifier>();
        var store = sp.GetRequiredKeyedService<IEventStore>("banking");
        using var watch = notifier.Watch("banking");

        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));

        // the append signalled the changed stream, carrying its id across the notifier
        Assert.Equal("s1", await watch.WaitAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2)));
    }
}
