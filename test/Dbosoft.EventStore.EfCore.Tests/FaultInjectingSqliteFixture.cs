using Dbosoft.EventStore.DependencyInjection;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Dbosoft.EventStore.EfCore.Tests;

/// <summary>
/// Like <see cref="SqliteEventStoreFixture"/>, but wires a <see cref="FaultInjectingInterceptor"/> into
/// the context so a test can make a specific <c>SaveChanges</c> fail. Used to exercise partial-failure
/// paths (transaction rollback, error masking) that are otherwise unreachable against a healthy store.
/// </summary>
public sealed class FaultInjectingSqliteFixture : IDisposable
{
    private readonly SqliteConnection _keepAlive;
    private readonly ServiceProvider _services;

    public FaultInjectingSqliteFixture()
    {
        var connectionString = $"DataSource=file:memdb-{Guid.NewGuid():N}?mode=memory&cache=shared";
        _keepAlive = new SqliteConnection(connectionString);
        _keepAlive.Open();

        var services = new ServiceCollection();
        services.AddEventStore(es => es
            .UseEfCore(
                options => options.UseSqlite(connectionString).AddInterceptors(Interceptor),
                model => model.StoreTimestampsAsTicks().UseVersionConcurrencyToken())
            .AddDomain("test", _ => { }));
        _services = services.BuildServiceProvider();

        using var ctx = _services.GetRequiredService<IDbContextFactory<EventStoreDbContext>>().CreateDbContext();
        ctx.Database.EnsureCreated();
    }

    public FaultInjectingInterceptor Interceptor { get; } = new();

    public IEventStoreProvider Provider => _services.GetRequiredService<IEventStoreProvider>();

    public void Dispose()
    {
        _services.Dispose();
        _keepAlive.Dispose();
    }
}

/// <summary>A save interceptor that throws whatever <see cref="Fault"/> returns, when armed.</summary>
public sealed class FaultInjectingInterceptor : SaveChangesInterceptor
{
    /// <summary>When set, its return value (if non-null) is thrown from the next <c>SaveChanges</c>.</summary>
    public Func<Exception?>? Fault { get; set; }

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        if (Fault?.Invoke() is { } ex)
            throw ex;
        return base.SavingChangesAsync(eventData, result, cancellationToken);
    }
}
