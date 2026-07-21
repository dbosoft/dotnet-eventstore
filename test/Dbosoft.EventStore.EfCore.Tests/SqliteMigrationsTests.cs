using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.EfCore.Tests;

/// <summary>
/// Exercises the shipped SQLite migrations as a consumer would apply them: <c>MigrateAsync</c> builds a usable
/// schema, records itself in the configured (isolated) history table, and stays in sync with the model.
/// </summary>
public sealed class SqliteMigrationsTests
{
    [Fact]
    public async Task Migrate_builds_a_usable_schema_recorded_in_the_default_history_table()
    {
        await using var harness = Harness.Create();

        await using (var ctx = await harness.CreateContextAsync())
        {
            await ctx.Database.MigrateAsync();

            // The migrations are in sync with the model — the drift guard.
            ctx.Database.HasPendingModelChanges().Should().BeFalse();
            // EF recorded the applied migration in (and read it back from) the configured history table.
            (await ctx.Database.GetAppliedMigrationsAsync()).Should().ContainSingle().Which.Should().EndWith("InitialCreate");
        }

        // The history table carries the store's isolated default name, not EF's __EFMigrationsHistory.
        (await harness.TableExistsAsync("es_migrations_history")).Should().BeTrue();
        (await harness.TableExistsAsync("__EFMigrationsHistory")).Should().BeFalse();

        // The migrated schema actually works.
        var store = harness.Store;
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(2));
        (await store.GetStreamInfoAsync("s1"))!.Version.Should().Be(2);
    }

    [Fact]
    public async Task The_history_table_name_is_configurable()
    {
        await using var harness = Harness.Create(migrationsHistoryTable: "es_own_history");

        await using (var ctx = await harness.CreateContextAsync())
            await ctx.Database.MigrateAsync();

        (await harness.TableExistsAsync("es_own_history")).Should().BeTrue();
        (await harness.TableExistsAsync("es_migrations_history")).Should().BeFalse();
    }

    private sealed class Harness : IAsyncDisposable
    {
        private readonly ServiceProvider _services;
        private readonly SqliteConnection _keepAlive;

        private Harness(ServiceProvider services, SqliteConnection keepAlive)
        {
            _services = services;
            _keepAlive = keepAlive;
        }

        public static Harness Create(string? migrationsHistoryTable = null)
        {
            var connectionString = $"DataSource=file:memdb-{Guid.NewGuid():N}?mode=memory&cache=shared";
            var keepAlive = new SqliteConnection(connectionString);
            keepAlive.Open();

            var services = new ServiceCollection();
            services.AddEventStore(es => es
                .UseSqlite(connectionString,
                    migrationsHistoryTable: migrationsHistoryTable ?? EventStoreDbContext.DefaultMigrationsHistoryTable)
                .AddDomain("test", _ => { }));
            // Note: no EnsureCreated here — the test applies the migrations itself.
            return new Harness(services.BuildServiceProvider(), keepAlive);
        }

        public IEventStore Store => _services.GetRequiredKeyedService<IEventStore>("test");

        public Task<EventStoreDbContext> CreateContextAsync() =>
            _services.GetRequiredService<IDbContextFactory<EventStoreDbContext>>().CreateDbContextAsync();

        public async Task<bool> TableExistsAsync(string name)
        {
            await using var cmd = _keepAlive.CreateCommand();
            cmd.CommandText = "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = $name";
            var parameter = cmd.CreateParameter();
            parameter.ParameterName = "$name";
            parameter.Value = name;
            cmd.Parameters.Add(parameter);
            return Convert.ToInt64(await cmd.ExecuteScalarAsync()) > 0;
        }

        public async ValueTask DisposeAsync()
        {
            await _services.DisposeAsync();
            await _keepAlive.DisposeAsync();
        }
    }
}
