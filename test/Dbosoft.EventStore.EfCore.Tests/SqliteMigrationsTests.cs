using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
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
            (await ctx.Database.GetAppliedMigrationsAsync()).Select(m => m[(m.IndexOf('_') + 1)..])
                .Should().Equal("InitialCreate", "CommitSequence");
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
    public async Task Events_stored_before_the_commit_sequence_keep_their_order_and_new_ones_follow()
    {
        await using var harness = Harness.Create();

        await using (var ctx = await harness.CreateContextAsync())
        {
            var migrator = ctx.GetService<IMigrator>();
            var initial = ctx.Database.GetMigrations().First(m => m.EndsWith("_InitialCreate", StringComparison.Ordinal));
            await migrator.MigrateAsync(initial);

            // Two events as the store wrote them before commit sequences, ordered by row id only.
            await ctx.Database.ExecuteSqlRawAsync(
                "INSERT INTO es_streams (Domain, StreamId, Version, TruncatedBefore, IsDeleted, HardDeleted) VALUES ('test', 'old', 2, 1, 0, 0)");
            for (var version = 1; version <= 2; version++)
            {
                await ctx.Database.ExecuteSqlRawAsync(
                    "INSERT INTO es_events (Domain, StreamId, StreamVersion, EventId, EventType, Data, ContentType, CreatedAt) "
                    + "VALUES ('test', 'old', {0}, {1}, 'Old', X'7B7D', 'application/json', 0)",
                    version, Guid.NewGuid().ToString().ToUpperInvariant());
            }

            await migrator.MigrateAsync();
        }

        var store = harness.Store;
        await store.AppendToStreamAsync("new", ExpectedVersion.NoStream, Events(1));

        var all = await ((IAllStreamReader)store).ReadAllAsync(GlobalPosition.Start).ToListAsync();
        all.Select(e => e.StreamId).Should().Equal("old", "old", "new");
        all.Select(e => e.Position).Should().Equal(
            GlobalPosition.FromSequence(1), GlobalPosition.FromSequence(2), GlobalPosition.FromSequence(3));
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
