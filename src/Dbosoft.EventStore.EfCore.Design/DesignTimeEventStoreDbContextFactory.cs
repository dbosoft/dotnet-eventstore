using Dbosoft.EventStore.EfCore.PostgreSql;
using Dbosoft.EventStore.EfCore.Sqlite;
using Dbosoft.EventStore.EfCore.SqlServer;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Dbosoft.EventStore.EfCore.Design;

/// <summary>
/// The one design-time factory <c>dotnet ef</c> uses to (re)generate the providers' migrations. It lives in a
/// non-shipped project so no published package carries a <c>Microsoft.EntityFrameworkCore.Design</c> reference.
/// The provider is chosen by the first argument, and migrations are written to that provider's assembly:
/// <code>dotnet ef migrations add &lt;Name&gt; -p src/Dbosoft.EventStore.EfCore.Sqlite -s src/Dbosoft.EventStore.EfCore.Design -- sqlite</code>
/// Each branch reuses the provider's own <c>ConfigureModel</c> (via InternalsVisibleTo) so the snapshot matches
/// the runtime model exactly. Connection strings are placeholders — <c>migrations add</c> works off the model.
/// </summary>
[UsedImplicitly]
internal sealed class DesignTimeEventStoreDbContextFactory : IDesignTimeDbContextFactory<EventStoreDbContext>
{
    public EventStoreDbContext CreateDbContext(string[] args)
    {
        var provider = args.Length > 0
            ? args[0]
            : throw new ArgumentException("Specify the provider as the first argument: sqlite | sqlserver | npgsql.");

        var options = new DbContextOptionsBuilder<EventStoreDbContext>();
        Action<ModelBuilder> configureModel;
        switch (provider)
        {
            case "sqlite":
                options.UseSqlite("Data Source=eventstore.design.db",
                    o => o.MigrationsAssembly("Dbosoft.EventStore.EfCore.Sqlite"));
                configureModel = SqliteEventStoreBuilderExtensions.ConfigureModel;
                break;
            case "sqlserver":
                options.UseSqlServer("Server=.;Database=eventstore-design;Trusted_Connection=True;TrustServerCertificate=True",
                    o => o.MigrationsAssembly("Dbosoft.EventStore.EfCore.SqlServer"));
                configureModel = SqlServerEventStoreBuilderExtensions.ConfigureModel;
                break;
            case "npgsql":
                options.UseNpgsql("Host=localhost;Database=eventstore-design;Username=postgres",
                    o => o.MigrationsAssembly("Dbosoft.EventStore.EfCore.PostgreSql"));
                configureModel = PostgreSqlEventStoreBuilderExtensions.ConfigureModel;
                break;
            default:
                throw new ArgumentException($"Unknown provider '{provider}'. Use sqlite | sqlserver | npgsql.");
        }

        return new EventStoreDbContext(options.Options, configureModel);
    }
}
