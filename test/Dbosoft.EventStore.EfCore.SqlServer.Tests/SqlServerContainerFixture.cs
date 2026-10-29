using Testcontainers.MsSql;
using Xunit;

namespace Dbosoft.EventStore.EfCore.SqlServer.Tests;

/// <summary>
/// Starts a single SQL Server container for the whole collection. Each test gets a fresh database
/// inside it (see <see cref="SqlServerTestStore"/>), so tests are isolated without a container each.
/// </summary>
[UsedImplicitly]
public sealed class SqlServerContainerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04").Build();

    public string ConnectionString => _container.GetConnectionString();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

/// <summary>Binds the SQL Server test classes to one shared container.</summary>
[CollectionDefinition(Name)]
public sealed class SqlServerCollection : ICollectionFixture<SqlServerContainerFixture>
{
    public const string Name = "SqlServer";
}
