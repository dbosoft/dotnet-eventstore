using Testcontainers.PostgreSql;
using Xunit;

namespace Dbosoft.EventStore.EfCore.PostgreSql.Tests;

/// <summary>
/// Starts a single PostgreSQL container for the whole collection. Each test gets a fresh database
/// inside it (see <see cref="PostgreSqlTestStore"/>), so tests are isolated without a container each.
/// </summary>
[UsedImplicitly]
public sealed class PostgreSqlContainerFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:15.1").Build();

    public string ConnectionString => _container.GetConnectionString();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

/// <summary>Binds the PostgreSQL test classes to one shared container.</summary>
[CollectionDefinition(Name)]
public sealed class PostgreSqlCollection : ICollectionFixture<PostgreSqlContainerFixture>
{
    public const string Name = "PostgreSql";
}
