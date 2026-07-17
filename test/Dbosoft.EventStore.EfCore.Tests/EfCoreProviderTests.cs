using AwesomeAssertions;
using Xunit;

namespace Dbosoft.EventStore.EfCore.Tests;

public sealed class EfCoreProviderTests
{
    [Fact]
    public void Resolving_a_store_for_an_unregistered_domain_throws()
    {
        using var fixture = new SqliteEventStoreFixture();
        var provider = fixture.Provider;

        // "test" is the only registered domain; a typo must fail loudly, not materialize an empty store.
        var act = () => provider.GetStore("typo");

        act.Should().Throw<ArgumentException>();
    }
}
