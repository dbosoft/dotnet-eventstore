using AwesomeAssertions;
using Microsoft.EntityFrameworkCore;
using Xunit;
using static Dbosoft.EventStore.Testing.ConformanceHelpers;

namespace Dbosoft.EventStore.EfCore.Tests;

/// <summary>
/// Provider-specific tests that inject a failure into the EF Core write path. They cover the two ways
/// the naive implementation diverges from the atomic reference: a destructive op that is not all-or-
/// nothing, and a broad catch that disguises an infrastructure error as a version conflict.
/// </summary>
public sealed class EfCoreFaultInjectionTests
{
    [Fact]
    public async Task Truncate_is_atomic_when_the_marker_write_fails()
    {
        using var fixture = new FaultInjectingSqliteFixture();
        var store = fixture.Provider.GetStore("test");
        var maintenance = (IStreamMaintenance)store;
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(5));

        // The physical delete and the head/marker update must commit together. If the marker write
        // fails, the delete must roll back too — otherwise events vanish with no tombstone to record it.
        fixture.Interceptor.Fault = () => new InvalidOperationException("marker write failed");
        var act = () => maintenance.TruncateStreamAsync("s1", 3, force: true);
        await act.Should().ThrowAsync<InvalidOperationException>();

        fixture.Interceptor.Fault = null;
        (await store.ReadAllOf("s1")).Should().HaveCount(5);
    }

    [Fact]
    public async Task Delete_is_atomic_when_the_marker_write_fails()
    {
        using var fixture = new FaultInjectingSqliteFixture();
        var store = fixture.Provider.GetStore("test");
        var maintenance = (IStreamMaintenance)store;
        await store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(5));

        fixture.Interceptor.Fault = () => new InvalidOperationException("marker write failed");
        var act = () => maintenance.DeleteStreamAsync("s1", DeleteMode.Soft);
        await act.Should().ThrowAsync<InvalidOperationException>();

        fixture.Interceptor.Fault = null;
        (await store.ReadAllOf("s1")).Should().HaveCount(5);
    }

    [Fact]
    public async Task Append_surfaces_a_non_concurrency_failure_instead_of_masking_it()
    {
        using var fixture = new FaultInjectingSqliteFixture();
        var store = fixture.Provider.GetStore("test");

        // A genuine infrastructure failure on save must not be reported to the caller as a wrong
        // expected version — that would send them into a pointless reload-and-retry loop.
        fixture.Interceptor.Fault = () => new DbUpdateException("infra failure", new InvalidOperationException("boom"));
        var act = () => store.AppendToStreamAsync("s1", ExpectedVersion.NoStream, Events(1));

        await act.Should().ThrowAsync<DbUpdateException>();
    }
}
