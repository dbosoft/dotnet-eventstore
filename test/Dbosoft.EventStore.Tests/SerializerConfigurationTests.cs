using System.Text;
using System.Text.Json;
using AwesomeAssertions;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.InMemory;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Xunit;

namespace Dbosoft.EventStore.Tests;

public sealed class SerializerConfigurationTests
{
    [Fact]
    public void Use_serializer_replaces_the_default()
    {
        var custom = Substitute.For<IEventSerializer>();
        var services = new ServiceCollection();
        services.AddEventStore(b => b.UseInMemory().UseSerializer(custom));

        using var sp = services.BuildServiceProvider();

        sp.GetRequiredService<IEventSerializer>().Should().BeSameAs(custom);
    }

    [Fact]
    public void Use_system_text_json_applies_custom_mapping_rules()
    {
        var services = new ServiceCollection();
        services.AddEventStore(b => b
            .UseInMemory()
            .UseSystemTextJson(options => options.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower));

        using var sp = services.BuildServiceProvider();
        var serializer = sp.GetRequiredService<IEventSerializer>();

        var json = Encoding.UTF8.GetString(serializer.Serialize(new { SomeField = 1 }).Span);

        json.Should().Contain("some_field");
    }

    [Fact]
    public void Default_serializer_is_used_when_none_is_configured()
    {
        var services = new ServiceCollection();
        services.AddEventStore(b => b.UseInMemory());

        using var sp = services.BuildServiceProvider();

        sp.GetRequiredService<IEventSerializer>().Should().BeOfType<SystemTextJsonEventSerializer>();
    }
}
