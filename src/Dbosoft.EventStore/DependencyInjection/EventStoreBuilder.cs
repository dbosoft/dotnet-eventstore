using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using Dbosoft.EventStore.Aggregates;
using Dbosoft.EventStore.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace Dbosoft.EventStore.DependencyInjection;

/// <summary>
/// Fluent configuration for the event store, returned by <c>AddEventStore</c>. Providers extend this
/// with <c>UseXxx</c> methods that register an <see cref="IEventStoreProvider"/>. Events are grouped
/// into domains via <see cref="AddDomain"/>; the convenience methods here target a single default
/// domain for simple, single-context applications.
/// </summary>
[PublicAPI]
public sealed class EventStoreBuilder
{
    /// <summary>The name of the implicit domain used by the top-level convenience methods.</summary>
    public const string DefaultDomainName = "default";

    private readonly Dictionary<string, DomainBuilder> _builders = new(StringComparer.Ordinal);

    internal EventStoreBuilder(IServiceCollection services) => Services = services;

    /// <summary>The underlying service collection, for providers and advanced wiring.</summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// The configured domain builders. Validation and domain publication happen once, at build time,
    /// so the default domain is a single builder even when <see cref="AddDomain"/> and the convenience
    /// methods both target it.
    /// </summary>
    internal IReadOnlyCollection<DomainBuilder> DomainBuilders => _builders.Values;

    /// <summary>
    /// Configures a domain — a group of related events mapped to its own storage unit, with its own
    /// ordering and global read.
    /// </summary>
    public EventStoreBuilder AddDomain(string name, Action<DomainBuilder> configure)
    {
        if (_builders.ContainsKey(name))
            throw new InvalidOperationException($"Domain '{name}' is already registered.");

        var domain = new DomainBuilder(name, Services);
        _builders[name] = domain;
        configure(domain);
        return this;
    }

    /// <summary>
    /// Replaces the serializer used for events and snapshots with a custom instance. The default is
    /// <see cref="SystemTextJsonEventSerializer"/>; plug in your own to change the wire format or
    /// mapping rules.
    /// </summary>
    public EventStoreBuilder UseSerializer(IEventSerializer serializer)
    {
        Services.AddSingleton(serializer);
        return this;
    }

    /// <summary>Replaces the serializer with one resolved from the container.</summary>
    public EventStoreBuilder UseSerializer<TSerializer>() where TSerializer : class, IEventSerializer
    {
        Services.AddSingleton<IEventSerializer, TSerializer>();
        return this;
    }

    /// <summary>
    /// Keeps the System.Text.Json serializer but adjusts its options — naming policy, converters,
    /// unmapped-member handling — so fields can be mapped differently without writing a serializer.
    /// </summary>
    public EventStoreBuilder UseSystemTextJson(Action<JsonSerializerOptions> configure)
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        configure(options);
        Services.AddSingleton<IEventSerializer>(new SystemTextJsonEventSerializer(options));
        return this;
    }

    /// <summary>
    /// Uses the System.Text.Json source generator via a <see cref="JsonSerializerContext"/>: keeps the
    /// readable, portable JSON format but serializes reflection-free — faster, and AOT/trim-safe.
    /// </summary>
    public EventStoreBuilder UseSystemTextJson(
        JsonSerializerContext context,
        Action<JsonSerializerOptions>? configure = null)
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web) { TypeInfoResolver = context };
        configure?.Invoke(options);
        Services.AddSingleton<IEventSerializer>(new SystemTextJsonEventSerializer(options));
        return this;
    }

    /// <summary>Registers an event type in the default domain.</summary>
    public EventStoreBuilder AddEventType<TEvent>(string? eventType = null)
    {
        Default.AddEventType<TEvent>(eventType);
        return this;
    }

    /// <summary>Registers every concrete implementor of a marker type in the default domain.</summary>
    public EventStoreBuilder AddEventTypesImplementing<TMarker>(Assembly? assembly = null)
    {
        Default.AddEventTypesImplementing<TMarker>(assembly);
        return this;
    }

    /// <summary>Registers an upcaster in the default domain.</summary>
    public EventStoreBuilder AddUpcaster<TUpcaster>() where TUpcaster : class, IEventUpcaster
    {
        Default.AddUpcaster<TUpcaster>();
        return this;
    }

    /// <summary>Registers an aggregate repository in the default domain.</summary>
    public EventStoreBuilder AddAggregateRepository<T>(
        string category,
        Func<string, T> factory,
        ISnapshotPolicy? snapshotPolicy = null)
        where T : AggregateBase
    {
        Default.AddAggregateRepository(category, factory, snapshotPolicy);
        return this;
    }

    // The default domain reuses its one builder, whether it was first touched by AddDomain("default")
    // or by a convenience method — so its event types and options never split across two registrations.
    private DomainBuilder Default
    {
        get
        {
            if (_builders.TryGetValue(DefaultDomainName, out var existing))
                return existing;

            var builder = new DomainBuilder(DefaultDomainName, Services);
            _builders[DefaultDomainName] = builder;
            return builder;
        }
    }
}
