namespace Dbosoft.EventStore.DependencyInjection;

/// <summary>
/// A registered domain, published to the container so a provider can enumerate the domains it must
/// back with storage. One is registered per <c>AddDomain</c> (and for the default domain).
/// </summary>
/// <param name="Name">The domain's name.</param>
/// <param name="Options">The domain's marker and tombstone policy.</param>
[PublicAPI]
public sealed record RegisteredDomain(string Name, DomainOptions Options);
