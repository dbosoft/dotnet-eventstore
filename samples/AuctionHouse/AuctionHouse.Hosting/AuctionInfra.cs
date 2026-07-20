using AuctionHouse.Contracts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Rebus.Transport.InMem;

namespace AuctionHouse.Hosting;

/// <summary>The SQLite connection string every module opens the shared event store against.</summary>
public sealed record AuctionConnectionString(string Value);

/// <summary>The event-store domain name (and change-notification key) used across the sample.</summary>
public static class AuctionDomain
{
    public const string Name = "auction";

    /// <summary>Stream id for a lot: <c>lot-{id}</c>.</summary>
    public static string StreamId(string lotId) => $"lot-{lotId}";

    /// <summary>Extracts the lot id back out of a <c>lot-{id}</c> stream id.</summary>
    public static string LotId(string streamId) => streamId.StartsWith("lot-", StringComparison.Ordinal) ? streamId[4..] : streamId;
}

/// <summary>Rebus input-queue names, one per module.</summary>
public static class QueueNames
{
    public const string Writer = "auction-writer";
    public const string Projection = "auction-projection";
}

/// <summary>Registers the cross-cutting resources shared by every module on the host (outer) container.</summary>
public static class AuctionInfra
{
    /// <summary>
    /// A single in-memory Rebus network shared by all modules — the trick that lets the in-memory transport
    /// carry messages across the modules' separate containers (mirrors SAPHub). Plus the shared read store
    /// and the connection string every module's event store opens.
    /// </summary>
    public static IServiceCollection AddAuctionSharedInfra(this IServiceCollection services, string connectionString)
    {
        services.TryAddSingleton(new InMemNetwork());
        services.TryAddSingleton(new AuctionConnectionString(connectionString));
        services.TryAddSingleton<ILotReadStore, InMemoryLotReadStore>();
        return services;
    }
}
