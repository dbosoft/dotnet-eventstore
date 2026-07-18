using System.Security.Cryptography;
using System.Text;

namespace Dbosoft.EventStore.AzureTables;

/// <summary>
/// Maps a domain to its three Azure table names. Each domain is its own set of tables (cheap to create
/// and to drop, unlike a relational schema), so there is no cross-domain discriminator. Table names
/// must match <c>^[A-Za-z][A-Za-z0-9]{2,62}$</c>; a domain that is already alphanumeric is used
/// verbatim for readability, otherwise it is mapped to a stable hash so any domain name is accepted.
/// The checkpoint table is shared: the checkpoint store is one domain-agnostic service.
/// </summary>
internal static class AzureTableNames
{
    public static string Events(string prefix, string domain) => Compose(prefix, domain, "Events");

    public static string Snapshots(string prefix, string domain) => Compose(prefix, domain, "Snapshots");

    public static string Index(string prefix, string domain) => Compose(prefix, domain, "Index");

    public static string Checkpoints(string prefix) => (prefix.Length == 0 ? "Es" : prefix) + "Checkpoints";

    // The blob container holding large payloads for a domain. Container names are lowercase; the token
    // is already alphanumeric, so lower-casing keeps it valid.
    public static string Container(string prefix, string domain) =>
        Compose(prefix, domain, "payloads").ToLowerInvariant();

    // Uses the domain verbatim when it is alphanumeric and the whole name fits Azure's 63-char limit,
    // otherwise a stable short hash — so any domain, of any length, yields a valid name.
    private static string Compose(string prefix, string domain, string suffix)
    {
        var readable = prefix + domain + suffix;
        return IsTableSafe(domain) && readable.Length <= 63 ? readable : prefix + "d" + Hash(domain) + suffix;
    }

    private static bool IsTableSafe(string domain) =>
        domain.Length >= 1 && char.IsAsciiLetter(domain[0]) && domain.All(char.IsAsciiLetterOrDigit);

    private static string Hash(string domain) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(domain)))[..16];
}
