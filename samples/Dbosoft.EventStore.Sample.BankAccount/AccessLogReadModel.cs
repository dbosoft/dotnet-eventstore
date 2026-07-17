using Dbosoft.EventStore.InMemory;
using Microsoft.Extensions.Logging;

namespace Dbosoft.EventStore.Sample.BankAccount;

/// <summary>
/// A read model of the access log, fed from the bus. It keeps one entry per (stream, version) and,
/// when a <see cref="StreamTruncatedMarker"/> arrives — the store propagating a retention delete — it
/// prunes the entries the store dropped. This is the deletion-propagation loop end to end.
/// </summary>
internal sealed class AccessLogReadModel(ILogger<AccessLogReadModel> logger)
{
    private readonly Dictionary<(string Stream, long Version), string> _entries = [];

    public int LiveEntries => _entries.Count;

    public Task Handle(BusMessage message, CancellationToken ct = default)
    {
        switch (message.Payload)
        {
            case AccessLogged logged:
                _entries[(message.Source.StreamId, message.Source.StreamVersion)] = $"{logged.User} → {logged.Resource}";
                break;

            case StreamTruncatedMarker truncated:
                var pruned = _entries.Keys
                    .Where(k => k.Stream == truncated.StreamId && k.Version < truncated.BeforeVersion)
                    .ToList();
                foreach (var key in pruned)
                    _entries.Remove(key);
                logger.LogInformation(
                    "read model: pruned {Count} access-log entries from {Stream} (retention)",
                    pruned.Count, truncated.StreamId);
                break;
        }

        return Task.CompletedTask;
    }
}
