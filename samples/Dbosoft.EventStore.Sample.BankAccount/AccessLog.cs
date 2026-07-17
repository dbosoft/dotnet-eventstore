namespace Dbosoft.EventStore.Sample.BankAccount;

/// <summary>Marker (contract) for the access-log domain's events.</summary>
public interface IAccessLogEvent;

/// <summary>One access-log entry. A log stream is read, never folded as an aggregate.</summary>
public sealed record AccessLogged(string Resource, string User) : IAccessLogEvent;
