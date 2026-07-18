namespace Dbosoft.EventStore;

/// <summary>
/// One stream's part of a multi-stream atomic append: the <paramref name="Events"/> to add to
/// <paramref name="StreamId"/> under <paramref name="ExpectedVersion"/> (see <see cref="ExpectedVersion"/>
/// for the sentinels). Used with <c>IMultiStreamAppend</c>.
/// </summary>
[PublicAPI]
public sealed record StreamAppend(string StreamId, long ExpectedVersion, IReadOnlyList<EventData> Events);
