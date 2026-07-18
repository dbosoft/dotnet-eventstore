using Azure;
using Azure.Core;
using Azure.Core.Pipeline;

namespace Dbosoft.EventStore.AzureTables.Tests;

/// <summary>
/// An Azure SDK pipeline policy that fails requests a test chooses, so partial-failure paths (a lost
/// marker, a vanished blob) can be exercised. Added per-call, so it fires once per operation and a
/// thrown fault is not retried by the SDK — making fault counting deterministic.
/// </summary>
public sealed class FaultInjectionPolicy : HttpPipelinePolicy
{
    /// <summary>When set and it returns true for a request, that request fails with <see cref="Status"/>.</summary>
    public Func<Request, bool>? ShouldFail { get; set; }

    public int Status { get; set; } = 500;

    public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
    {
        Fail(message.Request);
        ProcessNext(message, pipeline);
    }

    public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
    {
        Fail(message.Request);
        await ProcessNextAsync(message, pipeline).ConfigureAwait(false);
    }

    private void Fail(Request request)
    {
        if (ShouldFail?.Invoke(request) == true)
            throw new RequestFailedException(Status, "Injected fault");
    }

    /// <summary>Whether a request is an entity-group transaction (a batch write).</summary>
    public static bool IsBatch(Request request) =>
        request.Method == RequestMethod.Post && request.Uri.ToString().Contains("$batch", StringComparison.Ordinal);
}
