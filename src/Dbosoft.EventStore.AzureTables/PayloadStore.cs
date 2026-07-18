using Azure.Storage.Blobs;

namespace Dbosoft.EventStore.AzureTables;

/// <summary>How a payload field is held: absent, inline in the entity, or spilled to a blob.</summary>
internal enum PayloadMode
{
    Null = 0,
    Inline = 1,
    Blob = 2
}

/// <summary>An event or snapshot payload field, either carried inline or referencing a blob.</summary>
internal readonly struct PayloadSlot
{
    private PayloadSlot(PayloadMode mode, ReadOnlyMemory<byte> inline, string? blobName)
    {
        Mode = mode;
        Inline = inline;
        BlobName = blobName;
    }

    public PayloadMode Mode { get; }
    public ReadOnlyMemory<byte> Inline { get; }
    public string? BlobName { get; }

    public static PayloadSlot None => new(PayloadMode.Null, default, null);
    public static PayloadSlot OfInline(ReadOnlyMemory<byte> value) => new(PayloadMode.Inline, value, null);
    public static PayloadSlot OfBlob(string blobName) => new(PayloadMode.Blob, default, blobName);
}

/// <summary>
/// Large-payload storage for one domain: a field small enough stays inline in its table entity; a
/// field that would exceed the inline limit is written to a blob and referenced by name. Reads resolve
/// the reference transparently, so the size ceiling of a table entity does not bound event or snapshot
/// payloads.
/// </summary>
internal sealed class PayloadStore(BlobContainerClient container)
{
    // A table binary property caps at 64 KB; stay comfortably under it so a field plus the rest of the
    // entity never breaches the 1 MB entity limit either.
    private const int InlineLimit = 60_000;

    public async Task<PayloadSlot> PutAsync(string blobName, ReadOnlyMemory<byte>? value, CancellationToken ct)
    {
        if (value is not { } bytes)
            return PayloadSlot.None;
        if (bytes.Length <= InlineLimit)
            return PayloadSlot.OfInline(bytes);

        await container.GetBlobClient(blobName)
            .UploadAsync(BinaryData.FromBytes(bytes), overwrite: true, ct)
            .ConfigureAwait(false);
        return PayloadSlot.OfBlob(blobName);
    }

    public async Task<ReadOnlyMemory<byte>?> GetAsync(PayloadSlot slot, CancellationToken ct)
    {
        switch (slot.Mode)
        {
            case PayloadMode.Null:
                return null;
            case PayloadMode.Inline:
                return slot.Inline;
            default:
                var content = await container.GetBlobClient(slot.BlobName!)
                    .DownloadContentAsync(ct).ConfigureAwait(false);
                return content.Value.Content.ToMemory();
        }
    }

    public Task DeleteAsync(string blobName, CancellationToken ct) =>
        container.DeleteBlobIfExistsAsync(blobName, cancellationToken: ct);
}
