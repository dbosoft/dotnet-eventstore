using Microsoft.EntityFrameworkCore;

namespace Dbosoft.EventStore.EfCore;

/// <summary>
/// The event store's EF Core model. Domains are realized as an indexed discriminator column rather
/// than physically separate tables — same per-domain ordering and read isolation, simpler schema.
/// (Physical table-per-domain is a possible later isolation option.)
///
/// The model here is deliberately provider-agnostic: keys, indexes, sizes only. Anything
/// database-specific — the optimistic-concurrency token, a value converter for a database that cannot
/// compare a native type — is supplied at registration through <c>configureModel</c>, not hardcoded.
/// </summary>
[PublicAPI]
public sealed class EventStoreDbContext : DbContext
{
    private readonly Action<ModelBuilder>? _configureModel;

    /// <summary>Creates the context with the given options.</summary>
    public EventStoreDbContext(DbContextOptions<EventStoreDbContext> options) : base(options)
    {
    }

    /// <summary>Creates the context with a registration-supplied, provider-specific model hook.</summary>
    public EventStoreDbContext(DbContextOptions<EventStoreDbContext> options, Action<ModelBuilder>? configureModel)
        : base(options) =>
        _configureModel = configureModel;

    internal DbSet<StreamRow> Streams => Set<StreamRow>();
    internal DbSet<EventRow> Events => Set<EventRow>();
    internal DbSet<SnapshotRow> Snapshots => Set<SnapshotRow>();
    internal DbSet<CheckpointRow> Checkpoints => Set<CheckpointRow>();

    /// <inheritdoc />
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<StreamRow>(b =>
        {
            b.ToTable("es_streams");
            b.HasKey(s => new { s.Domain, s.StreamId });
            b.Property(s => s.Domain).HasMaxLength(128);
            b.Property(s => s.StreamId).HasMaxLength(256);
        });

        modelBuilder.Entity<EventRow>(b =>
        {
            b.ToTable("es_events");
            b.HasKey(e => e.Position);
            b.Property(e => e.Position).ValueGeneratedOnAdd();
            b.Property(e => e.Domain).HasMaxLength(128);
            b.Property(e => e.StreamId).HasMaxLength(256);
            b.Property(e => e.EventType).HasMaxLength(512);
            b.Property(e => e.ContentType).HasMaxLength(128);
            b.HasIndex(e => new { e.Domain, e.StreamId, e.StreamVersion }).IsUnique();
            b.HasIndex(e => new { e.Domain, e.StreamId, e.EventId }).IsUnique();
            b.HasIndex(e => new { e.Domain, e.Position });
        });

        modelBuilder.Entity<SnapshotRow>(b =>
        {
            b.ToTable("es_snapshots");
            b.HasKey(s => new { s.Domain, s.StreamId, s.StreamVersion });
            b.Property(s => s.Domain).HasMaxLength(128);
            b.Property(s => s.StreamId).HasMaxLength(256);
            b.Property(s => s.ContentType).HasMaxLength(128);
        });

        modelBuilder.Entity<CheckpointRow>(b =>
        {
            b.ToTable("es_checkpoints");
            b.HasKey(c => c.Name);
            b.Property(c => c.Name).HasMaxLength(256);
        });

        // Registration supplies any provider-specific model tweaks (concurrency token, converters).
        _configureModel?.Invoke(modelBuilder);
    }
}
