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
    /// <summary>
    /// The default migrations-history table the provider registers, distinct from EF's
    /// <c>__EFMigrationsHistory</c> so the store's migrations never share a history table with a consumer's own
    /// migrations in the same database. Matches the <c>es_</c> prefix of the store's tables; overridable per
    /// registration.
    /// </summary>
    public const string DefaultMigrationsHistoryTable = "es_migrations_history";

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
    internal DbSet<SequenceRow> Sequence => Set<SequenceRow>();

    /// <summary>The id of the one counter row.</summary>
    internal const int SequenceId = 1;

    // Event rows this context inserted that have no commit sequence yet. Collected on save, so every enlisted
    // insert — an append, a maintenance marker — is sequenced at prepare without each writer having to remember it.
    private readonly List<EventRow> _unsequenced = [];

    /// <summary>Event rows this context has inserted and not yet given a commit sequence.</summary>
    internal IReadOnlyList<EventRow> Unsequenced => _unsequenced;

    /// <summary>Drops the rows waiting for a sequence, for when their insert was discarded.</summary>
    internal void ForgetUnsequenced() => _unsequenced.Clear();

    /// <inheritdoc />
    public override async Task<int> SaveChangesAsync(bool acceptAllChangesOnSuccess, CancellationToken cancellationToken = default)
    {
        var inserted = InsertedEvents();
        var saved = await base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken).ConfigureAwait(false);
        AwaitSequence(inserted);
        return saved;
    }

    /// <inheritdoc />
    public override int SaveChanges(bool acceptAllChangesOnSuccess)
    {
        var inserted = InsertedEvents();
        var saved = base.SaveChanges(acceptAllChangesOnSuccess);
        AwaitSequence(inserted);
        return saved;
    }

    // Rows inserted without a sequence are the ones an enlisted append leaves for the frame's PrepareAsync; an
    // autonomous save gives its rows their sequence before inserting them.
    private List<EventRow> InsertedEvents() => ChangeTracker.Entries<EventRow>()
        .Where(e => e.State == EntityState.Added && e.Entity.CommitSequence is null)
        .Select(e => e.Entity)
        .ToList();

    private void AwaitSequence(List<EventRow> inserted)
    {
        foreach (var row in inserted)
        {
            if (!_unsequenced.Contains(row))
                _unsequenced.Add(row);
        }
    }

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
            b.HasIndex(e => new { e.Domain, e.CommitSequence });
        });

        modelBuilder.Entity<SequenceRow>(b =>
        {
            b.ToTable("es_sequence");
            b.HasKey(s => s.Id);
            b.Property(s => s.Id).ValueGeneratedNever();
            b.HasData(new SequenceRow { Id = SequenceId, Last = 0 });
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
            b.Property(c => c.Token).HasMaxLength(128);
        });

        // Registration supplies any provider-specific model tweaks (concurrency token, converters).
        _configureModel?.Invoke(modelBuilder);
    }
}
