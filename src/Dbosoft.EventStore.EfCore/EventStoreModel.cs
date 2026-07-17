using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace Dbosoft.EventStore.EfCore;

/// <summary>
/// Composable, provider-specific model tweaks a registration passes to <c>UseEfCore(configureModel)</c>.
/// The base model is provider-agnostic; pick the pieces a given database needs.
/// </summary>
[PublicAPI]
public static class EventStoreModel
{
    /// <summary>
    /// Makes the stream head's version an optimistic-concurrency token, so an append updates it with
    /// <c>WHERE Version = @expected</c> and a lost race surfaces as a clean concurrency conflict. The
    /// unique index already makes append race-free without it; this yields tidier conflict detection.
    /// Works on SQL databases that support value concurrency tokens (SQL Server, PostgreSQL, SQLite).
    /// </summary>
    public static ModelBuilder UseVersionConcurrencyToken(this ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<StreamRow>().Property(s => s.Version).IsConcurrencyToken();
        return modelBuilder;
    }

    /// <summary>
    /// Stores event and snapshot timestamps as sortable UTC ticks. Needed for databases that cannot
    /// translate ordering comparisons on a native timestamp type (SQLite stores DateTimeOffset as
    /// TEXT); leave it off where the native type is orderable (SQL Server, PostgreSQL).
    /// </summary>
    public static ModelBuilder StoreTimestampsAsTicks(this ModelBuilder modelBuilder)
    {
        var toTicks = new ValueConverter<DateTimeOffset, long>(d => d.UtcTicks, t => new DateTimeOffset(t, TimeSpan.Zero));
        modelBuilder.Entity<EventRow>().Property(e => e.CreatedAt).HasConversion(toTicks);
        modelBuilder.Entity<SnapshotRow>().Property(s => s.CreatedAt).HasConversion(toTicks);
        return modelBuilder;
    }
}
