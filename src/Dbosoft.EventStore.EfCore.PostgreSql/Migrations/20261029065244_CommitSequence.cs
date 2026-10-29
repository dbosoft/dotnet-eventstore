using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Dbosoft.EventStore.EfCore.PostgreSql.Migrations
{
    /// <inheritdoc />
    public partial class CommitSequence : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_es_events_Domain_Position",
                table: "es_events");

            migrationBuilder.AddColumn<long>(
                name: "CommitSequence",
                table: "es_events",
                type: "bigint",
                nullable: true);

            migrationBuilder.CreateTable(
                name: "es_sequence",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false),
                    Last = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_es_sequence", x => x.Id);
                });

            migrationBuilder.InsertData(
                table: "es_sequence",
                columns: new[] { "Id", "Last" },
                values: new object[] { 1, 0L });

            // Events stored before keep their order and their position, so a checkpoint saved before stays
            // valid; the counter continues after the highest.
            migrationBuilder.Sql(@"UPDATE es_events SET ""CommitSequence"" = ""Position"";
UPDATE es_sequence SET ""Last"" = COALESCE((SELECT MAX(""Position"") FROM es_events), 0) WHERE ""Id"" = 1;");

            migrationBuilder.CreateIndex(
                name: "IX_es_events_Domain_CommitSequence",
                table: "es_events",
                columns: new[] { "Domain", "CommitSequence" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "es_sequence");

            migrationBuilder.DropIndex(
                name: "IX_es_events_Domain_CommitSequence",
                table: "es_events");

            migrationBuilder.DropColumn(
                name: "CommitSequence",
                table: "es_events");

            migrationBuilder.CreateIndex(
                name: "IX_es_events_Domain_Position",
                table: "es_events",
                columns: new[] { "Domain", "Position" });
        }
    }
}
