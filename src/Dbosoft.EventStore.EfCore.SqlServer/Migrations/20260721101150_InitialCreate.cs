using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Dbosoft.EventStore.EfCore.SqlServer.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "es_checkpoints",
                columns: table => new
                {
                    Name = table.Column<string>(type: "nvarchar(256)", maxLength: 256, nullable: false),
                    Token = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_es_checkpoints", x => x.Name);
                });

            migrationBuilder.CreateTable(
                name: "es_events",
                columns: table => new
                {
                    Position = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    Domain = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    StreamId = table.Column<string>(type: "nvarchar(256)", maxLength: 256, nullable: false),
                    StreamVersion = table.Column<long>(type: "bigint", nullable: false),
                    EventId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    EventType = table.Column<string>(type: "nvarchar(512)", maxLength: 512, nullable: false),
                    Data = table.Column<byte[]>(type: "varbinary(max)", nullable: false),
                    Metadata = table.Column<byte[]>(type: "varbinary(max)", nullable: true),
                    ContentType = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    CreatedAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_es_events", x => x.Position);
                });

            migrationBuilder.CreateTable(
                name: "es_snapshots",
                columns: table => new
                {
                    Domain = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    StreamId = table.Column<string>(type: "nvarchar(256)", maxLength: 256, nullable: false),
                    StreamVersion = table.Column<long>(type: "bigint", nullable: false),
                    Payload = table.Column<byte[]>(type: "varbinary(max)", nullable: false),
                    ContentType = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    CreatedAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_es_snapshots", x => new { x.Domain, x.StreamId, x.StreamVersion });
                });

            migrationBuilder.CreateTable(
                name: "es_streams",
                columns: table => new
                {
                    Domain = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    StreamId = table.Column<string>(type: "nvarchar(256)", maxLength: 256, nullable: false),
                    Version = table.Column<long>(type: "bigint", nullable: false),
                    TruncatedBefore = table.Column<long>(type: "bigint", nullable: false),
                    IsDeleted = table.Column<bool>(type: "bit", nullable: false),
                    HardDeleted = table.Column<bool>(type: "bit", nullable: false),
                    Metadata = table.Column<string>(type: "nvarchar(max)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_es_streams", x => new { x.Domain, x.StreamId });
                });

            migrationBuilder.CreateIndex(
                name: "IX_es_events_Domain_Position",
                table: "es_events",
                columns: new[] { "Domain", "Position" });

            migrationBuilder.CreateIndex(
                name: "IX_es_events_Domain_StreamId_EventId",
                table: "es_events",
                columns: new[] { "Domain", "StreamId", "EventId" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_es_events_Domain_StreamId_StreamVersion",
                table: "es_events",
                columns: new[] { "Domain", "StreamId", "StreamVersion" },
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "es_checkpoints");

            migrationBuilder.DropTable(
                name: "es_events");

            migrationBuilder.DropTable(
                name: "es_snapshots");

            migrationBuilder.DropTable(
                name: "es_streams");
        }
    }
}
