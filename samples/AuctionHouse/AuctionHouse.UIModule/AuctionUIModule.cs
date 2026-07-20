using System.Text.Json;
using System.Threading.Channels;
using AuctionHouse.Contracts;
using AuctionHouse.Hosting;
using Dbosoft.Hosuto.Modules;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Rebus.Transport.InMem;

namespace AuctionHouse.UIModule;

/// <summary>
/// The UI, hosted as a Hosuto web module. It serves a small server-rendered page whose live updates come over
/// Server-Sent Events: the page consumes the shared read model (written by the projection module) and re-renders
/// whenever it changes, and reaches the write side by sending commands to the writer's queue over the shared
/// bus. Its own container, no write handle on the store.
/// </summary>
public class AuctionUIModule : WebModule
{
    public override string Path => "/";

    public void ConfigureServices(IServiceProvider sharedSp, IServiceCollection services)
    {
        var network = sharedSp.GetRequiredService<InMemNetwork>();

        // The shared read model (written by the projection module), and a send-only bus to reach the writer.
        services.AddSingleton(sharedSp.GetRequiredService<ILotReadStore>());
        services.AddOneWayModuleBus(network);
        services.AddScoped<AuctionUiClient>();
        services.AddRouting();
    }

    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        app.UseRouting();
        app.UseEndpoints(endpoints =>
        {
            endpoints.MapGet("/", () => Results.Content(AuctionPage.Html, "text/html; charset=utf-8"));

            endpoints.MapGet("/events", StreamEventsAsync);

            endpoints.MapPost("/bid", async (HttpContext ctx, AuctionUiClient client) =>
            {
                var form = await ctx.Request.ReadFormAsync();
                if (decimal.TryParse(form["amount"], out var amount) && amount > 0 && !string.IsNullOrEmpty(form["lotId"]))
                    await client.PlaceBid(form["lotId"]!, amount, Who(form["bidder"]!));
                return Results.NoContent();
            });

            endpoints.MapPost("/list", async (HttpContext ctx, AuctionUiClient client) =>
            {
                var form = await ctx.Request.ReadFormAsync();
                if (!string.IsNullOrWhiteSpace(form["title"]) && decimal.TryParse(form["startPrice"], out var start) && start > 0)
                    await client.ListLot(form["title"]!, start, Who(form["seller"]!));
                return Results.NoContent();
            });
        });
    }

    // Server-Sent Events: the last hop of the chain. The change ping already woke the projection and updated the
    // shared read model; here we push each new snapshot to the browser, which re-renders live.
    private static async Task StreamEventsAsync(HttpContext ctx, ILotReadStore store, CancellationToken ct)
    {
        ctx.Response.Headers.ContentType = "text/event-stream";
        ctx.Response.Headers.CacheControl = "no-cache";

        var channel = Channel.CreateUnbounded<string>();
        void OnChanged() => channel.Writer.TryWrite(Snapshot(store));
        store.Changed += OnChanged;
        try
        {
            await WriteEventAsync(ctx, Snapshot(store), ct); // initial state on connect
            await foreach (var json in channel.Reader.ReadAllAsync(ct))
                await WriteEventAsync(ctx, json, ct);
        }
        catch (OperationCanceledException)
        {
            // the client disconnected — fall through to unsubscribe
        }
        finally
        {
            store.Changed -= OnChanged;
        }
    }

    private static string Snapshot(ILotReadStore store) =>
        JsonSerializer.Serialize(store.All().Select(l => new
        {
            l.LotId,
            l.Title,
            l.Seller,
            Price = l.CurrentPrice,
            l.BidCount,
            l.HighBidder,
            l.Closed,
            l.Winner
        }));

    private static async Task WriteEventAsync(HttpContext ctx, string json, CancellationToken ct)
    {
        await ctx.Response.WriteAsync($"data: {json}\n\n", ct);
        await ctx.Response.Body.FlushAsync(ct);
    }

    private static string Who(string? value) => string.IsNullOrWhiteSpace(value) ? "you" : value;
}
