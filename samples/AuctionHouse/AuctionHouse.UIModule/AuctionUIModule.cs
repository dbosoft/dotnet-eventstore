using System.Text.Json;
using System.Threading.Channels;
using AuctionHouse.Contracts;
using AuctionHouse.Hosting;
using Dbosoft.EventStore.DependencyInjection;
using Dbosoft.EventStore.EfCore.Sqlite;
using Dbosoft.EventStore.Rebus;
using Dbosoft.Hosuto.Modules;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Rebus.Transport.InMem;

namespace AuctionHouse.UIModule;

/// <summary>
/// The read side, hosted as a Hosuto web module. It owns everything the UI needs: a <b>read-only</b> view of the
/// shared event store, its <b>own</b> read model, and the projection that folds <c>$all</c> into it. The page
/// pushes updates over Server-Sent Events. It reaches the write side by sending commands to the writer's queue
/// over the shared bus — its own container, no write handle, and nothing shared with the writer but the store
/// itself (a database) and the change ping.
/// </summary>
public class AuctionUIModule : WebModule
{
    public override string Path => "/";

    public void ConfigureServices(IServiceProvider sharedSp, IServiceCollection services)
    {
        var network = sharedSp.GetRequiredService<InMemNetwork>();
        var connectionString = sharedSp.GetRequiredService<AuctionConnectionString>().Value;

        // A read-only view of the store plus the change-notification bridge (the notifier the subscription
        // waits on and the handler that releases it when the writer's ping arrives).
        services.AddEventStore(es => es
            .UseSqlite(connectionString)
            .UseRebusNotifications()
            .AddDomain(AuctionDomain.Name, domain => domain
                .AddEventTypesImplementing<IAuctionEvent>()));

        // This module's own read model, and the projection that fills it from $all.
        services.AddSingleton<ILotReadStore, InMemoryLotReadStore>();
        services.AddHostedService<ProjectionRunner>();

        // A full bus (input queue): it receives the writer's change pings and sends commands to the writer.
        services.AddModuleBus(network, QueueNames.Ui);
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

    // Server-Sent Events: the last hop of the chain. The change ping already woke the projection and updated
    // this module's read model; here we push each new snapshot to the browser, which re-renders live.
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
