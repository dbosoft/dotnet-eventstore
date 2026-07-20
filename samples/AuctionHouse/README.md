# Auction House — event store + Hosuto modules + change-notification bridge

A live auction where the **read side wakes on writes without a message bus carrying the events**. It shows
the library's change-notification design in a realistic modular app built on [Dbosoft.Hosuto](https://github.com/dbosoft/Hosuto):
several independent modules run in one process, each with its own DI container, sharing only cross-cutting
infrastructure — exactly the shape that lets a module be split into its own process later.

## What it demonstrates

The whole point is this chain, and that it crosses module boundaries:

```
UI (bid)  ──command over in-mem bus──▶  Writer module
                                          └─ appends BidPlaced to the lot's stream
                                             └─ store fires a best-effort "changed" PING over the bus
                                                (Dbosoft.EventStore.Rebus — a signal, not the event)
Projection module  ──woken by the ping──▶  reads the store's ordered $all, folds it into the read model
UI  ──Server-Sent Events──▶  browser re-renders live
```

Because the writer and the projection are **separate Hosuto modules with separate DI containers**, they share
no in-process notifier. The only thing that crosses between them is the store's **change ping** over the
in-memory bus (the `Dbosoft.EventStore.Rebus` bridge). The bus never carries the events themselves — the
**store is the ordered log and the source of truth**, and a lost ping only costs latency because the
subscription still polls as a backstop.

## The modules

| Project | Role |
| --- | --- |
| `AuctionHouse.WriterModule` | Write side. Handles `PlaceBid`/`ListLot` commands, appends events through an aggregate. Its store publishes the change ping (`UseRebusNotifications`). |
| `AuctionHouse.ProjectionModule` | Read side. A **read-only** `$all` subscription (`EventStoreSubscription.ForReader`) that wakes on the ping and folds events into the shared read model. |
| `AuctionHouse.UIModule` | A Hosuto web module serving a live page over Server-Sent Events, and sending commands to the writer over the bus. |
| `AuctionHouse.Contracts` | Events, commands, the `LotView` read model and the shared `ILotReadStore`. |
| `AuctionHouse.Hosting` | Cross-cutting host wiring: the shared in-memory Rebus network, the connection string, and the per-module bus helper. |
| `AuctionHouse.Server` | Composes the three modules into one process (`ModulesHost` + `HostModule<T>`), with an in-memory SQLite event store shared by all. |

Each module registers its own event store, bus and handlers in its **own** container. The host container
holds only the shared `InMemNetwork`, the connection string, and the `ILotReadStore` — the analogue of a
shared read database. Flip the bus transport and point the connection string at a real database, and the
projection (or the UI) splits into its own process with **no module-code change**.

## Run it

```sh
dotnet run --project AuctionHouse.Server
```

Then open <http://localhost:5005>. A couple of lots are seeded and a short burst of rival bids plays out, so
the grid moves on its own; then place your own bids and watch it update live. Open a second tab to see both
update at once. The same read model is also printed to the console as it changes.

## Notes

- **Why Server-Sent Events, not Blazor?** The UI is a Hosuto web module. On .NET 10, Blazor's client script
  (`blazor.web.js`) is delivered through the framework's `MapStaticAssets` endpoint registration, which
  Hosuto's (pre-.NET-10) module web host does not perform — so the Blazor circuit can't start inside a module
  without a Hosuto-side change. SSE needs no framework static assets, runs in any Hosuto web module, and gives
  a true server push, so it shows the same live-update principle end to end.
- **Idempotency.** Delivery is at-least-once; the projection skips any event whose stream version it has
  already folded (`LotView.LastVersion`).
- This sample is deliberately isolated from the library's solution and CI (its own `Directory.Build.props`,
  inline package versions, not in `Dbosoft.EventStore.slnx`), because it pulls in external packages (Hosuto,
  Rebus) and targets a runnable app rather than the library's strict build rules.
