# Auction House — event store + Hosuto modules + change-notification bridge

A live auction where the **read side wakes on writes without a message bus carrying the events**. It shows
the library's change-notification design in a realistic modular app built on [Dbosoft.Hosuto](https://github.com/dbosoft/Hosuto):
a **writer** module and a **UI** module run in one process, each with its own DI container, sharing only the
store (a database) and a change ping — exactly the shape that lets a module be split into its own process later.

## What it demonstrates

The whole point is this chain, and that it crosses the module boundary:

```
UI (bid)  ──command over in-mem bus──▶  Writer module
                                          └─ appends BidPlaced to the lot's stream
                                             └─ store fires a best-effort "changed" PING over the bus
                                                (Dbosoft.EventStore.Rebus — a signal, not the event)
UI module's projection  ──woken by the ping──▶  reads the store's ordered $all, folds it into its read model
UI  ──Server-Sent Events──▶  browser re-renders live
```

The **read side belongs to the UI** — it builds the read model it renders, by tailing the store. Because the
writer and the UI are **separate Hosuto modules with separate DI containers**, they share no in-process
notifier: the only thing that crosses between them is the store's **change ping** over the in-memory bus (the
`Dbosoft.EventStore.Rebus` bridge). The bus never carries the events themselves — the **store is the ordered
log and the source of truth**, and a lost ping only costs latency because the subscription still polls as a
backstop.

## The modules

| Project | Role |
| --- | --- |
| `AuctionHouse.WriterModule` | Write side. Handles `PlaceBid`/`ListLot` commands and appends events through an aggregate. Its store publishes the change ping (`UseRebusNotifications`). |
| `AuctionHouse.UIModule` | Read side + UI. A Hosuto web module that opens a **read-only** view of the store, folds `$all` into its **own** read model with an `EventStoreSubscription.ForReader` subscription woken by the ping, serves a live page over Server-Sent Events, and sends commands to the writer over the bus. |
| `AuctionHouse.Contracts` | Events, commands, the `LotView` read model and the `ILotReadStore` type. |
| `AuctionHouse.Hosting` | Cross-cutting host wiring: the shared in-memory Rebus network, the connection string, and the per-module bus helper. |
| `AuctionHouse.Server` | Composes the two modules into one process (`ModulesHost` + `HostModule<T>`), over one in-memory SQLite event store. |

Each module registers its own event store, read model, bus and handlers in its **own** container. The host
container holds only the shared `InMemNetwork` and the connection string each module opens its own store
against — the store (a database) is the only state the modules share. Flip the bus transport and point the
connection string at a real database, and a module splits into its own process with **no module-code change**.

## Run it

```sh
dotnet run --project AuctionHouse.Server
```

Then open <http://localhost:5005>. A couple of lots are seeded and a short burst of rival bids plays out, so
the grid moves on its own; then place your own bids (the bid box is pre-filled with a winning amount) and watch
it update live. Open a second tab to see both update at once.

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
