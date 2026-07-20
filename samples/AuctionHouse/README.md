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
                                             └─ store fires a best-effort "lot-42 changed" PING over the bus
                                                (Dbosoft.EventStore.Rebus — a signal + the stream id, not the event)
UI module's projection  ──told which lot changed──▶  re-reads just that lot's stream, re-folds its view
UI  ──Server-Sent Events──▶  browser re-renders live
```

The **read side belongs to the UI** — it builds the read model it renders. And it does so the simple way: it
does **not** run an ordered `$all` catch-up. The change ping carries the id of the changed stream, so the
projection just re-reads that one lot and re-folds its whole view. That read is a pure function of the
committed stream, so it is idempotent and **self-healing** — no global position, no checkpoint, no safety
window, and a lost ping only leaves a lot stale until the next ping or the periodic sweep. Because the writer
and the UI are **separate Hosuto modules with separate DI containers**, they share no in-process notifier: the
only thing that crosses between them is that ping over the in-memory bus (the `Dbosoft.EventStore.Rebus`
bridge). The bus never carries the events themselves — the **store is the source of truth**.

## The modules

| Project | Role |
| --- | --- |
| `AuctionHouse.WriterModule` | Write side. Handles `PlaceBid`/`ListLot` commands and appends events through an aggregate. Its store publishes the change ping (`UseRebusNotifications`). |
| `AuctionHouse.UIModule` | Read side + UI. A Hosuto web module that opens a **read-only** view of the store (`IEventReader`) and keeps its **own** read model current by re-reading and re-folding a lot whenever a ping says it changed — no `$all` catch-up, no checkpoint, no safety window. Serves a live page over Server-Sent Events and sends commands to the writer over the bus. |
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
- **Idempotency comes for free.** Because the projection re-derives a lot's whole view from its committed
  stream, re-running it is harmless — it converges to the current state. There is no per-event cursor to keep
  in step, so a duplicate or lost ping only affects latency, never correctness. (A periodic `$all` sweep is the
  backstop that re-derives everything, in case a ping was lost.)
- **Notify carries the stream id.** `IChangeNotifier` signals *which* stream changed and its watcher never
  drops a distinct stream between reads, which is what lets a fold-style read model re-derive per stream
  instead of running an ordered `$all` catch-up.
- This sample is deliberately isolated from the library's solution and CI (its own `Directory.Build.props`,
  inline package versions, not in `Dbosoft.EventStore.slnx`), because it pulls in external packages (Hosuto,
  Rebus) and targets a runnable app rather than the library's strict build rules.
