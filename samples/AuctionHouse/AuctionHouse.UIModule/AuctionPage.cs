namespace AuctionHouse.UIModule;

/// <summary>The single server-rendered page: a live auction board fed by Server-Sent Events.</summary>
internal static class AuctionPage
{
    public const string Html =
        """
        <!DOCTYPE html>
        <html lang="en">
        <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Auction House</title>
        <style>
        :root { color-scheme: light dark; }
        body { font-family: system-ui, sans-serif; margin: 0; background: #0f1115; color: #e6e6e6; }
        main { max-width: 900px; margin: 0 auto; padding: 2rem 1.5rem 4rem; }
        h1 { margin: 0 0 .25rem; font-size: 1.6rem; }
        .lede { color: #9aa4b2; margin: 0 0 1.25rem; font-size: .9rem; line-height: 1.45; }
        code { background: #1c2230; padding: .05rem .3rem; border-radius: 4px; }
        .who { display: block; margin-bottom: 1rem; font-size: .85rem; color: #9aa4b2; }
        table { width: 100%; border-collapse: collapse; }
        th, td { text-align: left; padding: .55rem .6rem; border-bottom: 1px solid #232833; }
        th { color: #9aa4b2; font-weight: 600; font-size: .78rem; text-transform: uppercase; letter-spacing: .03em; }
        td.price { font-variant-numeric: tabular-nums; font-weight: 700; color: #7ee0a0; }
        tr.closed td { color: #6b7280; }
        .muted { color: #6b7280; }
        .pill { font-size: .72rem; padding: .1rem .5rem; border-radius: 999px; background: #1c2230; color: #9aa4b2; }
        .pill.open { background: #12321f; color: #7ee0a0; }
        input[type=number], input[type=text] { background: #171b22; border: 1px solid #2a3140; color: #e6e6e6; border-radius: 6px; padding: .3rem .45rem; width: 6rem; }
        button { background: #2b6cff; color: #fff; border: 0; border-radius: 6px; padding: .35rem .7rem; cursor: pointer; font-weight: 600; }
        button:hover { background: #205ae0; }
        .newlot { margin-top: 2rem; padding: 1rem; border: 1px solid #232833; border-radius: 10px; background: #12151c; }
        .newlot h3 { margin: 0 0 .75rem; font-size: 1rem; }
        .row { display: flex; gap: .5rem; flex-wrap: wrap; align-items: center; }
        label { font-size: .8rem; color: #9aa4b2; display: flex; flex-direction: column; gap: .2rem; }
        .who input { width: 6rem; }
        </style>
        </head>
        <body>
        <main>
        <h1>🔨 Auction House</h1>
        <p class="lede">This grid is the <b>read model</b> — a fold of the event store's <code>$all</code> stream, built by the
        projection module. Place a bid and the UI sends a command to the writer over the in-memory bus; the writer appends an
        event; the store's change ping wakes the projection; the fold updates; and this page — pushed over Server-Sent Events —
        re-renders live. Three DI-separated modules, one process. Open a second tab to watch both update at once.</p>
        <label class="who">Bidding as <input type="text" id="who" value="you" /></label>
        <table>
        <thead><tr><th>Lot</th><th>Title</th><th>Seller</th><th>Price</th><th>Bids</th><th>High bidder</th><th>Status</th><th>Your bid</th></tr></thead>
        <tbody id="lots"><tr><td colspan="8" class="muted">Connecting…</td></tr></tbody>
        </table>
        <div class="newlot">
        <h3>List a new lot</h3>
        <div class="row">
        <label>Title<input type="text" id="nt" style="width:12rem" /></label>
        <label>Start price<input type="number" id="ns" value="100" /></label>
        <label>Seller<input type="text" id="nl" value="you" /></label>
        <button onclick="listLot()">List lot</button>
        </div>
        </div>
        </main>
        <script>
        const es = new EventSource('/events');
        es.onmessage = e => render(JSON.parse(e.data));
        function esc(s){ const d = document.createElement('div'); d.textContent = s == null ? '' : s; return d.innerHTML; }
        function who(){ return document.getElementById('who').value || 'you'; }
        function render(lots){
          const tb = document.getElementById('lots');
          tb.innerHTML = '';
          if(!lots.length){ tb.innerHTML = '<tr><td colspan="8" class="muted">No lots yet — list one below.</td></tr>'; return; }
          for(const l of lots){
            const tr = document.createElement('tr');
            if(l.Closed) tr.className = 'closed';
            const status = l.Closed ? `<span class="pill">closed · ${esc(l.Winner)}</span>` : '<span class="pill open">open</span>';
            const action = l.Closed ? '' : `<div class="row"><input type="number" min="0" id="b_${l.LotId}" /><button onclick="bid('${l.LotId}')">Bid</button></div>`;
            tr.innerHTML =
              `<td>${l.LotId}</td><td>${esc(l.Title)}</td><td>${esc(l.Seller)}</td>` +
              `<td class="price">${l.Price.toFixed(2)}</td><td>${l.BidCount}</td>` +
              `<td>${esc(l.HighBidder) || '—'}</td><td>${status}</td><td>${action}</td>`;
            tb.appendChild(tr);
          }
        }
        async function bid(lotId){
          const el = document.getElementById('b_' + lotId);
          const v = el && el.value;
          if(!v || +v <= 0) return;
          await fetch('/bid', { method: 'POST', body: new URLSearchParams({ lotId, amount: v, bidder: who() }) });
        }
        async function listLot(){
          const title = document.getElementById('nt').value;
          const startPrice = document.getElementById('ns').value;
          const seller = document.getElementById('nl').value || 'you';
          if(!title || !startPrice) return;
          await fetch('/list', { method: 'POST', body: new URLSearchParams({ title, startPrice, seller }) });
          document.getElementById('nt').value = '';
        }
        </script>
        </body>
        </html>
        """;
}
