# app.py — FastAPI + WebSocket + UI (graphite×lime)
# Adds: min-liquidity, slippage, start amount, paper toggle, tuned alerts

import asyncio
from typing import Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from arb_core import prepare, scan_once, discover_top_quotes

app = FastAPI(title="Triangular Arbitrage Monitor Pro")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

HTML = r"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Triangular Arbitrage Monitor</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    :root{--graphite:#0b0f14;--graphite-2:#101720;--ink:#cfd6de;--muted:#9aa3ad;--lime:#a3f37c;--border:#1b2735;--card:#0f1a22;--radius:14px;--shadow:0 6px 24px rgba(0,0,0,.28)}
    *{box-sizing:border-box}
    body{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,'Segoe UI',Roboto,Arial;background:linear-gradient(180deg,var(--graphite),var(--graphite-2));color:var(--ink)}
    header{position:sticky;top:0;z-index:50;backdrop-filter:blur(6px);background:rgba(11,15,20,.7);border-bottom:1px solid var(--border)}
    .container{max-width:1200px;margin:0 auto;padding:18px}
    h1{margin:0 0 8px 0;font-weight:700;display:flex;align-items:center;gap:.6rem}
    .badge{background:rgba(163,243,124,.1);color:var(--lime);border:1px solid rgba(163,243,124,.35);padding:2px 8px;border-radius:999px;font-size:12px}
    .panel{background:linear-gradient(180deg,var(--card),#0c141d);border:1px solid var(--border);border-radius:var(--radius);box-shadow:var(--shadow);padding:12px 14px;margin-top:12px}
    .row{display:flex;flex-wrap:wrap;gap:10px;align-items:center}
    label{opacity:.85;font-size:13px;margin-right:6px}
    input,button{height:34px;border-radius:10px;border:1px solid var(--border);background:#0b1420;color:var(--ink);padding:0 10px;outline:none}
    input.small{width:180px} input.xs{width:76px}
    button{background:linear-gradient(180deg,#111b29,#0b1420);cursor:pointer}
    button.primary{background:linear-gradient(180deg,rgba(163,243,124,.18),rgba(163,243,124,.05));border-color:rgba(163,243,124,.35);color:var(--lime);font-weight:600}
    .status{font-size:12px;color:var(--muted)}
    table{border-collapse:collapse;width:100%;margin-top:8px;font-size:14px}
    th,td{border-bottom:1px solid var(--border);padding:8px 10px}
    th{text-align:left;color:var(--muted);font-weight:600}
    .mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
    .muted{color:var(--muted)} .green{color:var(--lime)}
    .ex{margin-top:14px;border:1px solid var(--border);border-radius:var(--radius);padding:10px 12px;background:#0c141d}
    .hdr{display:flex;align-items:center;justify-content:space-between;gap:10px}
    .chip{display:inline-flex;align-items:center;gap:6px;padding:4px 8px;border-radius:999px;border:1px solid var(--border);background:#0b1420;color:var(--muted);font-size:12px}
    .grid{display:grid;grid-template-columns:1fr;gap:12px}
    @media(min-width:900px){.grid{grid-template-columns:1fr 1fr}}
  </style>
</head>
<body>
<header>
  <div class="container">
    <h1>Triangular Arbitrage Monitor <span class="badge">graphite × lime</span></h1>

    <div class="panel">
      <div class="row">
        <label>Exchanges</label><input id="ex" class="small" value="binance,okx,kucoin,mexc,bybit,exmo">
        <label>Quotes</label><input id="quotes" class="small" value="USDT,USDC,FDUSD,BTC,ETH">
        <label>Threshold %</label><input id="th" class="xs" value="0.05">
        <label>Fee</label><input id="fee" class="xs" value="0.001">
        <label>Interval (s)</label><input id="intv" class="xs" value="1.0">
        <label>Auto Top-N</label><input id="auto" type="checkbox" checked><input id="topn" class="xs" value="100" title="Top-N bases">
        <label>Auto Quotes</label><input id="autoq" type="checkbox" checked><input id="qtop" class="xs" value="6" title="Top-k quotes">
      </div>
      <div class="row" style="margin-top:8px">
        <label>Cross-X</label><input id="xsp" type="checkbox" checked>
        <label>≥%</label><input id="xth" class="xs" value="0.20">
        <label>Min Liquidity (USD)</label><input id="minliq" class="small" value="5000">
        <label>Max Slippage (bps)</label><input id="slipbps" class="xs" value="30">
        <label>Start amount</label><input id="start" class="xs" value="200">
        <label>Paper</label><input id="paper" type="checkbox" checked>
        <label>🔔 Alerts</label><input id="alerts" type="checkbox" checked>
        <label>Tri ≥%</label><input id="ath_tri" class="xs" value="0.50">
        <label>Cross-X ≥%</label><input id="ath_x" class="xs" value="0.30">
        <button id="apply" class="primary">Apply</button>
        <span class="status" id="status">connecting...</span>
      </div>
    </div>

    <div class="panel mono muted" id="gstats">global: loading...</div>
    <div class="panel mono muted" id="qbadge" style="font-size:13px;display:none;"></div>
  </div>
</header>

<main class="container">
  <div class="grid">
    <div class="panel">
      <table>
        <thead>
          <tr><th colspan="7">Cross-exchange Spreads (net ≥ threshold)</th></tr>
          <tr><th>#</th><th>Pair</th><th>Buy @</th><th>Sell @</th><th>Gross %</th><th>Net %</th><th>Time</th></tr>
        </thead>
        <tbody id="xbox"><tr><td colspan="7" class="muted">Waiting...</td></tr></tbody>
      </table>
    </div>

    <div class="panel">
      <div id="ex-box"></div>
    </div>
  </div>
</main>

<script>
let ws;
let params = {
  ex: "binance,okx,kucoin,mexc,bybit,exmo",
  quotes: "USDT,USDC,FDUSD,BTC,ETH",
  threshold: 0.05, fee: 0.001, interval: 1.0, auto: true, topn: 100,
  autoq: true, qtop: 6, xsp: true, xth: 0.20,
  minliq: 5000, slipbps: 30, start: 200, paper: true,
  alerts: true, ath_tri: 0.50, ath_x: 0.30,
};

let audioCtx;
function ping(){
  try{
    if(!audioCtx) audioCtx = new (window.AudioContext||window.webkitAudioContext)();
    const o = audioCtx.createOscillator(); const g = audioCtx.createGain();
    o.type = 'sine'; o.frequency.value = 880; g.gain.value = 0.0001;
    o.connect(g); g.connect(audioCtx.destination); o.start();
    const now = audioCtx.currentTime;
    g.gain.exponentialRampToValueAtTime(0.06, now + 0.01);
    g.gain.exponentialRampToValueAtTime(0.00001, now + 0.25);
    o.stop(now + 0.26);
  }catch(e){}
}
async function askNotify(){
  if (!("Notification" in window)) return;
  if (Notification.permission === "granted") return;
  try{ await Notification.requestPermission(); }catch(e){}
}
function notify(title, body){
  if (!params.alerts) return;
  try{
    if (("Notification" in window) && Notification.permission==="granted"){
      const n = new Notification(title, { body }); setTimeout(()=>n.close(), 4000);
    }
  }catch(e){}
  ping();
}

function connect() {
  if (ws) ws.close();

  const url = `ws://${location.host}/ws?ex=${encodeURIComponent(params.ex)}`
            + `&quotes=${encodeURIComponent(params.quotes)}`
            + `&threshold=${params.threshold}&fee=${params.fee}&interval=${params.interval}`
            + `&auto=${params.auto?1:0}&topn=${params.topn}`
            + `&xsp=${params.xsp?1:0}&xth=${params.xth}`
            + `&autoq=${params.autoq?1:0}&qtop=${params.qtop}`
            + `&minliq=${params.minliq}&slipbps=${params.slipbps}&start=${params.start}&paper=${params.paper?1:0}`;

  ws = new WebSocket(url);
  const status = document.getElementById('status');
  const exBox = document.getElementById('ex-box');
  const gstats = document.getElementById('gstats');
  const xbox = document.getElementById('xbox');
  const qbadge = document.getElementById('qbadge');

  ws.onopen = () => { status.textContent = 'connected'; askNotify(); };
  ws.onclose = () => status.textContent = 'disconnected';
  ws.onerror = () => status.textContent = 'error';

  let lastTriAlert = 0;
  let lastXAlert = 0;

  ws.onmessage = (ev) => {
    const data = JSON.parse(ev.data);

    if (data.quotes_used && data.quotes_used.length) {
      qbadge.style.display = 'block';
      qbadge.textContent = 'using quotes: ' + data.quotes_used.join(', ');
    } else {
      qbadge.style.display = 'none';
    }

    const gb = data.summary && data.summary.global_best;
    const tri = data.summary ? data.summary.total_triangles : 0;
    if (gb) {
      gstats.textContent = `triangles: ${tri} | best_global: ${gb.profit_pct.toFixed(6)}% | ex=${gb.exchange} | ${gb.quote} | ${gb.legs.join(' / ')} | ` + gb.route.map(r => `${r.from}->${r.to} [${r.symbol}]`).join(' -> ');
      // Alert tuning: only fire for realistic band 0.5–5.0%
      if (params.alerts && gb.profit_pct >= 0.5 && gb.profit_pct <= 5.0 && (Date.now()-lastTriAlert>3000)) {
        notify('Triangle 🚀 ' + gb.profit_pct.toFixed(3) + '%', `${gb.exchange} ${gb.quote} ${gb.legs.join(' / ')}`);
        lastTriAlert = Date.now();
      }
    } else {
      gstats.textContent = `triangles: ${tri} | best_global: n/a`;
    }

    xbox.innerHTML = '';
    if (data.x_spreads && data.x_spreads.length) {
      data.x_spreads.forEach((s, i) => {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${i+1}</td><td>${s.pair}</td><td>${s.buy_ex} @ ${s.buy_price}</td><td>${s.sell_ex} @ ${s.sell_price}</td><td>${s.gross_pct.toFixed(4)}</td><td class="green">${s.net_pct.toFixed(4)}</td><td>${new Date().toLocaleTimeString()}</td>`;
        xbox.appendChild(tr);
        if (params.alerts && s.net_pct >= 0.5 && s.net_pct <= 5.0 && (Date.now()-lastXAlert>3000)) {
          notify('Cross-X 💹 ' + s.net_pct.toFixed(3) + '%', `${s.pair}: buy ${s.buy_ex} @ ${s.buy_price} → sell ${s.sell_ex} @ ${s.sell_price}`);
          lastXAlert = Date.now();
        }
      });
    } else {
      const tr = document.createElement('tr'); tr.innerHTML = '<td colspan="7" class="muted">No qualifying spreads.</td>';
      xbox.appendChild(tr);
    }

    exBox.innerHTML = '';
    for (const ex in data.per_exchange) {
      const exData = data.per_exchange[ex];
      const wrap = document.createElement('div'); wrap.className='ex';
      const hdr = document.createElement('div'); hdr.className='hdr mono muted';
      let bestTxt = 'best_now: n/a';
      if (exData.stats && exData.stats.best_now) {
        const b = exData.stats.best_now;
        bestTxt = `best_now: ${b.profit_pct.toFixed(6)}% | ${b.quote} | ${b.legs.join(' / ')} | `
                + b.route.map(r => `${r.from}->${r.to} [${r.symbol}]`).join(' -> ')
                + (b.exec_note ? `  (${b.exec_note})` : ``);
      }
      hdr.innerHTML = `<div><b>${ex}</b> <span class="chip">triangles: ${exData.stats ? exData.stats.triangles : 0}</span></div><div>${bestTxt}</div>`;
      wrap.appendChild(hdr);

      const tbl = document.createElement('table');
      const thead = document.createElement('thead'); thead.innerHTML = '<tr><th>#</th><th>Profit %</th><th>Quote</th><th>Legs</th><th>Route</th><th>Time</th></tr>';
      const tb = document.createElement('tbody');
      const opps = exData.opps || [];
      if (opps.length === 0) {
        const tr = document.createElement('tr'); tr.innerHTML = '<td colspan="6" class="muted">No opportunities above threshold.</td>';
        tb.appendChild(tr);
      } else {
        opps.forEach((o, i) => {
          const tr = document.createElement('tr');
          const route = o.route.map(r => `${r.from}->${r.to} [${r.symbol}]`).join(' -> ');
          const note = o.exec_note ? ` <span class="muted">(${o.exec_note})</span>` : "";
          tr.innerHTML = `<td>${i+1}</td><td class="green">${o.profit_pct.toFixed(6)}</td><td>${o.quote}</td><td>${o.legs.join(' / ')}</td><td>${route}${note}</td><td>${new Date().toLocaleTimeString()}</td>`;
          tb.appendChild(tr);
        });
      }
      tbl.appendChild(thead); tbl.appendChild(tb);
      wrap.appendChild(tbl);
      exBox.appendChild(wrap);
    }
  };
}

document.getElementById('apply').onclick = () => {
  params.ex = document.getElementById('ex').value;
  params.quotes = document.getElementById('quotes').value;
  params.threshold = parseFloat(document.getElementById('th').value);
  params.fee = parseFloat(document.getElementById('fee').value);
  params.interval = parseFloat(document.getElementById('intv').value);
  params.auto = document.getElementById('auto').checked;
  params.topn = parseInt(document.getElementById('topn').value, 10);
  params.autoq = document.getElementById('autoq').checked;
  params.qtop  = parseInt(document.getElementById('qtop').value, 10);
  params.xsp = document.getElementById('xsp').checked;
  params.xth = parseFloat(document.getElementById('xth').value);
  params.minliq = parseFloat(document.getElementById('minliq').value);
  params.slipbps = parseFloat(document.getElementById('slipbps').value);
  params.start = parseFloat(document.getElementById('start').value);
  params.paper = document.getElementById('paper').checked;
  params.alerts = document.getElementById('alerts').checked;
  params.ath_tri = parseFloat(document.getElementById('ath_tri').value);
  params.ath_x = parseFloat(document.getElementById('ath_x').value);
  connect();
};

connect();
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML

@app.websocket("/ws")
async def ws_progress(
    ws: WebSocket,
    ex: str = "binance",
    quotes: str = "USDT",
    threshold: float = 0.2,
    fee: float = 0.001,
    interval: float = 1.0,
    auto: int = 1,
    topn: int = 100,
    xsp: int = 1,
    xth: float = 0.25,
    autoq: int = 1,
    qtop: int = 6,
    minliq: float = 5000.0,
    slipbps: float = 30.0,
    start: float = 200.0,
    paper: int = 1,
):
    await ws.accept()
    try:
        exchanges = [x.strip().lower() for x in ex.split(",") if x.strip()]
        prep = await prepare(exchanges)

        # quotes per exchange (auto or manual)
        quotes_used: Set[str] = set()
        if int(autoq):
            tasks = [discover_top_quotes(e, top_k=int(qtop)) for e in exchanges]
            got = await asyncio.gather(*tasks, return_exceptions=True)
            for qlist in got:
                if isinstance(qlist, list):
                    quotes_used.update([q.upper() for q in qlist])
            if not quotes_used:
                quotes_used = {"USDT", "USDC", "FDUSD", "BTC", "ETH"}
            quotes_by_ex = {e: sorted(list(quotes_used)) for e in exchanges}
        else:
            qlist = [q.strip().upper() for q in quotes.split(",") if q.strip()]
            quotes_by_ex = {e: qlist for e in exchanges}
            quotes_used = set(qlist)

        while True:
            try:
                result = await asyncio.wait_for(
                    scan_once(
                        prep,
                        quotes_by_ex,
                        fee=fee,
                        threshold=threshold,
                        top_n=20,
                        auto_top=bool(auto),
                        auto_top_n=int(topn),
                        x_spreads=bool(xsp),
                        x_threshold=xth,
                        start_amount=start,
                        max_slip_bps=slipbps,
                        min_liq_usd=minliq,
                        do_paper=bool(paper),
                    ),
                    timeout=12.0,
                )
                result["quotes_used"] = sorted(list(quotes_used))
            except asyncio.TimeoutError:
                result = {"per_exchange": {},
                          "summary": {"total_triangles": 0, "global_best": None},
                          "x_spreads": [],
                          "quotes_used": sorted(list(quotes_used)),
                          "note": "scan timeout"}
            except Exception as e:
                result = {"per_exchange": {},
                          "summary": {"total_triangles": 0, "global_best": None},
                          "x_spreads": [],
                          "quotes_used": sorted(list(quotes_used)),
                          "note": f"scan error: {e}"}

            await ws.send_json(result)
            await asyncio.sleep(max(0.5, interval))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await ws.send_json({"error": str(e)})
        finally:
            await ws.close()
