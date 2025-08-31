# arb_core.py — triangles + cross-exchange + auto top-N + auto quotes
#                 + liquidity filter + paper trading + CSV logging

from __future__ import annotations
from typing import Dict, List, Tuple, Iterable, Any, Set
import math
import csv
import os
import asyncio
import httpx
from datetime import datetime

from exchanges import EXCHANGE_MAP, ExchangeBase  # your existing exchanges.py


# ------------------------- Triangle builder (normalized) -------------------------

def build_triangles(
    sym_map: Dict[str, Tuple[str, str]],
    quotes: Iterable[str],
    base_whitelist: Dict[str, Set[str]] | None = None,
) -> List[Tuple[str, str, str, str]]:
    """
    Build triangles: (quote, leg1, leg2, leg3)
      leg1: A/quote     leg2: A/B or B/A       leg3: B/quote
    Normalize bases/quotes to UPPERCASE and apply whitelist only if non-empty.
    """
    qlist = list({q.upper() for q in quotes})

    by_pair: Dict[Tuple[str, str], str] = {}
    by_asset_q: Dict[Tuple[str, str], str] = {}

    for sym, (base_raw, quote_raw) in sym_map.items():
        base = (base_raw or "").upper()
        quote = (quote_raw or "").upper()
        by_pair[(base, quote)] = sym
        by_asset_q[(base, quote)] = sym

    assets_by_quote: Dict[str, List[str]] = {q: [] for q in qlist}
    for sym, (base_raw, quote_raw) in sym_map.items():
        base = (base_raw or "").upper()
        quote = (quote_raw or "").upper()
        if quote in assets_by_quote:
            if (
                base_whitelist
                and quote in base_whitelist
                and base_whitelist[quote]
                and base not in base_whitelist[quote]
            ):
                continue
            assets_by_quote[quote].append(base)

    triangles: List[Tuple[str, str, str, str]] = []
    for q in qlist:
        assets = list(set(assets_by_quote[q]))
        for i, A in enumerate(assets):
            ausym = by_asset_q.get((A, q))
            if not ausym:
                continue
            for j, B in enumerate(assets):
                if i == j:
                    continue
                busym = by_asset_q.get((B, q))
                if not busym:
                    continue
                cross = by_pair.get((A, B)) or by_pair.get((B, A))
                if not cross:
                    continue
                triangles.append((q, ausym, cross, busym))
    return triangles


# ------------------------ 24h volume ranking (Top-N bases) ------------------------

async def top_bases_by_volume(ex_name: str, quotes: List[str], top_n: int) -> Dict[str, Set[str]]:
    quotes = [q.upper() for q in quotes]
    out: Dict[str, Dict[str, float]] = {q: {} for q in quotes}

    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
        if ex_name == "binance":
            try:
                r = await client.get("https://api.binance.com/api/v3/ticker/24hr")
                r.raise_for_status()
                for it in r.json():
                    sym = it.get("symbol", "")
                    qvol = float(it.get("quoteVolume", 0) or 0)
                    for q in quotes:
                        if sym.endswith(q):
                            base = sym[:-len(q)]
                            out[q][base] = out[q].get(base, 0.0) + qvol
            except Exception:
                pass

        elif ex_name == "exmo":
            try:
                r = await client.get("https://api.exmo.com/v1.1/ticker")
                r.raise_for_status()
                for pair, it in r.json().items():
                    if "_" not in pair:
                        continue
                    base, q = pair.split("_", 1)
                    if q in quotes:
                        qvol = float(it.get("vol_curr", 0) or 0)
                        out[q][base] = out[q].get(base, 0.0) + qvol
            except Exception:
                pass

        elif ex_name == "okx":
            try:
                r = await client.get("https://www.okx.com/api/v5/market/tickers", params={"instType": "SPOT"})
                r.raise_for_status()
                for it in r.json().get("data", []):
                    inst = it.get("instId", "")
                    if "-" not in inst: continue
                    base, q = inst.split("-", 1)
                    if q in quotes:
                        qvol = float(it.get("volCcy24h", 0) or 0)
                        out[q][base] = out[q].get(base, 0.0) + qvol
            except Exception:
                pass

        elif ex_name == "kucoin":
            try:
                r = await client.get("https://api.kucoin.com/api/v1/market/allTickers")
                r.raise_for_status()
                for it in r.json().get("data", {}).get("ticker", []):
                    sym = it.get("symbol", "")
                    if "-" not in sym: continue
                    base, q = sym.split("-", 1)
                    if q not in quotes: continue
                    qvol = float(it.get("volValue", 0) or 0)
                    if not qvol:
                        try:
                            qvol = float(it.get("last", 0) or 0) * float(it.get("vol", 0) or 0)
                        except Exception:
                            qvol = 0.0
                    out[q][base] = out[q].get(base, 0.0) + qvol
            except Exception:
                pass

        elif ex_name == "mexc":
            try:
                r = await client.get("https://api.mexc.com/api/v3/ticker/24hr")
                r.raise_for_status()
                for it in r.json():
                    sym = it.get("symbol", "")
                    qvol = float(it.get("quoteVolume", 0) or 0)
                    for q in quotes:
                        if sym.endswith(q):
                            base = sym[:-len(q)]
                            out[q][base] = out[q].get(base, 0.0) + qvol
            except Exception:
                pass

        elif ex_name == "gate":
            try:
                r = await client.get("https://api.gateio.ws/api/v4/spot/tickers")
                r.raise_for_status()
                for it in r.json():
                    pair = it.get("currency_pair", "")
                    if "_" not in pair: continue
                    base, q = pair.split("_", 1)
                    if q in quotes:
                        qvol = float(it.get("quote_volume", 0) or 0)
                        out[q][base] = out[q].get(base, 0.0) + qvol
            except Exception:
                pass

        elif ex_name == "bybit":
            try:
                r = await client.get("https://api.bybit.com/v5/market/tickers", params={"category": "spot"})
                r.raise_for_status()
                for it in r.json().get("result", {}).get("list", []):
                    sym = it.get("symbol", "")
                    last = float(it.get("lastPrice", 0) or 0)
                    vol = float(it.get("volume24h", 0) or 0)
                    trn = float(it.get("turnover24h", 0) or 0) or (last * vol)
                    for q in quotes:
                        if sym.endswith(q):
                            base = sym[:-len(q)]
                            out[q][base] = out[q].get(base, 0.0) + trn
            except Exception:
                pass

    res: Dict[str, Set[str]] = {}
    for q, base_map in out.items():
        top = sorted(base_map.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
        res[q] = {b for b, _ in top}
    return res


# ------------------------ Auto quote discovery (Top-k quotes) ------------------------

async def _httpx_client_short():
    return httpx.AsyncClient(timeout=httpx.Timeout(6.0, connect=3.0))

async def discover_top_quotes(ex_name: str, top_k: int = 6) -> List[str]:
    ex = ex_name.lower()
    totals: Dict[str, float] = {}
    def bump(q: str, v: float):
        if not q or v <= 0: return
        totals[q] = totals.get(q, 0.0) + v

    suffixes = ("USDT","USDC","FDUSD","BUSD","TUSD","DAI","USDP","BTC","ETH","BNB","SOL","XRP","TRX")

    async with await _httpx_client_short() as client:
        if ex in ("binance", "mexc"):
            url = "https://api.binance.com/api/v3/ticker/24hr" if ex == "binance" else "https://api.mexc.com/api/v3/ticker/24hr"
            try:
                r = await client.get(url)
                for it in r.json():
                    sym = it.get("symbol", "")
                    qvol = float(it.get("quoteVolume", 0) or 0)
                    for suf in suffixes:
                        if sym.endswith(suf):
                            bump(suf, qvol); break
            except Exception: pass

        if ex == "okx":
            try:
                r = await client.get("https://www.okx.com/api/v5/market/tickers", params={"instType": "SPOT"})
                for it in r.json().get("data", []):
                    inst = it.get("instId", "")
                    if "-" in inst:
                        base, q = inst.split("-", 1)
                        bump(q, float(it.get("volCcy24h", 0) or 0))
            except Exception: pass

        if ex == "kucoin":
            try:
                r = await client.get("https://api.kucoin.com/api/v1/market/allTickers")
                for it in r.json().get("data", {}).get("ticker", []):
                    sym = it.get("symbol", "")
                    if "-" in sym:
                        base, q = sym.split("-", 1)
                        qvol = float(it.get("volValue", 0) or 0)
                        if not qvol:
                            try: qvol = float(it.get("last", 0) or 0) * float(it.get("vol", 0) or 0)
                            except Exception: qvol = 0.0
                        bump(q, qvol)
            except Exception: pass

        if ex == "gate":
            try:
                r = await client.get("https://api.gateio.ws/api/v4/spot/tickers")
                for it in r.json():
                    pair = it.get("currency_pair", "")
                    if "_" in pair:
                        base, q = pair.split("_", 1)
                        bump(q, float(it.get("quote_volume", 0) or 0))
            except Exception: pass

        if ex == "bybit":
            try:
                r = await client.get("https://api.bybit.com/v5/market/tickers", params={"category": "spot"})
                for it in r.json().get("result", {}).get("list", []):
                    sym = it.get("symbol", "")
                    turnover = float(it.get("turnover24h", 0) or 0)
                    for suf in suffixes:
                        if sym.endswith(suf):
                            bump(suf, turnover); break
            except Exception: pass

        if ex == "exmo":
            try:
                r = await client.get("https://api.exmo.com/v1.1/ticker")
                for pair, it in r.json().items():
                    if "_" in pair:
                        base, q = pair.split("_", 1)
                        bump(q, float(it.get("vol_curr", 0) or 0))
            except Exception: pass

    ranked = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)
    return [q for q, _ in ranked[:max(1, top_k)]]


# --------------------------- Basic price math (fallback) ---------------------------

def conv_best(amount: float, leg_symbol: str, sym_map, book, from_asset, to_asset, fee: float) -> float:
    if leg_symbol not in book:
        return 0.0
    base, quote = sym_map[leg_symbol]
    bid = float(book[leg_symbol].get("bid", 0))
    ask = float(book[leg_symbol].get("ask", 0))
    if from_asset == quote and to_asset == base:  # BUY
        return (amount / ask) * (1.0 - fee) if ask > 0 else 0.0
    if from_asset == base and to_asset == quote:  # SELL
        return (amount * bid) * (1.0 - fee) if bid > 0 else 0.0
    return 0.0


def eval_triangle_best(q, l1, l2, l3, sym_map, book, fee: float, start_q_amount: float = 100.0):
    base_a, _ = sym_map[l1]; base_b, _ = sym_map[l3]
    amt = start_q_amount
    a_amt = conv_best(amt, l1, sym_map, book, q, base_a, fee)
    if a_amt <= 0: return -math.inf, []
    b_amt = conv_best(a_amt, l2, sym_map, book, base_a, base_b, fee)
    if b_amt <= 0: return -math.inf, []
    q_amt = conv_best(b_amt, l3, sym_map, book, base_b, q, fee)
    if q_amt <= 0: return -math.inf, []
    profit = (q_amt - start_q_amount) / start_q_amount * 100.0
    route = [{"from": q, "to": base_a, "symbol": l1},
             {"from": base_a, "to": base_b, "symbol": l2},
             {"from": base_b, "to": q, "symbol": l3}]
    return profit, route


# ---------------------- Liquidity / Slippage paper simulator ----------------------

def _bps(x: float) -> float: return x / 10000.0

def _book_levels(book_side: list[tuple[float, float]], reverse: bool = False):
    lvls = [(float(p), float(q)) for p, q in book_side if float(p) > 0 and float(q) > 0]
    return sorted(lvls, key=lambda x: x[0], reverse=reverse)

def _walk_book_buy(amount_quote: float, asks: list[tuple[float, float]]) -> tuple[float, float]:
    base_filled, cost, remain = 0.0, 0.0, amount_quote
    for price, qty in asks:
        lot = price * qty
        if lot <= remain:
            base_filled += qty; cost += lot; remain -= lot
        else:
            partial = remain / price
            base_filled += partial; cost += remain; remain = 0.0; break
    vwap = (cost / base_filled) if base_filled > 0 else 0.0
    return base_filled, vwap

def _walk_book_sell(amount_base: float, bids: list[tuple[float, float]]) -> tuple[float, float]:
    left, quote_got = amount_base, 0.0
    for price, qty in bids:
        use = min(qty, left)
        quote_got += use * price
        left -= use
        if left <= 1e-18: break
    vwap = (quote_got / (amount_base - left)) if amount_base > left else 0.0
    return quote_got, vwap

def _slippage_ok(mid: float, vwap: float, max_bps: float, side: str) -> bool:
    tol = _bps(max_bps)
    if mid <= 0 or vwap <= 0: return False
    if side == "buy":  return vwap <= mid * (1.0 + tol + 1e-12)
    else:              return vwap >= mid * (1.0 - tol - 1e-12)

def simulate_leg(side: str, amount: float, symbol: str, sym_map, book, fee: float, max_slip_bps: float) -> tuple[float, float, str]:
    if symbol not in book: return 0.0, 0.0, "no_book"
    base, quote = sym_map[symbol]
    b = book[symbol]
    bid, ask = float(b.get("bid", 0)), float(b.get("ask", 0))
    if bid <= 0 or ask <= 0: return 0.0, 0.0, "bad_top"
    mid = (bid + ask) / 2

    bids = _book_levels(b.get("bids", []), True) if b.get("bids") else ([(bid, 1e9)] if bid>0 else [])
    asks = _book_levels(b.get("asks", []), False) if b.get("asks") else ([(ask, 1e9)] if ask>0 else [])

    if side == "buy":  # quote -> base
        base_filled, vwap = _walk_book_buy(amount, asks)
        if base_filled <= 0: return 0.0, 0.0, "no_fill"
        if not _slippage_ok(mid, vwap, max_slip_bps, "buy"): return 0.0, vwap, "slippage"
        return base_filled * (1 - fee), vwap, "ok"
    else:              # sell base -> quote
        quote_got, vwap = _walk_book_sell(amount, bids)
        if quote_got <= 0: return 0.0, 0.0, "no_fill"
        if not _slippage_ok(mid, vwap, max_slip_bps, "sell"): return 0.0, vwap, "slippage"
        return quote_got * (1 - fee), vwap, "ok"

def simulate_triangle(q, l1, l2, l3, sym_map, book, fee: float, start_q_amount: float, max_slip_bps: float, min_liq_usd: float):
    # quick liq gate on each leg (approx: use top-of-book levels if no L2)
    for lg in (l1, l2, l3):
        b = book.get(lg)
        if not b: return -1e9, [], "no_book"
        bid, ask = float(b.get("bid", 0)), float(b.get("ask", 0))
        if bid <= 0 or ask <= 0: return -1e9, [], "bad_top"
        mid = (bid + ask)/2
        bids = _book_levels(b.get("bids", []), True) if b.get("bids") else [(bid, 1e9)]
        asks = _book_levels(b.get("asks", []), False) if b.get("asks") else [(ask, 1e9)]
        # naive depth window within slippage bounds
        tol = _bps(max_slip_bps)
        buy_limit = mid*(1+tol); sell_limit = mid*(1-tol)
        buy_depth = sum(p*q for p,q in asks if p<=buy_limit)
        sell_depth = sum(p*q for p,q in bids if p>=sell_limit)
        if max(buy_depth, sell_depth) < min_liq_usd:
            return -1e9, [], "liquidity"

    base_a, _ = sym_map[l1]
    base_b, _ = sym_map[l3]

    amt_q = start_q_amount
    a_base, _, n1 = simulate_leg("buy", amt_q, l1, sym_map, book, fee, max_slip_bps)
    if a_base <= 0: return -1e9, [], f"leg1:{n1}"

    # l2 direction depends on mapping
    l2_base, l2_quote = sym_map[l2]
    if l2_quote == base_b:  # A -> B by SELL A for B using pair A/B (quote=B)
        b_base, _, n2 = simulate_leg("sell", a_base, l2, sym_map, book, fee, max_slip_bps)
    else:                   # A -> B by BUY B using pair B/A (quote=A), spend 'a_base'
        # To buy B using quote A we treat amount in quote (A). We approximate one-tick buy.
        # Better: transform by inverse symbol, but for simplicity reuse simulate_leg with "buy" on l2.
        b_base, _, n2 = simulate_leg("buy", a_base, l2, sym_map, book, fee, max_slip_bps)
    if b_base <= 0: return -1e9, [], f"leg2:{n2}"

    q_out, _, n3 = simulate_leg("sell", b_base, l3, sym_map, book, fee, max_slip_bps)
    if q_out <= 0: return -1e9, [], f"leg3:{n3}"

    profit = (q_out - start_q_amount) / start_q_amount * 100.0
    route = [
        {"from": q, "to": base_a, "symbol": l1},
        {"from": base_a, "to": base_b, "symbol": l2},
        {"from": base_b, "to": q, "symbol": l3},
    ]
    return profit, route, "ok"


# ------------------------- Cross-exchange spreads (top-of-book) -------------------

def normalize_pair(base: str, quote: str) -> str:
    return f"{base.upper()}/{quote.upper()}"

def book_to_norm_map(sym_map: Dict[str, Tuple[str, str]], book: Dict[str, Dict[str, float]]) -> Dict[str, Dict[str, float]]:
    out = {}
    for sym, (base, quote) in sym_map.items():
        ba = book.get(sym)
        if ba: out[normalize_pair(base, quote)] = ba
    return out

def cross_exchange_spreads(prep: Dict[str, Any], books: Dict[str, Dict[str, Dict[str, float]]], fee_per_trade: float, threshold_pct: float = 0.2) -> List[Dict[str, Any]]:
    pair_map: Dict[str, Dict[str, Dict[str, float]]] = {}
    for ex, obj in prep.items():
        sym_map = obj["sym_map"]
        bnorm = book_to_norm_map(sym_map, books.get(ex, {}))
        for pair, ba in bnorm.items():
            pair_map.setdefault(pair, {})[ex] = ba

    results = []
    for pair, ex_map in pair_map.items():
        if len(ex_map) < 2: continue
        best_ask_ex, best_ask = None, 1e99
        best_bid_ex, best_bid = None, -1e99
        for ex, ba in ex_map.items():
            ask = float(ba.get("ask", 0)); bid = float(ba.get("bid", 0))
            if ask and ask < best_ask: best_ask, best_ask_ex = ask, ex
            if bid and bid > best_bid: best_bid, best_bid_ex = bid, ex
        if best_ask_ex is None or best_bid_ex is None or best_ask_ex == best_bid_ex:
            continue
        gross = (best_bid - best_ask) / best_ask * 100.0
        net = gross - (fee_per_trade * 100.0 * 2.0)
        if net >= threshold_pct:
            results.append({"pair": pair,"buy_ex": best_ask_ex,"buy_price": best_ask,"sell_ex": best_bid_ex,"sell_price": best_bid,"gross_pct": round(gross, 6),"net_pct": round(net, 6)})
    results.sort(key=lambda x: x["net_pct"], reverse=True)
    return results


# ------------------------------ CSV logging --------------------------------------

def _log_csv(row: Dict[str, Any], fname: str = "paper_trades.csv"):
    header = list(row.keys())
    exists = os.path.exists(fname)
    with open(fname, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not exists: w.writeheader()
        w.writerow(row)


# ------------------------------ Pipeline -----------------------------------------

async def prepare(exchanges: List[str]):
    out = {}
    for ex in exchanges:
        ex = ex.lower()
        if ex not in EXCHANGE_MAP: continue
        client = EXCHANGE_MAP[ex]()
        sym_map = await client.get_symbols()
        out[ex] = {"client": client, "sym_map": sym_map}
    return out


async def scan_once(
    prep: Dict[str, Any],
    quotes_by_ex: Dict[str, List[str]],
    fee: float = 0.001,
    threshold: float = 0.2,
    top_n: int = 20,
    auto_top: bool = False,
    auto_top_n: int = 50,
    x_spreads: bool = False,
    x_threshold: float = 0.2,
    start_amount: float = 100.0,
    max_slip_bps: float = 30.0,
    min_liq_usd: float = 5000.0,
    do_paper: bool = True,
):
    # Optional Top-N base filters per exchange
    base_whitelist_by_ex: Dict[str, Dict[str, Set[str]]] = {}
    if auto_top:
        tasks = [top_bases_by_volume(ex, quotes_by_ex.get(ex, ["USDT"]), auto_top_n) for ex in prep.keys()]
        tops = await asyncio.gather(*tasks, return_exceptions=True)
        for (ex, topset) in zip(prep.keys(), tops):
            if isinstance(topset, dict):
                bw = {q: bases for q, bases in topset.items() if bases}
                base_whitelist_by_ex[ex] = bw
            else:
                base_whitelist_by_ex[ex] = {}

    # Build triangles + fetch books
    book_tasks = []
    for ex, obj in prep.items():
        sym_map = obj["sym_map"]
        quotes = quotes_by_ex.get(ex, ["USDT"])
        whitelist = base_whitelist_by_ex.get(ex) if auto_top else None
        triangles = build_triangles(sym_map, quotes, base_whitelist=whitelist)
        obj["triangles"] = triangles
        book_tasks.append(obj["client"].get_book())  # should return bid/ask and optional bids/asks arrays

    books_list = await asyncio.gather(*book_tasks, return_exceptions=True)
    books: Dict[str, Dict[str, Dict[str, float]]] = {}

    results: Dict[str, Any] = {}
    global_best = (-1e9, None, None, None)
    total_tri = 0

    for (ex, obj), book in zip(prep.items(), books_list):
        if isinstance(book, Exception):
            results[ex] = {"error": str(book)}
            continue
        books[ex] = book
        sym_map = obj["sym_map"]
        triangles = obj["triangles"]
        total_tri += len(triangles)

        opps: List[Dict[str, Any]] = []
        best = (-1e9, None, None, "n/a")

        for (q, l1, l2, l3) in triangles:
            if l1 not in book or l2 not in book or l3 not in book:
                continue
            if do_paper:
                profit, path, note = simulate_triangle(
                    q, l1, l2, l3, sym_map, book, fee=fee,
                    start_q_amount=start_amount, max_slip_bps=max_slip_bps,
                    min_liq_usd=min_liq_usd,
                )
            else:
                profit, path = eval_triangle_best(q, l1, l2, l3, sym_map, book, fee=fee, start_q_amount=start_amount)
                note = "calc"
            if profit > best[0]:
                best = (profit, (q, l1, l2, l3), path, note)
            if profit >= threshold:
                opps.append({
                    "profit_pct": round(profit, 6),
                    "quote": q,
                    "legs": [l1, l2, l3],
                    "route": [{"from": r["from"], "to": r["to"], "symbol": r["symbol"]} for r in path],
                    "exec_note": note,
                })
                # CSV autolog only realistic paper trades (net 0.5–5% & ok)
                if do_paper and note == "ok" and 0.5 <= profit <= 5.0:
                    _log_csv({
                        "ts": datetime.utcnow().isoformat(),
                        "exchange": ex,
                        "quote": q,
                        "legs": f"{l1}/{l2}/{l3}",
                        "profit_pct": round(profit, 6),
                        "start_amount": start_amount,
                        "fee": fee,
                        "max_slip_bps": max_slip_bps,
                        "min_liq_usd": min_liq_usd,
                    })

        opps.sort(key=lambda x: x["profit_pct"], reverse=True)

        best_json = None
        if best[1] is not None:
            q, l1, l2, l3 = best[1]
            best_json = {
                "profit_pct": round(best[0], 6),
                "quote": q,
                "legs": [l1, l2, l3],
                "route": [{"from": r["from"], "to": r["to"], "symbol": r["symbol"]} for r in (best[2] or [])],
                "exec_note": best[3],
            }

        results[ex] = {
            "opps": opps[:top_n],
            "stats": {"triangles": len(triangles), "best_now": best_json},
        }

        if best[1] is not None and best[0] > global_best[0]:
            global_best = (best[0], ex, best[1], best[2])

    g = None
    if global_best[2] is not None:
        p, exname, legs, path = global_best
        q, l1, l2, l3 = legs
        g = {
            "profit_pct": round(p, 6),
            "exchange": exname,
            "quote": q,
            "legs": [l1, l2, l3],
            "route": [{"from": r["from"], "to": r["to"], "symbol": r["symbol"]} for r in (path or [])],
        }

    xres: List[Dict[str, Any]] = []
    if x_spreads:
        xres = cross_exchange_spreads(prep, books, fee_per_trade=fee, threshold_pct=x_threshold)

    return {
        "per_exchange": results,
        "summary": {"total_triangles": total_tri, "global_best": g},
        "x_spreads": xres,
    }
