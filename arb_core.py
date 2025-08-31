# arb_core.py — triangles + auto Top-N + auto quote discovery + cross-exchange spreads

from typing import Dict, List, Tuple, Iterable, Any, Set
import math
import asyncio
import httpx

from exchanges import EXCHANGE_MAP, ExchangeBase  # your existing exchanges.py


# ------------------------- Triangle building -------------------------

def build_triangles(
    sym_map: Dict[str, Tuple[str, str]],
    quotes: Iterable[str],
    base_whitelist: Dict[str, Set[str]] | None = None,
) -> List[Tuple[str, str, str, str]]:
    """
    Build triangles: (quote, leg1, leg2, leg3)
      leg1: A/quote     leg2: A/B or B/A       leg3: B/quote

    NOTE: we normalize ALL bases/quotes to UPPERCASE so we can compare reliably
    across exchange clients that return 'usdt'/'Usdt'/'USDT', etc.

    If base_whitelist is provided, it is applied ONLY if a NON-EMPTY set exists
    for that quote (to avoid filtering everything out).
    """
    # Normalize quotes once
    qlist = list({q.upper() for q in quotes})

    # Lookup maps in normalized (BASE, QUOTE)
    by_pair: Dict[Tuple[str, str], str] = {}
    by_asset_q: Dict[Tuple[str, str], str] = {}

    for sym, (base_raw, quote_raw) in sym_map.items():
        base = (base_raw or "").upper()
        quote = (quote_raw or "").upper()
        by_pair[(base, quote)] = sym          # keep sym as-is (exchange’s native)
        by_asset_q[(base, quote)] = sym

    # Collect available bases per quote
    assets_by_quote: Dict[str, List[str]] = {q: [] for q in qlist}
    for sym, (base_raw, quote_raw) in sym_map.items():
        base = (base_raw or "").upper()
        quote = (quote_raw or "").upper()
        if quote in assets_by_quote:
            # Apply whitelist only if non-empty *for this quote*
            if (
                base_whitelist
                and quote in base_whitelist
                and base_whitelist[quote]       # only if non-empty
                and base not in base_whitelist[quote]
            ):
                continue
            assets_by_quote[quote].append(base)

    # Build triangles (use normalized A/B and Q to fetch correct legs)
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



# ------------------------ 24h volume ranking (Top-N bases) -------------------------

async def top_bases_by_volume(ex_name: str, quotes: List[str], top_n: int) -> Dict[str, Set[str]]:
    """
    Return {quote -> set(BASE)} for the Top-N bases ranked by 24h quote volume.
    """
    quotes = [q.upper() for q in quotes]
    out: Dict[str, Dict[str, float]] = {q: {} for q in quotes}

    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
        # BINANCE
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

        # EXMO
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

        # OKX
        elif ex_name == "okx":
            try:
                r = await client.get("https://www.okx.com/api/v5/market/tickers", params={"instType": "SPOT"})
                r.raise_for_status()
                for it in r.json().get("data", []):
                    inst = it.get("instId", "")  # BTC-USDT
                    if "-" not in inst:
                        continue
                    base, q = inst.split("-", 1)
                    if q in quotes:
                        qvol = float(it.get("volCcy24h", 0) or 0)
                        out[q][base] = out[q].get(base, 0.0) + qvol
            except Exception:
                pass

        # KUCOIN
        elif ex_name == "kucoin":
            try:
                r = await client.get("https://api.kucoin.com/api/v1/market/allTickers")
                r.raise_for_status()
                for it in r.json().get("data", {}).get("ticker", []):
                    sym = it.get("symbol", "")  # BTC-USDT
                    if "-" not in sym:
                        continue
                    base, q = sym.split("-", 1)
                    if q not in quotes:
                        continue
                    qvol = float(it.get("volValue", 0) or 0)
                    if not qvol:
                        try:
                            qvol = float(it.get("last", 0) or 0) * float(it.get("vol", 0) or 0)
                        except Exception:
                            qvol = 0.0
                    out[q][base] = out[q].get(base, 0.0) + qvol
            except Exception:
                pass

        # MEXC
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

        # GATE
        elif ex_name == "gate":
            try:
                r = await client.get("https://api.gateio.ws/api/v4/spot/tickers")
                r.raise_for_status()
                for it in r.json():
                    pair = it.get("currency_pair", "")  # BTC_USDT
                    if "_" not in pair:
                        continue
                    base, q = pair.split("_", 1)
                    if q in quotes:
                        qvol = float(it.get("quote_volume", 0) or 0)
                        out[q][base] = out[q].get(base, 0.0) + qvol
            except Exception:
                pass

        # BYBIT
        elif ex_name == "bybit":
            try:
                r = await client.get("https://api.bybit.com/v5/market/tickers", params={"category": "spot"})
                r.raise_for_status()
                for it in r.json().get("result", {}).get("list", []):
                    sym = it.get("symbol", "")  # BTCUSDT
                    last = float(it.get("lastPrice", 0) or 0)
                    vol = float(it.get("volume24h", 0) or 0)  # base volume
                    turnover = float(it.get("turnover24h", 0) or 0)  # quote turnover
                    qvol = turnover if turnover else (last * vol)
                    for q in quotes:
                        if sym.endswith(q):
                            base = sym[:-len(q)]
                            out[q][base] = out[q].get(base, 0.0) + qvol
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
    """
    Returns the most liquid quote currencies on an exchange by summing 24h
    quote turnover per quoted currency (USDT, USDC, FDUSD, BTC, ETH, ...).
    """
    ex = ex_name.lower()
    totals: Dict[str, float] = {}

    def bump(q: str, v: float):
        if not q or v <= 0:
            return
        totals[q] = totals.get(q, 0.0) + v

    suffixes = ("USDT","USDC","FDUSD","BUSD","TUSD","DAI","USDP","BTC","ETH","BNB","SOL","XRP","TRX")

    async with await _httpx_client_short() as client:
        # Binance / MEXC: BASEQUOTE symbols + quoteVolume
        if ex in ("binance", "mexc"):
            url = "https://api.binance.com/api/v3/ticker/24hr" if ex == "binance" \
                  else "https://api.mexc.com/api/v3/ticker/24hr"
            try:
                r = await client.get(url)
                for it in r.json():
                    sym = it.get("symbol", "")
                    qvol = float(it.get("quoteVolume", 0) or 0)
                    for suf in suffixes:
                        if sym.endswith(suf):
                            bump(suf, qvol)
                            break
            except Exception:
                pass

        # OKX: instId "BTC-USDT", volCcy24h
        if ex == "okx":
            try:
                r = await client.get("https://www.okx.com/api/v5/market/tickers", params={"instType": "SPOT"})
                for it in r.json().get("data", []):
                    inst = it.get("instId", "")
                    if "-" in inst:
                        base, q = inst.split("-", 1)
                        qvol = float(it.get("volCcy24h", 0) or 0)
                        bump(q, qvol)
            except Exception:
                pass

        # KuCoin: symbol "BTC-USDT", volValue
        if ex == "kucoin":
            try:
                r = await client.get("https://api.kucoin.com/api/v1/market/allTickers")
                for it in r.json().get("data", {}).get("ticker", []):
                    sym = it.get("symbol", "")
                    if "-" in sym:
                        base, q = sym.split("-", 1)
                        qvol = float(it.get("volValue", 0) or 0)
                        if not qvol:
                            try:
                                qvol = float(it.get("last", 0) or 0) * float(it.get("vol", 0) or 0)
                            except Exception:
                                qvol = 0.0
                        bump(q, qvol)
            except Exception:
                pass

        # Gate: currency_pair "BTC_USDT", quote_volume
        if ex == "gate":
            try:
                r = await client.get("https://api.gateio.ws/api/v4/spot/tickers")
                for it in r.json():
                    pair = it.get("currency_pair", "")
                    if "_" in pair:
                        base, q = pair.split("_", 1)
                        qvol = float(it.get("quote_volume", 0) or 0)
                        bump(q, qvol)
            except Exception:
                pass

        # Bybit: symbol "BTCUSDT", turnover24h
        if ex == "bybit":
            try:
                r = await client.get("https://api.bybit.com/v5/market/tickers", params={"category": "spot"})
                for it in r.json().get("result", {}).get("list", []):
                    sym = it.get("symbol", "")
                    turnover = float(it.get("turnover24h", 0) or 0)
                    for suf in suffixes:
                        if sym.endswith(suf):
                            bump(suf, turnover)
                            break
            except Exception:
                pass

        # Exmo: BTC_USDT etc.; approximate with vol_curr
        if ex == "exmo":
            try:
                r = await client.get("https://api.exmo.com/v1.1/ticker")
                for pair, it in r.json().items():
                    if "_" in pair:
                        base, q = pair.split("_", 1)
                        qvol = float(it.get("vol_curr", 0) or 0)
                        bump(q, qvol)
            except Exception:
                pass

    ranked = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)
    return [q for q, _ in ranked[:max(1, top_k)]]


# --------------------------- Price math -----------------------------

def conv_best(
    amount: float,
    leg_symbol: str,
    sym_map: Dict[str, Tuple[str, str]],
    book: Dict[str, Dict[str, float]],
    from_asset: str,
    to_asset: str,
    fee: float,
) -> float:
    if leg_symbol not in book:
        return 0.0
    base, quote = sym_map[leg_symbol]
    bid = book[leg_symbol]["bid"]
    ask = book[leg_symbol]["ask"]
    if from_asset == quote and to_asset == base:  # BUY
        return (amount / ask) * (1.0 - fee) if ask > 0 else 0.0
    if from_asset == base and to_asset == quote:  # SELL
        return (amount * bid) * (1.0 - fee) if bid > 0 else 0.0
    return 0.0


def eval_triangle_best(
    q: str,
    l1: str,
    l2: str,
    l3: str,
    sym_map: Dict[str, Tuple[str, str]],
    book: Dict[str, Dict[str, float]],
    fee: float,
    start_q_amount: float = 100.0,
):
    base_a, _ = sym_map[l1]
    base_b, _ = sym_map[l3]

    # Direction 1: q -> A -> B -> q
    amt = start_q_amount
    path1 = []
    a_amt = conv_best(amt, l1, sym_map, book, q, base_a, fee)
    if a_amt > 0:
        path1.append((q, base_a, l1))
        b_amt = conv_best(a_amt, l2, sym_map, book, base_a, base_b, fee)
        if b_amt > 0:
            path1.append((base_a, base_b, l2))
            q_amt = conv_best(b_amt, l3, sym_map, book, base_b, q, fee)
            if q_amt > 0:
                path1.append((base_b, q, l3))
                profit1 = (q_amt - start_q_amount) / start_q_amount * 100.0
            else:
                profit1 = -math.inf
        else:
            profit1 = -math.inf
    else:
        profit1 = -math.inf

    # Direction 2: q -> B -> A -> q
    amt = start_q_amount
    path2 = []
    b_amt = conv_best(amt, l3, sym_map, book, q, base_b, fee)
    if b_amt > 0:
        path2.append((q, base_b, l3))
        a_amt = conv_best(b_amt, l2, sym_map, book, base_b, base_a, fee)
        if a_amt > 0:
            path2.append((base_b, base_a, l2))
            q_amt = conv_best(a_amt, l1, sym_map, book, base_a, q, fee)
            if q_amt > 0:
                path2.append((base_a, q, l1))
                profit2 = (q_amt - start_q_amount) / start_q_amount * 100.0
            else:
                profit2 = -math.inf
        else:
            profit2 = -math.inf
    else:
        profit2 = -math.inf

    if profit1 >= profit2:
        return profit1, path1
    return profit2, path2


# ------------------------- Cross-exchange spreads -------------------

def normalize_pair(base: str, quote: str) -> str:
    return f"{base.upper()}/{quote.upper()}"


def book_to_norm_map(
    sym_map: Dict[str, Tuple[str, str]],
    book: Dict[str, Dict[str, float]],
) -> Dict[str, Dict[str, float]]:
    out = {}
    for sym, (base, quote) in sym_map.items():
        ba = book.get(sym)
        if ba:
            out[normalize_pair(base, quote)] = ba
    return out


def cross_exchange_spreads(
    prep: Dict[str, Any],
    books: Dict[str, Dict[str, Dict[str, float]]],
    fee_per_trade: float,
    threshold_pct: float = 0.2,
) -> List[Dict[str, Any]]:
    pair_map: Dict[str, Dict[str, Dict[str, float]]] = {}
    for ex, obj in prep.items():
        sym_map = obj["sym_map"]
        bnorm = book_to_norm_map(sym_map, books.get(ex, {}))
        for pair, ba in bnorm.items():
            pair_map.setdefault(pair, {})[ex] = ba

    results = []
    for pair, ex_map in pair_map.items():
        if len(ex_map) < 2:
            continue
        best_ask_ex, best_ask = None, 1e99
        best_bid_ex, best_bid = None, -1e99
        for ex, ba in ex_map.items():
            ask = ba.get("ask")
            bid = ba.get("bid")
            if ask and ask < best_ask:
                best_ask, best_ask_ex = ask, ex
            if bid and bid > best_bid:
                best_bid, best_bid_ex = bid, ex
        if best_ask_ex is None or best_bid_ex is None or best_ask_ex == best_bid_ex:
            continue
        gross = (best_bid - best_ask) / best_ask * 100.0
        net = gross - (fee_per_trade * 100.0 * 2.0)  # two taker trades
        if net >= threshold_pct:
            results.append(
                {
                    "pair": pair,
                    "buy_ex": best_ask_ex,
                    "buy_price": best_ask,
                    "sell_ex": best_bid_ex,
                    "sell_price": best_bid,
                    "gross_pct": round(gross, 6),
                    "net_pct": round(net, 6),
                }
            )
    results.sort(key=lambda x: x["net_pct"], reverse=True)
    return results


# ---------------------------- Pipeline --------------------------------

async def prepare(exchanges: List[str]):
    out = {}
    for ex in exchanges:
        ex = ex.lower()
        if ex not in EXCHANGE_MAP:
            continue
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
):
    # Optional Top-N base filters per exchange
    base_whitelist_by_ex: Dict[str, Dict[str, Set[str]]] = {}
    if auto_top:
        tasks = [
            top_bases_by_volume(ex, quotes_by_ex.get(ex, ["USDT"]), auto_top_n)
            for ex in prep.keys()
        ]
        tops = await asyncio.gather(*tasks, return_exceptions=True)
        for (ex, topset) in zip(prep.keys(), tops):
            if isinstance(topset, dict):
                # remove quotes that produced an empty set (avoid blocking)
                bw = {q: bases for q, bases in topset.items() if bases}
                base_whitelist_by_ex[ex] = bw
            else:
                base_whitelist_by_ex[ex] = {}

    # Build triangles and request books concurrently
    book_tasks = []
    for ex, obj in prep.items():
        sym_map = obj["sym_map"]
        quotes = quotes_by_ex.get(ex, ["USDT"])
        whitelist = base_whitelist_by_ex.get(ex) if auto_top else None
        triangles = build_triangles(sym_map, quotes, base_whitelist=whitelist)
        obj["triangles"] = triangles
        book_tasks.append(obj["client"].get_book())

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
        best = (-1e9, None, None)

        for (q, l1, l2, l3) in triangles:
            if l1 not in book or l2 not in book or l3 not in book:
                continue
            profit, path = eval_triangle_best(
                q, l1, l2, l3, sym_map, book, fee=fee, start_q_amount=start_amount
            )
            if profit > best[0]:
                best = (profit, (q, l1, l2, l3), path)
            if profit >= threshold:
                opps.append(
                    {
                        "profit_pct": round(profit, 6),
                        "quote": q,
                        "legs": [l1, l2, l3],
                        "route": [{"from": f, "to": t, "symbol": s} for (f, t, s) in path],
                    }
                )

        opps.sort(key=lambda x: x["profit_pct"], reverse=True)

        best_json = None
        if best[1] is not None:
            q, l1, l2, l3 = best[1]
            best_json = {
                "profit_pct": round(best[0], 6),
                "quote": q,
                "legs": [l1, l2, l3],
                "route": [{"from": f, "to": t, "symbol": s} for (f, t, s) in best[2]],
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
            "route": [{"from": f, "to": t, "symbol": s} for (f, t, s) in path],
        }

    xres: List[Dict[str, Any]] = []
    if x_spreads:
        xres = cross_exchange_spreads(prep, books, fee_per_trade=fee, threshold_pct=x_threshold)

    return {
        "per_exchange": results,
        "summary": {"total_triangles": total_tri, "global_best": g},
        "x_spreads": xres,
    }
