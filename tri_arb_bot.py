#!/usr/bin/env python3
"""
tri_arb_bot.py  (fixed)
Simple triangular-arbitrage opportunity finder for Binance SPOT.
- Polls best bid/ask for all symbols every ~1s (REST).
- Precomputes all triangles of the form: USDT -> A -> B -> USDT (and reverse).
- Prints top opportunities over threshold (default 0.2%).

This version has a simplified parse_symbols() so USDT pairs are not over-filtered.
No API keys needed.
"""

import asyncio
import argparse
import math
import time
from typing import Dict, List, Tuple

import httpx

BINANCE_REST = "https://api.binance.com"


# ----- Helpers -----

def parse_symbols(exchange_info: dict) -> Dict[str, Tuple[str, str]]:
    """
    Return {symbol: (base, quote)} for tradable symbols.
    Keep it simple: include all symbols with status TRADING.
    (Do NOT filter by permissions or try to exclude ETF/leveraged suffixes here.)
    """
    res: Dict[str, Tuple[str, str]] = {}
    for s in exchange_info.get("symbols", []):
        status = s.get("status")
        base = s.get("baseAsset")
        quote = s.get("quoteAsset")
        symbol = s.get("symbol")
        if status != "TRADING":
            continue
        if not base or not quote or not symbol:
            continue
        res[symbol] = (base, quote)
    return res


def build_usdt_triangles(sym_map: Dict[str, Tuple[str, str]]) -> List[Tuple[str, str, str]]:
    """
    Build triangles of the form:
      USDT->A via AUSDT,
      A->B via AB or BA,
      B->USDT via BUSDT

    Returns list of tuples with concrete symbol names for the 3 legs.
    """
    by_pair: Dict[Tuple[str, str], str] = {}
    by_asset_usdt: Dict[str, str] = {}

    for sym, (base, quote) in sym_map.items():
        by_pair[(base, quote)] = sym
        if quote == "USDT":
            by_asset_usdt[base] = sym  # A/USDT

    assets = list(by_asset_usdt.keys())
    triangles: List[Tuple[str, str, str]] = []

    for i in range(len(assets)):
        for j in range(len(assets)):
            if i == j:
                continue
            A = assets[i]
            B = assets[j]
            ausdt = by_asset_usdt[A]
            busdt = by_asset_usdt[B]

            cross = by_pair.get((A, B)) or by_pair.get((B, A))
            if cross:
                triangles.append((ausdt, cross, busdt))

    return triangles


# ----- Pricing math -----

def conv_amount(amount: float, leg_symbol: str, sym_map: Dict[str, Tuple[str, str]],
                book: Dict[str, Dict[str, float]], from_asset: str, to_asset: str,
                fee: float) -> float:
    """
    Convert `amount` of `from_asset` -> `to_asset` using one market leg (leg_symbol).
    Uses top-of-book: if BUY -> use ask; if SELL -> use bid. Applies taker fee.
    Returns resulting amount of `to_asset`.
    """
    if leg_symbol not in book:
        return 0.0
    base, quote = sym_map[leg_symbol]
    bid = book[leg_symbol]["bid"]
    ask = book[leg_symbol]["ask"]

    # BUY base with quote
    if from_asset == quote and to_asset == base:
        return (amount / ask) * (1.0 - fee)
    # SELL base for quote
    if from_asset == base and to_asset == quote:
        return (amount * bid) * (1.0 - fee)
    return 0.0


def eval_triangle(ausdt: str, cross: str, busdt: str,
                  sym_map: Dict[str, Tuple[str, str]],
                  book: Dict[str, Dict[str, float]],
                  fee: float, start_usdt: float = 100.0) -> Tuple[float, List[Tuple[str, str, str]]]:
    """
    Evaluate both directions of the triangle and return best result.
    """
    base_a, _ = sym_map[ausdt]     # A/USDT
    base_b, _ = sym_map[busdt]     # B/USDT

    # Path 1: USDT -> A -> B -> USDT
    amt = start_usdt
    path1: List[Tuple[str, str, str]] = []
    a_amt = conv_amount(amt, ausdt, sym_map, book, from_asset="USDT", to_asset=base_a, fee=fee)
    if a_amt > 0:
        path1.append(("USDT", base_a, ausdt))
        b_amt = conv_amount(a_amt, cross, sym_map, book, from_asset=base_a, to_asset=base_b, fee=fee)
        if b_amt > 0:
            path1.append((base_a, base_b, cross))
            usdt_amt = conv_amount(b_amt, busdt, sym_map, book, from_asset=base_b, to_asset="USDT", fee=fee)
            if usdt_amt > 0:
                path1.append((base_b, "USDT", busdt))
                profit1 = (usdt_amt - start_usdt) / start_usdt * 100.0
            else:
                profit1 = -math.inf
        else:
            profit1 = -math.inf
    else:
        profit1 = -math.inf

    # Path 2: USDT -> B -> A -> USDT
    amt = start_usdt
    path2: List[Tuple[str, str, str]] = []
    b_amt = conv_amount(amt, busdt, sym_map, book, from_asset="USDT", to_asset=base_b, fee=fee)
    if b_amt > 0:
        path2.append(("USDT", base_b, busdt))
        a_amt = conv_amount(b_amt, cross, sym_map, book, from_asset=base_b, to_asset=base_a, fee=fee)
        if a_amt > 0:
            path2.append((base_b, base_a, cross))
            usdt_amt = conv_amount(a_amt, ausdt, sym_map, book, from_asset=base_a, to_asset="USDT", fee=fee)
            if usdt_amt > 0:
                path2.append((base_a, "USDT", ausdt))
                profit2 = (usdt_amt - start_usdt) / start_usdt * 100.0
            else:
                profit2 = -math.inf
        else:
            profit2 = -math.inf
    else:
        profit2 = -math.inf

    if profit1 >= profit2:
        return profit1, path1
    else:
        return profit2, path2


# ----- Networking -----

async def fetch_json(client: httpx.AsyncClient, url: str) -> dict:
    r = await client.get(url, timeout=10.0)
    r.raise_for_status()
    return r.json()


async def get_exchange_info(client: httpx.AsyncClient) -> dict:
    return await fetch_json(client, f"{BINANCE_REST}/api/v3/exchangeInfo")


async def get_all_book_tickers(client: httpx.AsyncClient) -> List[dict]:
    return await fetch_json(client, f"{BINANCE_REST}/api/v3/ticker/bookTicker")


def make_book_map(book_items: List[dict]) -> Dict[str, Dict[str, float]]:
    m: Dict[str, Dict[str, float]] = {}
    for it in book_items:
        sym = it.get("symbol")
        try:
            bid = float(it["bidPrice"])
            ask = float(it["askPrice"])
        except Exception:
            continue
        if not sym or bid <= 0 or ask <= 0:
            continue
        m[sym] = {"bid": bid, "ask": ask}
    return m


# ----- Main loop -----

async def main():
    parser = argparse.ArgumentParser(description="Binance triangular arbitrage finder (REST polling).")
    parser.add_argument("--threshold", type=float, default=0.2, help="Min profit %% to print (default 0.2)")
    parser.add_argument("--interval", type=float, default=1.0, help="Polling interval seconds (default 1.0)")
    parser.add_argument("--fee", type=float, default=0.001, help="Taker fee per trade (default 0.001 = 0.1%)")
    parser.add_argument("--top", type=int, default=10, help="Show top-N triangles per iteration (default 10)")
    args = parser.parse_args()

    async with httpx.AsyncClient() as client:
        info = await get_exchange_info(client)
        sym_map = parse_symbols(info)

        triangles = build_usdt_triangles(sym_map)
        if not triangles:
            print("No USDT triangles found. Exiting.")
            return

        print(f"Loaded {len(sym_map)} tradable symbols; precomputed {len(triangles)} triangles.")
        print("Scanning... Press Ctrl+C to stop.\n")

        while True:
            t0 = time.time()
            try:
                book_items = await get_all_book_tickers(client)
                book = make_book_map(book_items)
            except Exception as e:
                print("Error fetching tickers:", e)
                await asyncio.sleep(max(args.interval, 1.0))
                continue

            opps: List[tuple] = []
            for ausdt, cross, busdt in triangles:
                if ausdt not in book or cross not in book or busdt not in book:
                    continue
                profit, path = eval_triangle(ausdt, cross, busdt, sym_map, book, fee=args.fee)
                if profit >= args.threshold:
                    opps.append((profit, (ausdt, cross, busdt), path))

            opps.sort(key=lambda x: x[0], reverse=True)
            if opps:
                print(f"=== Opportunities (>{args.threshold:.3f}%): {len(opps)}  ===")
                for p, (a, c, b), path in opps[: args.top]:
                    route = " -> ".join([f"{f}->{t} [{s}]" for (f, t, s) in path])
                    print(f"{p:6.3f}% | {a} / {c} / {b} | {route}")
                print()

            dt = time.time() - t0
            await asyncio.sleep(max(0, args.interval - dt))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
