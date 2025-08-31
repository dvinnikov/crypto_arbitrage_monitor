# exchanges.py (multi-exchange: binance, exmo, okx, kucoin, mexc, gate, bybit)
import httpx
from typing import Dict, List, Tuple, Any

def _parse_levels(levels):
    out = []
    for lv in levels or []:
        if isinstance(lv, (list, tuple)) and len(lv) >= 2:
            try:
                price = float(lv[0]); qty = float(lv[1])
            except Exception:
                continue
            out.append((price, qty))
        elif isinstance(lv, dict) and "price" in lv and "size" in lv:
            try:
                out.append((float(lv["price"]), float(lv["size"])))
            except Exception:
                pass
    return out

class ExchangeBase:
    name: str
    async def get_symbols(self) -> Dict[str, Tuple[str, str]]:
        raise NotImplementedError
    async def get_book(self) -> Dict[str, Dict[str, float]]:
        raise NotImplementedError
    async def get_depth(self, symbol: str, limit: int = 20) -> Dict[str, List[Tuple[float, float]]]:
        """Return {'bids':[(price,qty),...], 'asks':[(price,qty),...]}"""
        raise NotImplementedError
    def get_specs(self) -> Dict[str, Any]:
        return {}

# ---------------------- BINANCE ----------------------
class BinancePublic(ExchangeBase):
    name = "binance"
    REST = "https://api.binance.com"

    def __init__(self):
        self._sym_map = {}
        self._specs = {}

    async def get_symbols(self):
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{self.REST}/api/v3/exchangeInfo", timeout=20.0)
            data = r.json()
        res, specs = {}, {}
        for s in data.get("symbols", []):
            if s.get("status") != "TRADING":
                continue
            sym, base, quote = s["symbol"], s["baseAsset"], s["quoteAsset"]
            res[sym] = (base, quote)
            f_map = {f.get("filterType"): f for f in s.get("filters", [])}
            lot = f_map.get("LOT_SIZE", {})
            min_notional = f_map.get("MIN_NOTIONAL", {})
            specs[sym] = {
                "min_qty": float(lot.get("minQty", 0) or 0),
                "step_size": float(lot.get("stepSize", 0) or 0),
                "min_notional": float(min_notional.get("minNotional", 0) or 0),
            }
        self._sym_map, self._specs = res, specs
        return res

    def get_specs(self): return self._specs

    async def get_book(self):
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{self.REST}/api/v3/ticker/bookTicker", timeout=20.0)
            items = r.json()
        book = {}
        for it in items:
            try:
                bid, ask = float(it["bidPrice"]), float(it["askPrice"])
            except: continue
            book[it["symbol"]] = {"bid": bid, "ask": ask}
        return book

    async def get_depth(self, symbol, limit=20):
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{self.REST}/api/v3/depth",
                                 params={"symbol": symbol, "limit": limit}, timeout=20.0)
            data = r.json()
        return {"bids": _parse_levels(data.get("bids")),
                "asks": _parse_levels(data.get("asks"))}

# ---------------------- EXMO ----------------------
class ExmoPublic(ExchangeBase):
    name = "exmo"
    REST = "https://api.exmo.com/v1.1"
    def __init__(self): self._pair_specs = {}

    async def get_symbols(self):
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{self.REST}/pair_settings", timeout=20.0)
            data = r.json()
        res, specs = {}, {}
        for pair, s in data.items():
            if "_" not in pair: continue
            base, quote = pair.split("_", 1)
            res[pair] = (base, quote)
            min_qty = float(s.get("min_quantity", 0) or 0)
            step = float(s.get("quantity_increment", 0) or 0)
            min_price = float(s.get("min_price", 0) or 0)
            min_amount = float(s.get("min_amount", 0) or 0)
            min_notional = min_amount if min_amount else (min_qty * min_price)
            specs[pair] = {"min_qty": min_qty, "step_size": step, "min_notional": min_notional}
        self._pair_specs = specs
        return res

    def get_specs(self): return self._pair_specs

    async def get_book(self):
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{self.REST}/ticker", timeout=20.0)
            data = r.json()
        return {p: {"bid": float(v["buy_price"]), "ask": float(v["sell_price"])}
                for p, v in data.items()}

    async def get_depth(self, symbol, limit=20):
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{self.REST}/order_book",
                                 params={"pair": symbol, "limit": limit}, timeout=20.0)
            data = r.json().get(symbol, {})
        return {"bids": _parse_levels(data.get("bid")),
                "asks": _parse_levels(data.get("ask"))}

# ---------------------- OKX ----------------------
class OkxPublic(ExchangeBase):
    name = "okx"
    REST = "https://www.okx.com"
    async def get_symbols(self):
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{self.REST}/api/v5/public/instruments",
                                 params={"instType": "SPOT"}, timeout=20.0)
            data = r.json()
        return {d["instId"]: tuple(d["instId"].split("-",1))
                for d in data.get("data", [])}
    async def get_book(self):
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{self.REST}/api/v5/market/tickers",
                                 params={"instType": "SPOT"}, timeout=20.0)
            data = r.json()
        return {d["instId"]: {"bid": float(d["bidPx"]), "ask": float(d["askPx"])}
                for d in data.get("data", [])}
    async def get_depth(self, symbol, limit=20):
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{self.REST}/api/v5/market/books",
                                 params={"instId": symbol, "sz": limit}, timeout=20.0)
            data = r.json()
        ob = (data.get("data") or [{}])[0]
        return {"bids": _parse_levels(ob.get("bids")),
                "asks": _parse_levels(ob.get("asks"))}

# ---------------------- KuCoin ----------------------
class KucoinPublic(ExchangeBase):
    name = "kucoin"
    REST = "https://api.kucoin.com"
    async def get_symbols(self):
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{self.REST}/api/v1/symbols", timeout=20.0)
            data = r.json()
        return {d["symbol"]: (d["baseCurrency"], d["quoteCurrency"])
                for d in data.get("data", [])}
    async def get_book(self):
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{self.REST}/api/v1/market/allTickers", timeout=20.0)
            data = r.json()
        return {d["symbol"]: {"bid": float(d["buy"]), "ask": float(d["sell"])}
                for d in data.get("data",{}).get("ticker",[])}
    async def get_depth(self, symbol, limit=20):
        lvl = 20 if limit<=20 else 100
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{self.REST}/api/v1/market/orderbook/level2_{lvl}",
                                 params={"symbol": symbol}, timeout=20.0)
            data = r.json()
        d = data.get("data",{})
        return {"bids": _parse_levels(d.get("bids")),
                "asks": _parse_levels(d.get("asks"))}

# ---------------------- MEXC ----------------------
class MexcPublic(ExchangeBase):
    name = "mexc"
    REST = "https://api.mexc.com"
    async def get_symbols(self):
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{self.REST}/api/v3/exchangeInfo", timeout=20.0)
            data = r.json()
        return {s["symbol"]:(s["baseAsset"],s["quoteAsset"])
                for s in data.get("symbols",[]) if s.get("status")=="TRADING"}
    async def get_book(self):
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{self.REST}/api/v3/ticker/bookTicker", timeout=20.0)
            items = r.json()
        return {it["symbol"]:{"bid":float(it["bidPrice"]),"ask":float(it["askPrice"])}
                for it in items}
    async def get_depth(self,symbol,limit=20):
        async with httpx.AsyncClient() as client:
            r=await client.get(f"{self.REST}/api/v3/depth",
                               params={"symbol":symbol,"limit":limit},timeout=20.0)
            data=r.json()
        return {"bids":_parse_levels(data.get("bids")),
                "asks":_parse_levels(data.get("asks"))}

# ---------------------- Gate ----------------------
class GatePublic(ExchangeBase):
    name = "gate"
    REST = "https://api.gateio.ws"
    async def get_symbols(self):
        async with httpx.AsyncClient() as client:
            r=await client.get(f"{self.REST}/api/v4/spot/currency_pairs",timeout=20.0)
            items=r.json()
        return {it["id"]:(it["base"],it["quote"]) for it in items}
    async def get_book(self):
        async with httpx.AsyncClient() as client:
            r=await client.get(f"{self.REST}/api/v4/spot/tickers",timeout=20.0)
            items=r.json()
        return {it["currency_pair"]:{
                    "bid":float(it["highest_bid"]),
                    "ask":float(it["lowest_ask"])} for it in items}
    async def get_depth(self,symbol,limit=20):
        async with httpx.AsyncClient() as client:
            r=await client.get(f"{self.REST}/api/v4/spot/order_book",
                               params={"currency_pair":symbol,"limit":limit},timeout=20.0)
            data=r.json()
        return {"bids":_parse_levels(data.get("bids")),
                "asks":_parse_levels(data.get("asks"))}

# ---------------------- Bybit ----------------------
class BybitPublic(ExchangeBase):
    name="bybit"
    REST="https://api.bybit.com"
    async def get_symbols(self):
        async with httpx.AsyncClient() as client:
            r=await client.get(f"{self.REST}/v5/market/instruments-info",
                               params={"category":"spot"},timeout=20.0)
            data=r.json()
        return {d["symbol"]:(d["baseCoin"],d["quoteCoin"])
                for d in data.get("result",{}).get("list",[])}
    async def get_book(self):
        async with httpx.AsyncClient() as client:
            r=await client.get(f"{self.REST}/v5/market/tickers",
                               params={"category":"spot"},timeout=20.0)
            data=r.json()
        return {d["symbol"]:{
                    "bid":float(d["bid1Price"]),
                    "ask":float(d["ask1Price"])}
                for d in data.get("result",{}).get("list",[])}
    async def get_depth(self,symbol,limit=20):
        async with httpx.AsyncClient() as client:
            r=await client.get(f"{self.REST}/v5/market/orderbook",
                               params={"category":"spot","symbol":symbol,"limit":limit},timeout=20.0)
            data=r.json()
        d=data.get("result",{})
        return {"bids":_parse_levels(d.get("b")),
                "asks":_parse_levels(d.get("a"))}

EXCHANGE_MAP={
    "binance":BinancePublic,
    "exmo":ExmoPublic,
    "okx":OkxPublic,
    "kucoin":KucoinPublic,
    "mexc":MexcPublic,
    "gate":GatePublic,
    "bybit":BybitPublic,
}
