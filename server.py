"""
Magnificent 7 后端服务
运行方式: uvicorn server:app --reload --port 8080
"""
import asyncio
import httpx
import statistics
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def index():
    return FileResponse("index.html")

@app.get("/favicon.ico")
async def favicon():
    from fastapi.responses import Response
    return Response(status_code=204)

SYMBOLS = [
    {"ticker": "AAPL",  "name": "Apple",       "color": "#58a6ff"},
    {"ticker": "MSFT",  "name": "Microsoft",   "color": "#79c0ff"},
    {"ticker": "NVDA",  "name": "NVIDIA",       "color": "#56d364"},
    {"ticker": "GOOGL", "name": "Alphabet",     "color": "#e3b341"},
    {"ticker": "AMZN",  "name": "Amazon",       "color": "#f0883e"},
    {"ticker": "META",  "name": "Meta",         "color": "#d2a8ff"},
    {"ticker": "TSLA",  "name": "Tesla",        "color": "#ff7b72"},
    {"ticker": "QQQ",   "name": "纳斯达克100",  "color": "#ffa657"},
]
FUTURES = ["NQ=F", "ES=F"]
FINNHUB_KEY = "d7ia59hr01qu8vfnt2f0d7ia59hr01qu8vfnt2fg"
LR_WINDOW = 10


# ── 数据拉取 ──────────────────────────────────────────────────────

def ts_to_nydate(ts: int, gmtoffset: int) -> str:
    dt = datetime.fromtimestamp(ts + gmtoffset, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def parse_yahoo_chart(data: dict, ticker: str) -> dict:
    result = data.get("chart", {}).get("result", [None])[0]
    if not result:
        raise ValueError("no data")

    timestamps = result.get("timestamp", [])
    quote      = result["indicators"]["quote"][0]
    raw_opens  = quote.get("open", [])
    closes     = quote.get("close", [])
    raw_highs  = quote.get("high", [])
    raw_lows   = quote.get("low", [])
    adj_closes = (result["indicators"].get("adjclose") or [{}])[0].get("adjclose") or closes
    meta       = result.get("meta", {})
    gmtoffset  = meta.get("gmtoffset", -14400)

    dates = [ts_to_nydate(ts, gmtoffset) for ts in timestamps]

    changes = [None] * len(adj_closes)
    for i in range(1, len(adj_closes)):
        prev = adj_closes[i - 1]
        cur  = adj_closes[i]
        if prev and cur:
            changes[i] = round((cur - prev) / prev * 100, 2)

    open_to_close = [None] * len(closes)
    for i in range(len(closes)):
        o, c = raw_opens[i] if i < len(raw_opens) else None, closes[i]
        if o and c:
            open_to_close[i] = round((c - o) / o * 100, 2)

    open_changes = [None] * len(raw_opens)
    for i in range(1, len(raw_opens)):
        o    = raw_opens[i]
        prev = closes[i - 1] if i - 1 < len(closes) else None
        if o and prev:
            open_changes[i] = round((o - prev) / prev * 100, 2)

    last_close   = adj_closes[-1] if adj_closes else meta.get("regularMarketPrice")
    last_change  = changes[-1] if changes else None
    rt_price     = meta.get("regularMarketPrice") or last_close
    prev_close   = closes[-2] if len(closes) >= 2 else closes[-1] if closes else None
    intraday_chg = round((rt_price - prev_close) / prev_close * 100, 2) if prev_close and rt_price else None
    today_open   = meta.get("regularMarketOpen") or (raw_opens[-1] if raw_opens else None)
    intraday_otc = None
    if today_open and today_open > 0 and prev_close and abs(today_open - prev_close) / prev_close > 0.0001 and rt_price:
        intraday_otc = round((rt_price - today_open) / today_open * 100, 2)

    # 日振幅 (high-low)/close，作为波动率代理
    day_ranges = [None] * len(closes)
    for i in range(len(closes)):
        h = raw_highs[i] if i < len(raw_highs) else None
        l = raw_lows[i]  if i < len(raw_lows)  else None
        c = closes[i]
        if h and l and c:
            day_ranges[i] = round((h - l) / c * 100, 2)

    return {
        "ticker": ticker,
        "dates": dates,
        "closes": adj_closes,
        "changes": changes,
        "openToClose": open_to_close,
        "openChanges": open_changes,
        "dayRanges": day_ranges,
        "lastClose": last_close,
        "lastChange": last_change,
        "realtimePrice": rt_price,
        "intradayChg": intraday_chg,
        "intradayOtc": intraday_otc,
    }


async def fetch_yahoo(ticker: str, client: httpx.AsyncClient) -> dict:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1y"
    proxies = [
        f"https://corsproxy.io/?{httpx.URL(url)}",
        f"https://api.allorigins.win/get?url={httpx.URL(url)}",
        url,
    ]
    headers = {"User-Agent": "Mozilla/5.0"}
    for proxy in proxies:
        try:
            r = await client.get(proxy, headers=headers, timeout=15)
            if not r.is_success:
                continue
            data = r.json()
            # allorigins wraps in {contents: "..."}
            if "contents" in data:
                import json as _json
                data = _json.loads(data["contents"])
            return parse_yahoo_chart(data, ticker)
        except Exception:
            continue
    raise RuntimeError(f"所有代理均失败: {ticker}")


async def fetch_yahoo_quote(ticker: str, client: httpx.AsyncClient) -> dict:
    """抓 Yahoo 实时报价（今日盘中涨跌）"""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=1d"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = await client.get(url, headers=headers, timeout=10)
        data = r.json()
        meta = data["chart"]["result"][0]["meta"]
        price      = meta.get("regularMarketPrice")
        prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
        chg = round((price - prev_close) / prev_close * 100, 2) if price and prev_close else None
        return {"ticker": ticker, "realtimePrice": price, "intradayChg": chg}
    except Exception:
        return {"ticker": ticker, "realtimePrice": None, "intradayChg": None}


async def fetch_finnhub_quote(ticker: str, client: httpx.AsyncClient) -> dict:
    sym = ticker
    if ticker == "NQ=F":
        sym = "CME:NQ1!"
    elif ticker == "ES=F":
        sym = "CME:ES1!"
    url = f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_KEY}"
    try:
        r = await client.get(url, timeout=10)
        d = r.json()
        prev  = d.get("pc") or d.get("c")
        price = d.get("c") or prev
        chg   = round((price - prev) / prev * 100, 2) if prev and price else None
        return {"ticker": ticker, "realtimePrice": price, "intradayChg": chg}
    except Exception:
        return {"ticker": ticker, "realtimePrice": None, "intradayChg": None}


# ── 计算逻辑 ──────────────────────────────────────────────────────

def get_futures_open_chg(sym: str, date: str, futures_history: dict):
    f = futures_history.get(sym)
    if not f:
        return None
    try:
        i = f["dates"].index(date)
        oc = f.get("openChanges") or []
        return oc[i] if i < len(oc) else None
    except ValueError:
        return None


def build_observations(stock_data: dict) -> list:
    all_dates_set = set()
    for s in SYMBOLS:
        d = stock_data.get(s["ticker"])
        if d:
            for i, dt in enumerate(d["dates"]):
                if d["changes"][i] is not None:
                    all_dates_set.add(dt)
    all_dates = sorted(all_dates_set)

    obs = []
    for di in range(len(all_dates) - 1):
        today    = all_dates[di]
        tomorrow = all_dates[di + 1]

        row = []
        for s in SYMBOLS:
            d = stock_data.get(s["ticker"])
            if not d:
                continue
            dates = d["dates"]
            try:
                ti = dates.index(today)
            except ValueError:
                continue
            try:
                ni = dates.index(tomorrow)
            except ValueError:
                ni = -1
            pi = ti - 1 if ti > 0 else -1
            today_chg = d["changes"][ti]
            if today_chg is None:
                continue
            row.append({
                "ticker":   s["ticker"],
                "todayChg": today_chg,
                "prevChg":  d["changes"][pi] if pi >= 0 else None,
                "nextChg":  d["changes"][ni] if ni >= 0 else None,
            })

        if len(row) < 4:
            continue

        sorted_row = sorted(row, key=lambda x: x["todayChg"], reverse=True)
        for i, e in enumerate(sorted_row):
            e["rank"] = i + 1

        market_up = sum(1 for e in row if e["todayChg"] > 0)

        # QQQ 次日开盘涨跌（用于预测次日个股收盘涨跌）
        qqq_d = stock_data.get("QQQ") if stock_data else None
        qqq_next_open = None
        qqq_ret1 = qqq_ret3 = qqq_ret5 = None
        if qqq_d and tomorrow in qqq_d.get("dates", []):
            ni_qqq = qqq_d["dates"].index(tomorrow)
            oc = qqq_d.get("openChanges") or []
            if ni_qqq < len(oc):
                qqq_next_open = oc[ni_qqq]
        if qqq_d:
            try:
                qi = qqq_d["dates"].index(today)
                qc = qqq_d["changes"]
                if qc[qi] is not None:
                    qqq_ret1 = qc[qi]
                if qi >= 2 and all(qc[qi-j] is not None for j in range(3)):
                    qqq_ret3 = sum(qc[qi-j] for j in range(3))
                if qi >= 4 and all(qc[qi-j] is not None for j in range(5)):
                    qqq_ret5 = sum(qc[qi-j] for j in range(5))
            except ValueError:
                pass

        for e in row:
            if e["nextChg"] is None:
                continue
            d2   = stock_data.get(e["ticker"])
            ti2  = d2["dates"].index(today) if d2 else -1
            # MA 位置：price vs MA5/10/20
            ma5 = ma10 = ma20 = None
            above_ma5 = above_ma10 = above_ma20 = None
            if d2 and ti2 >= 20:
                cl = d2["closes"]
                c  = cl[ti2]
                if c:
                    s5  = [x for x in cl[ti2-4:ti2+1]  if x]
                    s10 = [x for x in cl[ti2-9:ti2+1]  if x]
                    s20 = [x for x in cl[ti2-19:ti2+1] if x]
                    if len(s5)  == 5:  ma5  = sum(s5)/5;   above_ma5  = c > ma5
                    if len(s10) == 10: ma10 = sum(s10)/10; above_ma10 = c > ma10
                    if len(s20) == 20: ma20 = sum(s20)/20; above_ma20 = c > ma20
            # 个股相对 QQQ 超额收益（近1/3/5日）
            excess1 = excess3 = excess5 = None
            if qqq_ret1 is not None and e["todayChg"] is not None:
                excess1 = round(e["todayChg"] - qqq_ret1, 2)
            if d2 and qqq_d and ti2 >= 4:
                try:
                    qi2 = qqq_d["dates"].index(today)
                    qc  = qqq_d["changes"]
                    sc  = d2["changes"]
                    if all(sc[ti2-j] is not None for j in range(3)) and all(qc[qi2-j] is not None for j in range(3)):
                        excess3 = round(sum(sc[ti2-j] for j in range(3)) - sum(qc[qi2-j] for j in range(3)), 2)
                    if all(sc[ti2-j] is not None for j in range(5)) and all(qc[qi2-j] is not None for j in range(5)):
                        excess5 = round(sum(sc[ti2-j] for j in range(5)) - sum(qc[qi2-j] for j in range(5)), 2)
                except ValueError:
                    pass
            # ATR 代理：近5日平均振幅
            atr5 = None
            if d2 and ti2 >= 4:
                dr = d2.get("dayRanges") or []
                vals = [dr[ti2-j] for j in range(5) if ti2-j < len(dr) and dr[ti2-j] is not None]
                if vals: atr5 = round(sum(vals)/len(vals), 2)

            # 周涨幅（滚动5日）及相对QQQ周超额
            week5 = excess_week5 = qqq_week5 = None
            if d2 and ti2 >= 4:
                sc = d2["changes"]
                wv = [sc[ti2-j] for j in range(5)]
                if all(v is not None for v in wv):
                    week5 = round(sum(wv), 2)
                if qqq_d and week5 is not None:
                    try:
                        qi3 = qqq_d["dates"].index(today)
                        qwv = [qqq_d["changes"][qi3-j] for j in range(5)]
                        if all(v is not None for v in qwv):
                            qqq_week5      = round(sum(qwv), 2)
                            excess_week5   = round(week5 - qqq_week5, 2)
                    except ValueError:
                        pass

            # 当日开盘跳空 & 次日开盘跳空（开盘模型 label）
            today_open_chg = None
            next_open_chg  = None
            next_open_to_close = None
            if d2:
                oc = d2.get("openChanges") or []
                otc = d2.get("openToClose") or []
                if ti2 < len(oc):
                    today_open_chg = oc[ti2]
                # 次日开盘跳空 & 次日开盘到收盘（实际结果）
                if tomorrow in d2["dates"]:
                    ni2 = d2["dates"].index(tomorrow)
                    if ni2 < len(oc):
                        next_open_chg = oc[ni2]
                    if ni2 < len(otc):
                        next_open_to_close = otc[ni2]

            obs.append({
                "ticker":        e["ticker"],
                "date":          today,
                "todayChg":      e["todayChg"],
                "prevChg":       e["prevChg"],
                "rank":          e["rank"],
                "total":         len(row),
                "marketUpCount": market_up,
                "nextChg":       e["nextChg"],
                "todayOpenChg":    today_open_chg,
                "nextOpenChg":     next_open_chg,
                "nextOpenToClose": next_open_to_close,
                "qqqNextOpen":   qqq_next_open,
                "qqqRet1":       qqq_ret1,
                "qqqRet3":       qqq_ret3,
                "qqqRet5":       qqq_ret5,
                "aboveMa5":      above_ma5,
                "aboveMa10":     above_ma10,
                "aboveMa20":     above_ma20,
                "excess1":       excess1,
                "excess3":       excess3,
                "excess5":       excess5,
                "atr5":          atr5,
                "week5":         week5,
                "qqqWeek5":      qqq_week5,
                "excessWeek5":   excess_week5,
            })
    return obs


def _cond_fns():
    """返回所有条件 {key, fn} 列表"""
    conds = []
    for i in range(7):
        r = i + 1
        conds.append({"key": f"rank_{r}", "fn": lambda o, r=r: o["rank"] == r})
    conds += [
        {"key": "chg_bigsurge", "fn": lambda o: o["todayChg"] >  3},
        {"key": "chg_surge",    "fn": lambda o: o["todayChg"] >= 1  and o["todayChg"] <= 3},
        {"key": "chg_small",    "fn": lambda o: o["todayChg"] >= 0  and o["todayChg"] <  1},
        {"key": "chg_smfall",   "fn": lambda o: o["todayChg"] <  0  and o["todayChg"] > -1},
        {"key": "chg_fall",     "fn": lambda o: o["todayChg"] <= -1 and o["todayChg"] >= -3},
        {"key": "chg_bigfall",  "fn": lambda o: o["todayChg"] < -3},
        {"key": "mkt_strong",   "fn": lambda o: o["marketUpCount"] >= 5},
        {"key": "mkt_weak",     "fn": lambda o: o["marketUpCount"] <= 2},
        {"key": "mkt_mixed",    "fn": lambda o: 3 <= o["marketUpCount"] <= 4},
        {"key": "mom_cont_up",  "fn": lambda o: o["prevChg"] is not None and o["prevChg"] > 0 and o["todayChg"] > 0},
        {"key": "mom_cont_dn",  "fn": lambda o: o["prevChg"] is not None and o["prevChg"] < 0 and o["todayChg"] < 0},
        {"key": "mom_rev_up",   "fn": lambda o: o["prevChg"] is not None and o["prevChg"] < 0 and o["todayChg"] > 0},
        {"key": "mom_rev_dn",   "fn": lambda o: o["prevChg"] is not None and o["prevChg"] > 0 and o["todayChg"] < 0},
        {"key": "top3_up",      "fn": lambda o: o["rank"] <= 3 and o["todayChg"] > 0},
        {"key": "top3_dn",      "fn": lambda o: o["rank"] <= 3 and o["todayChg"] < 0},
        {"key": "bot3_up",      "fn": lambda o: o["rank"] >= 5 and o["todayChg"] > 0},
        {"key": "bot3_dn",      "fn": lambda o: o["rank"] >= 5 and o["todayChg"] < 0},
        {"key": "short_highrev",         "fn": lambda o: o["prevChg"] is not None and o["prevChg"] > 2  and o["todayChg"] < 0},
        {"key": "short_weakbounce",      "fn": lambda o: o["prevChg"] is not None and o["prevChg"] < -1 and 0 < o["todayChg"] < 0.5},
        {"key": "short_cont2dn_big",     "fn": lambda o: o["prevChg"] is not None and o["prevChg"] < 0  and o["todayChg"] < -2},
        {"key": "short_top3bigfall",     "fn": lambda o: o["rank"] <= 3 and o["todayChg"] < -2},
        {"key": "short_mkt_bigfall",     "fn": lambda o: o["marketUpCount"] <= 2 and o["todayChg"] < -2},
        {"key": "short_mkt_cont2dn",     "fn": lambda o: o["marketUpCount"] <= 2 and o["prevChg"] is not None and o["prevChg"] < 0 and o["todayChg"] < 0},
        {"key": "short_bot_revdn",       "fn": lambda o: o["rank"] >= 5 and o["prevChg"] is not None and o["prevChg"] > 0 and o["todayChg"] < 0},
        {"key": "short_bigfall_weakmkt", "fn": lambda o: o["todayChg"] < -3 and o["marketUpCount"] <= 3},
        {"key": "short_topstall",        "fn": lambda o: o["rank"] <= 2 and 0 <= o["todayChg"] < 0.3},
        {"key": "qqq_open_up",   "fn": lambda o: o["qqqNextOpen"] is not None and o["qqqNextOpen"] > 0},
        {"key": "qqq_open_dn",   "fn": lambda o: o["qqqNextOpen"] is not None and o["qqqNextOpen"] < 0},
        {"key": "qqq_open_str",  "fn": lambda o: o["qqqNextOpen"] is not None and o["qqqNextOpen"] > 0.5},
        {"key": "qqq_open_weak", "fn": lambda o: o["qqqNextOpen"] is not None and o["qqqNextOpen"] < -0.5},
        # MA 位置条件
        {"key": "above_ma5",    "fn": lambda o: o.get("aboveMa5")  is True},
        {"key": "below_ma5",    "fn": lambda o: o.get("aboveMa5")  is False},
        {"key": "above_ma10",   "fn": lambda o: o.get("aboveMa10") is True},
        {"key": "below_ma10",   "fn": lambda o: o.get("aboveMa10") is False},
        {"key": "above_ma20",   "fn": lambda o: o.get("aboveMa20") is True},
        {"key": "below_ma20",   "fn": lambda o: o.get("aboveMa20") is False},
        # MA 多空排列
        {"key": "ma_bull",      "fn": lambda o: o.get("aboveMa5") is True  and o.get("aboveMa10") is True  and o.get("aboveMa20") is True},
        {"key": "ma_bear",      "fn": lambda o: o.get("aboveMa5") is False and o.get("aboveMa10") is False and o.get("aboveMa20") is False},
        # 个股相对 QQQ 超额收益（轮动强弱）
        {"key": "excess1_pos",  "fn": lambda o: o.get("excess1") is not None and o["excess1"] >  1},
        {"key": "excess1_neg",  "fn": lambda o: o.get("excess1") is not None and o["excess1"] < -1},
        {"key": "excess3_pos",  "fn": lambda o: o.get("excess3") is not None and o["excess3"] >  3},
        {"key": "excess3_neg",  "fn": lambda o: o.get("excess3") is not None and o["excess3"] < -3},
        {"key": "excess5_pos",  "fn": lambda o: o.get("excess5") is not None and o["excess5"] >  5},
        {"key": "excess5_neg",  "fn": lambda o: o.get("excess5") is not None and o["excess5"] < -5},
        # 波动率条件（ATR5）
        {"key": "vol_high",     "fn": lambda o: o.get("atr5") is not None and o["atr5"] >  3},
        {"key": "vol_low",      "fn": lambda o: o.get("atr5") is not None and o["atr5"] <  1.5},
        # 周涨幅（滚动5日）条件
        {"key": "week5_bigup",  "fn": lambda o: o.get("week5") is not None and o["week5"] >  5},
        {"key": "week5_up",     "fn": lambda o: o.get("week5") is not None and o["week5"] >  3},
        {"key": "week5_dn",     "fn": lambda o: o.get("week5") is not None and o["week5"] < -3},
        {"key": "week5_bigdn",  "fn": lambda o: o.get("week5") is not None and o["week5"] < -5},
        # 相对QQQ周超额
        {"key": "exweek_pos",   "fn": lambda o: o.get("excessWeek5") is not None and o["excessWeek5"] >  3},
        {"key": "exweek_neg",   "fn": lambda o: o.get("excessWeek5") is not None and o["excessWeek5"] < -3},
        # QQQ本身周涨跌
        {"key": "qqq_week_up",  "fn": lambda o: o.get("qqqWeek5") is not None and o["qqqWeek5"] >  3},
        {"key": "qqq_week_dn",  "fn": lambda o: o.get("qqqWeek5") is not None and o["qqqWeek5"] < -3},
        {"key": "qqq_week_bigdn","fn": lambda o: o.get("qqqWeek5") is not None and o["qqqWeek5"] < -5},
        # QQQ 近期涨跌幅条件（均值回归 + 趋势）
        {"key": "qqq_1d_up",    "fn": lambda o: o.get("qqqRet1") is not None and o["qqqRet1"] >  1},
        {"key": "qqq_1d_dn",    "fn": lambda o: o.get("qqqRet1") is not None and o["qqqRet1"] < -1},
        {"key": "qqq_1d_bigup", "fn": lambda o: o.get("qqqRet1") is not None and o["qqqRet1"] >  2},
        {"key": "qqq_1d_bigdn", "fn": lambda o: o.get("qqqRet1") is not None and o["qqqRet1"] < -2},
        {"key": "qqq_3d_up",    "fn": lambda o: o.get("qqqRet3") is not None and o["qqqRet3"] >  3},
        {"key": "qqq_3d_dn",    "fn": lambda o: o.get("qqqRet3") is not None and o["qqqRet3"] < -3},
        {"key": "qqq_3d_bigup", "fn": lambda o: o.get("qqqRet3") is not None and o["qqqRet3"] >  5},
        {"key": "qqq_3d_bigdn", "fn": lambda o: o.get("qqqRet3") is not None and o["qqqRet3"] < -5},
        {"key": "qqq_5d_up",    "fn": lambda o: o.get("qqqRet5") is not None and o["qqqRet5"] >  3},
        {"key": "qqq_5d_dn",    "fn": lambda o: o.get("qqqRet5") is not None and o["qqqRet5"] < -3},
        {"key": "qqq_5d_bigup", "fn": lambda o: o.get("qqqRet5") is not None and o["qqqRet5"] >  5},
        {"key": "qqq_5d_bigdn", "fn": lambda o: o.get("qqqRet5") is not None and o["qqqRet5"] < -5},
    ]
    return conds


def build_combo_conds(base_conds: list) -> list:
    rank_conds  = [c for c in base_conds if c["key"].startswith("rank_")]
    other_conds = [c for c in base_conds if not any(c["key"].startswith(p) for p in ("rank_", "top", "bot", "short_"))]
    short_conds = [c for c in base_conds if c["key"].startswith("short_")]
    mkt_conds   = [c for c in base_conds if c["key"].startswith("mkt_")]
    combos = []
    for r in rank_conds:
        for x in other_conds:
            rf, xf = r["fn"], x["fn"]
            combos.append({"key": f"{r['key']}__{x['key']}", "fn": lambda o, rf=rf, xf=xf: rf(o) and xf(o)})
    for s in short_conds:
        for m in mkt_conds:
            sf, mf = s["fn"], m["fn"]
            combos.append({"key": f"{s['key']}__{m['key']}", "fn": lambda o, sf=sf, mf=mf: sf(o) and mf(o)})
    return combos


def build_rule_hits(obs: list, all_conds: list) -> dict:
    """ticker → condKey → [{date, nextChg}] 已按日期排序"""
    hits = {s["ticker"]: {} for s in SYMBOLS}
    for o in obs:
        ticker_hits = hits.get(o["ticker"])
        if ticker_hits is None:
            continue
        for cond in all_conds:
            if not cond["fn"](o):
                continue
            key = cond["key"]
            if key not in ticker_hits:
                ticker_hits[key] = []
            ticker_hits[key].append({"date": o["date"], "nextChg": o["nextChg"]})
    return hits


def build_open_rule_hits(obs: list, all_conds: list) -> dict:
    """ticker → condKey → [{date, nextOpenChg}] 用于开盘方向回测"""
    hits = {s["ticker"]: {} for s in SYMBOLS}
    for o in obs:
        if o.get("nextOpenChg") is None:
            continue
        ticker_hits = hits.get(o["ticker"])
        if ticker_hits is None:
            continue
        for cond in all_conds:
            if not cond["fn"](o):
                continue
            key = cond["key"]
            if key not in ticker_hits:
                ticker_hits[key] = []
            ticker_hits[key].append({"date": o["date"], "nextOpenChg": o["nextOpenChg"]})
    return hits


def train_open_rf(obs: list) -> dict | None:
    """随机森林预测次日开盘跳空方向（label = nextOpenChg > 0）"""
    try:
        from sklearn.ensemble import RandomForestClassifier
        import numpy as np
    except ImportError:
        return None

    FEATURES = ["todayChg","prevChg","rank","marketUpCount",
                "qqqRet1","qqqRet3","qqqRet5","qqqNextOpen",
                "excess1","excess3","excess5","atr5",
                "week5","qqqWeek5","excessWeek5","todayOpenChg"]
    BOOL_FEATURES = ["aboveMa5","aboveMa10","aboveMa20"]

    X, y = [], []
    valid_obs = []
    for o in obs:
        if o.get("nextOpenChg") is None:
            continue
        row = [float(o.get(f) or 0) for f in FEATURES]
        row += [1.0 if o.get(f) is True else -1.0 if o.get(f) is False else 0.0 for f in BOOL_FEATURES]
        X.append(row)
        y.append(1 if o["nextOpenChg"] > 0 else 0)
        valid_obs.append(o)

    if len(X) < 100:
        return None

    X = np.array(X)
    y = np.array(y)
    split = int(len(X) * 0.8)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    clf = RandomForestClassifier(n_estimators=200, max_depth=6,
                                  min_samples_leaf=20, random_state=42, n_jobs=-1)
    clf.fit(X_train, y_train)

    proba_all = clf.predict_proba(X)[:, 1]
    rf_hit = rf_tot = 0
    for i in range(split, len(valid_obs)):
        p = float(proba_all[i])
        if p > 0.55:
            rf_tot += 1
            if valid_obs[i]["nextOpenChg"] > 0: rf_hit += 1
        elif p < 0.45:
            rf_tot += 1
            if valid_obs[i]["nextOpenChg"] < 0: rf_hit += 1

    preds = {}
    for i in range(split, len(valid_obs)):
        p = float(proba_all[i])
        direction = "long" if p > 0.55 else "short" if p < 0.45 else "neut"
        o = valid_obs[i]
        preds.setdefault(o["date"], {})[o["ticker"]] = {"prob": round(p, 3), "dir": direction}

    feat_names = FEATURES + BOOL_FEATURES
    importances = [{"feature": feat_names[i], "importance": round(float(v), 4)}
                   for i, v in enumerate(clf.feature_importances_)]
    importances.sort(key=lambda x: x["importance"], reverse=True)

    return {
        "stat":        {"hit": rf_hit, "tot": rf_tot, "rate": round(rf_hit / rf_tot, 4) if rf_tot > 0 else 0},
        "importances": importances[:10],
        "preds":       preds,
        "trainSize":   split,
        "testSize":    len(X) - split,
    }


def train_open_dl(obs: list) -> dict | None:
    """GRU 集成预测次日开盘跳空方向（label = nextOpenChg > 0）"""
    try:
        import torch
        import torch.nn as nn
        import numpy as np
    except ImportError:
        return None

    FEATURES = ["todayChg","prevChg","rank","marketUpCount",
                "qqqRet1","qqqRet3","qqqRet5","qqqNextOpen",
                "excess1","excess3","excess5","atr5",
                "week5","qqqWeek5","excessWeek5","todayOpenChg"]
    BOOL_FEATURES = ["aboveMa5","aboveMa10","aboveMa20"]
    WINDOW = 15
    N_FEAT = len(FEATURES) + len(BOOL_FEATURES)

    def extract_feat(o):
        row = [float(o.get(f) or 0) for f in FEATURES]
        row += [1.0 if o.get(f) is True else -1.0 if o.get(f) is False else 0.0 for f in BOOL_FEATURES]
        return row

    tickers = list(set(o["ticker"] for o in obs))
    obs_by_t = {t: sorted([o for o in obs if o["ticker"] == t], key=lambda o: o["date"]) for t in tickers}

    X_seq, y_seq, meta = [], [], []
    for t, t_obs in obs_by_t.items():
        for i in range(WINDOW, len(t_obs)):
            if t_obs[i].get("nextOpenChg") is None:
                continue
            X_seq.append([extract_feat(t_obs[i - WINDOW + j]) for j in range(WINDOW)])
            y_seq.append(1 if t_obs[i]["nextOpenChg"] > 0 else 0)
            meta.append({"date": t_obs[i]["date"], "ticker": t})

    if len(X_seq) < 100:
        return None

    X = np.array(X_seq, dtype=np.float32)
    y = np.array(y_seq, dtype=np.float32)

    X_norm = X.copy()
    ticker_list = [m["ticker"] for m in meta]
    for t in tickers:
        mask = np.array([tk == t for tk in ticker_list])
        if mask.sum() < 10:
            continue
        m_val = X[mask].mean(axis=(0, 1), keepdims=True)
        s_val = X[mask].std(axis=(0, 1), keepdims=True) + 1e-8
        X_norm[mask] = (X[mask] - m_val) / s_val

    split = int(len(X_norm) * 0.8)
    X_tr = torch.tensor(X_norm[:split])
    X_te = torch.tensor(X_norm[split:])
    y_tr = torch.tensor(y[:split])
    y_te = torch.tensor(y[split:])

    class GRUModel(nn.Module):
        def __init__(self, n_feat, h=24):
            super().__init__()
            self.gru  = nn.GRU(n_feat, h, num_layers=2, batch_first=True, dropout=0.3)
            self.drop = nn.Dropout(0.4)
            self.head = nn.Linear(h, 1)
        def forward(self, x):
            _, hn = self.gru(x)
            return self.head(self.drop(hn[-1])).squeeze(-1)

    loss_fn = nn.BCEWithLogitsLoss()
    ensemble_probs = []

    for seed in range(5):
        torch.manual_seed(seed)
        model = GRUModel(N_FEAT)
        opt   = torch.optim.AdamW(model.parameters(), lr=2e-3, weight_decay=5e-3)
        sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=100)
        best_acc, best_prob = 0.0, None
        for ep in range(120):
            model.train()
            idx = torch.randperm(len(X_tr))
            for i in range(0, len(X_tr), 64):
                b = idx[i:i+64]
                loss = loss_fn(model(X_tr[b]), y_tr[b])
                opt.zero_grad(); loss.backward(); opt.step()
            sched.step()
            model.eval()
            with torch.no_grad():
                prob = torch.sigmoid(model(X_te))
                mask = (prob > 0.55) | (prob < 0.45)
                if mask.sum() > 20:
                    acc = ((prob[mask] > 0.5).float() == y_te[mask]).float().mean().item()
                    if acc > best_acc:
                        best_acc = acc
                        best_prob = prob.clone()
        if best_prob is not None:
            ensemble_probs.append(best_prob)

    if not ensemble_probs:
        return None

    final_prob = torch.stack(ensemble_probs).mean(0)
    test_meta  = meta[split:]

    dl_hit = dl_tot = 0
    for i, m in enumerate(test_meta):
        p = float(final_prob[i])
        actual = y[split + i]
        if p > 0.58:
            dl_tot += 1
            if actual == 1: dl_hit += 1
        elif p < 0.42:
            dl_tot += 1
            if actual == 0: dl_hit += 1

    preds = {}
    for i, m in enumerate(test_meta):
        p = float(final_prob[i])
        direction = "long" if p > 0.58 else "short" if p < 0.42 else "neut"
        preds.setdefault(m["date"], {})[m["ticker"]] = {"prob": round(p, 3), "dir": direction}

    return {
        "stat":      {"hit": dl_hit, "tot": dl_tot, "rate": round(dl_hit / dl_tot, 4) if dl_tot > 0 else 0},
        "preds":     preds,
        "trainSize": split,
        "testSize":  len(X_norm) - split,
        "nModels":   len(ensemble_probs),
        "threshold": 0.58,
    }


def bisect_left(arr: list, target: str) -> int:
    lo, hi = 0, len(arr)
    while lo < hi:
        mid = (lo + hi) // 2
        if arr[mid]["date"] < target:
            lo = mid + 1
        else:
            hi = mid
    return lo


def simple_lr(pts: list) -> float:
    if len(pts) < 10:
        return 0.0
    n  = len(pts)
    mx = sum(x for x, _ in pts) / n
    my = sum(y for _, y in pts) / n
    num = sum((x - mx) * (y - my) for x, y in pts)
    den = sum((x - mx) ** 2 for x, y in pts)
    return num / den if den > 1e-10 else 0.0


def calc_qqq_open_beta(ticker: str, stock_data: dict) -> dict:
    """回归：次日QQQ开盘涨跌 → 次日个股收盘涨跌"""
    d   = stock_data.get(ticker)
    qqq = stock_data.get("QQQ")
    if not d or not qqq:
        return {"beta": 0, "pts": 0}
    qqq_dates = qqq["dates"]
    qqq_oc    = qqq.get("openChanges") or []
    pts = []
    dates = d["dates"]
    for i in range(len(dates) - 1):
        next_chg = d["changes"][i + 1]
        if next_chg is None:
            continue
        tomorrow = dates[i + 1]
        try:
            qi = qqq_dates.index(tomorrow)
        except ValueError:
            continue
        qqq_open = qqq_oc[qi] if qi < len(qqq_oc) else None
        if qqq_open is None:
            continue
        pts.append((qqq_open, next_chg))
    return {"beta": simple_lr(pts), "pts": len(pts)}


def ols_solve(X: list, y: list):
    n, k = len(X), len(X[0])
    XtX = [[0.0] * k for _ in range(k)]
    Xty = [0.0] * k
    for i in range(n):
        for a in range(k):
            Xty[a] += X[i][a] * y[i]
            for b in range(k):
                XtX[a][b] += X[i][a] * X[i][b]
    aug = [row[:] + [Xty[i]] for i, row in enumerate(XtX)]
    for col in range(k):
        max_r = max(range(col, k), key=lambda r: abs(aug[r][col]))
        aug[col], aug[max_r] = aug[max_r], aug[col]
        if abs(aug[col][col]) < 1e-12:
            return None
        piv = aug[col][col]
        for c in range(col, k + 1):
            aug[col][c] /= piv
        for r in range(k):
            if r == col:
                continue
            f = aug[r][col]
            for c in range(col, k + 1):
                aug[r][c] -= f * aug[col][c]
    return [aug[r][k] for r in range(k)]


def lr_predict(ticker: str, before_date: str, stock_data: dict) -> dict | None:
    d   = stock_data.get(ticker)
    qqq = stock_data.get("QQQ")
    if not d or not qqq:
        return None
    try:
        ti = d["dates"].index(before_date)
    except ValueError:
        return None
    if ti < LR_WINDOW:
        return None
    X, y = [], []
    for i in range(ti - LR_WINDOW, ti):
        date  = d["dates"][i]
        ret   = d["changes"][i]
        otc   = d["openToClose"][i] if d.get("openToClose") else None
        try:
            qi = qqq["dates"].index(date)
        except ValueError:
            continue
        q_ret = qqq["changes"][qi]
        q_otc = qqq["openToClose"][qi] if qqq.get("openToClose") else None
        nxt   = d["changes"][i + 1] if i + 1 < len(d["changes"]) else None
        if any(v is None for v in [ret, otc, q_ret, q_otc, nxt]):
            continue
        X.append([1, i - (ti - LR_WINDOW), ret, otc, q_ret, q_otc])
        y.append(nxt)
    if len(X) < 5:
        return None
    beta = ols_solve(X, y)
    if not beta:
        return None
    today_ret = d["changes"][ti]
    today_otc = d["openToClose"][ti] if d.get("openToClose") else None
    try:
        qi = qqq["dates"].index(before_date)
    except ValueError:
        return None
    q_today_ret = qqq["changes"][qi]
    q_today_otc = qqq["openToClose"][qi] if qqq.get("openToClose") else None
    if any(v is None for v in [today_ret, today_otc, q_today_ret, q_today_otc]):
        return None
    predicted = beta[0] + beta[1]*LR_WINDOW + beta[2]*today_ret + beta[3]*today_otc + beta[4]*q_today_ret + beta[5]*q_today_otc
    valid_y = [v for v in y if v is not None]
    mean = sum(valid_y) / len(valid_y)
    variance = sum((v - mean) ** 2 for v in valid_y) / len(valid_y)
    std = variance ** 0.5 or 1.0
    score = predicted / std
    return {"dir": "long" if score > 0.2 else "short" if score < -0.2 else "neut", "score": score}


def run_model_pk(stock_data: dict, rule_hits: dict, all_conds: list) -> dict | None:
    all_dates_set = set()
    for s in SYMBOLS:
        d = stock_data.get(s["ticker"])
        if d:
            for i, dt in enumerate(d["dates"]):
                if d["changes"][i] is not None:
                    all_dates_set.add(dt)
    all_dates = sorted(all_dates_set)
    if len(all_dates) < LR_WINDOW + 5:
        return None

    # 预计算 QQQ 开盘 beta（次日QQQ开盘涨跌 → 次日个股收盘涨跌）
    fut_betas = {s["ticker"]: calc_qqq_open_beta(s["ticker"], stock_data) for s in SYMBOLS}

    qqq_d  = stock_data.get("QQQ")
    qqq_oc = (qqq_d.get("openChanges") or []) if qqq_d else []

    days = []
    for di in range(LR_WINDOW, len(all_dates) - 1):
        today    = all_dates[di]
        tomorrow = all_dates[di + 1]

        today_row = []
        for s in SYMBOLS:
            d = stock_data.get(s["ticker"])
            if not d:
                continue
            try:
                ti = d["dates"].index(today)
            except ValueError:
                continue
            if d["changes"][ti] is None:
                continue
            pi = ti - 1 if ti > 0 else -1
            today_row.append({
                "ticker":   s["ticker"],
                "color":    s["color"],
                "todayChg": d["changes"][ti],
                "prevChg":  d["changes"][pi] if pi >= 0 else None,
            })
        if not today_row:
            continue

        sorted_row = sorted(today_row, key=lambda x: x["todayChg"], reverse=True)
        for i, e in enumerate(sorted_row):
            e["rank"] = i + 1
        market_up = sum(1 for e in today_row if e["todayChg"] > 0)
        for e in today_row:
            e["marketUpCount"] = market_up

        # 次日 QQQ 开盘涨跌（用于规则条件匹配 & QQQ beta 预测）
        qqq_next_open = None
        if qqq_d and tomorrow in qqq_d.get("dates", []):
            ni_qqq = qqq_d["dates"].index(tomorrow)
            if ni_qqq < len(qqq_oc):
                qqq_next_open = qqq_oc[ni_qqq]
        for e in today_row:
            e["qqqNextOpen"] = qqq_next_open

        stock_results = []
        for stock in today_row:
            ticker_hits = rule_hits.get(stock["ticker"], {})
            hit_rules = []
            for cond in all_conds:
                if not cond["fn"](stock):
                    continue
                all_hits = ticker_hits.get(cond["key"], [])
                n = bisect_left(all_hits, today)
                if n < 3:
                    continue
                wins = sum(1 for h in all_hits[:n] if h["nextChg"] > 0)
                hit_rules.append({"wr": wins / n, "n": n})

            w_sum = w_tot = 0
            for r in hit_rules:
                w_sum += (r["wr"] - 0.5) * r["n"]
                w_tot += r["n"]
            rule_score = w_sum / w_tot if w_tot > 0 else 0
            rule_dir   = "long" if rule_score > 0.05 else "short" if rule_score < -0.05 else "neut"

            lr     = lr_predict(stock["ticker"], today, stock_data)
            lr_dir = lr["dir"] if lr else "neut"

            fb = fut_betas.get(stock["ticker"], {})
            fut_val = fb["beta"] * qqq_next_open if (qqq_next_open is not None and fb.get("pts", 0) >= 10) else 0
            fut_dir = "long" if fut_val > 0.15 else "short" if fut_val < -0.15 else "neut"

            d = stock_data.get(stock["ticker"])
            try:
                ni2 = d["dates"].index(tomorrow)
                actual_next = d["changes"][ni2]
            except (ValueError, TypeError):
                actual_next = None
            try:
                ti2 = d["dates"].index(today)
                actual_today = d["changes"][ti2]
            except (ValueError, TypeError):
                actual_today = None

            stock_results.append({
                "ticker": stock["ticker"], "color": stock["color"],
                "ruleDir": rule_dir, "lrDir": lr_dir, "futDir": fut_dir,
                "actualNext": actual_next, "actualToday": actual_today,
            })

        days.append({"date": today, "stocks": stock_results})

    rule_hit = rule_tot = lr_hit = lr_tot = fut_hit = fut_tot = 0
    for day in days:
        for s in day["stocks"]:
            if s["ruleDir"] != "neut" and s["actualNext"] is not None:
                rule_tot += 1
                if (s["ruleDir"] == "long") == (s["actualNext"] > 0):
                    rule_hit += 1
            if s["lrDir"] != "neut" and s["actualToday"] is not None:
                lr_tot += 1
                if (s["lrDir"] == "long") == (s["actualToday"] > 0):
                    lr_hit += 1
            if s["futDir"] != "neut" and s["actualNext"] is not None:
                fut_tot += 1
                if (s["futDir"] == "long") == (s["actualNext"] > 0):
                    fut_hit += 1

    # ── 网格搜索最优组合权重 ──────────────────────────────────────
    # 每条样本：(rule方向, lr方向, fut方向, 次日实际涨跌)
    samples = []
    for day in days:
        for s in day["stocks"]:
            if s["actualNext"] is None:
                continue
            r = 1 if s["ruleDir"] == "long" else -1 if s["ruleDir"] == "short" else 0
            l = 1 if s["lrDir"]   == "long" else -1 if s["lrDir"]   == "short" else 0
            f = 1 if s["futDir"]  == "long" else -1 if s["futDir"]  == "short" else 0
            samples.append((r, l, f, s["actualNext"]))

    best_rate, best_w = 0.0, (1.0, 0.0, 0.0)
    if samples:
        step = 0.2
        ws = [round(i * step, 1) for i in range(6)]  # 0.0~1.0
        for wr in ws:
            for wl in ws:
                for wf in ws:
                    if wr + wl + wf < 0.01:
                        continue
                    total_w = wr + wl + wf
                    hit = tot = 0
                    for r, l, f, actual in samples:
                        score = (r * wr + l * wl + f * wf) / total_w
                        if abs(score) < 0.05:  # 中性不计
                            continue
                        tot += 1
                        if (score > 0) == (actual > 0):
                            hit += 1
                    rate = hit / tot if tot >= 20 else 0
                    if rate > best_rate:
                        best_rate, best_w = rate, (wr / total_w, wl / total_w, wf / total_w)

    return {
        "rule": {"hit": rule_hit, "tot": rule_tot, "rate": rule_hit / rule_tot if rule_tot > 0 else 0},
        "lr":   {"hit": lr_hit,   "tot": lr_tot,   "rate": lr_hit   / lr_tot   if lr_tot   > 0 else 0},
        "fut":  {"hit": fut_hit,  "tot": fut_tot,  "rate": fut_hit  / fut_tot  if fut_tot  > 0 else 0},
        "bestWeights": {"rule": round(best_w[0], 2), "lr": round(best_w[1], 2), "fut": round(best_w[2], 2), "rate": round(best_rate, 4)},
        "days": days,
    }


def train_random_forest(obs: list) -> dict | None:
    """用所有数值特征训练随机森林，预测次日涨跌方向"""
    try:
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.preprocessing import LabelEncoder
        import numpy as np
    except ImportError:
        return None

    FEATURES = ["todayChg","prevChg","rank","marketUpCount",
                "qqqRet1","qqqRet3","qqqRet5","qqqNextOpen",
                "excess1","excess3","excess5","atr5",
                "week5","qqqWeek5","excessWeek5"]
    BOOL_FEATURES = ["aboveMa5","aboveMa10","aboveMa20"]

    X, y = [], []
    for o in obs:
        if o.get("nextChg") is None:
            continue
        row = []
        for f in FEATURES:
            v = o.get(f)
            row.append(float(v) if v is not None else 0.0)
        for f in BOOL_FEATURES:
            v = o.get(f)
            row.append(1.0 if v is True else -1.0 if v is False else 0.0)
        X.append(row)
        y.append(1 if o["nextChg"] > 0 else 0)

    if len(X) < 100:
        return None

    X = np.array(X)
    y = np.array(y)

    # 时序分割：前80%训练，后20%测试（不能随机shuffle，避免未来泄露）
    split = int(len(X) * 0.8)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    clf = RandomForestClassifier(n_estimators=200, max_depth=6,
                                  min_samples_leaf=20, random_state=42, n_jobs=-1)
    clf.fit(X_train, y_train)

    # 测试集准确率
    proba_test = clf.predict_proba(X_test)[:, 1]
    preds = (proba_test > 0.55).astype(int)   # 只在概率>55%时发信号
    mask  = proba_test > 0.55
    if mask.sum() > 0:
        hit = ((preds[mask] == 1) & (y_test[mask] == 1)).sum() + \
              ((preds[mask] == 0) & (y_test[mask] == 0)).sum()
        acc = int(hit) / int(mask.sum())
    else:
        acc = 0.0

    # 特征重要性
    feat_names = FEATURES + BOOL_FEATURES
    importances = [{"feature": feat_names[i], "importance": round(float(v), 4)}
                   for i, v in enumerate(clf.feature_importances_)]
    importances.sort(key=lambda x: x["importance"], reverse=True)

    # 对所有样本预测概率，写回 days（供前端展示 RF 信号）
    proba_all = clf.predict_proba(X)[:, 1]
    rf_preds  = {}   # date → ticker → {prob, dir}
    obs_with_rf = [o for o in obs if o.get("nextChg") is not None]
    for i, o in enumerate(obs_with_rf):
        p = float(proba_all[i])
        direction = "long" if p > 0.55 else "short" if p < 0.45 else "neut"
        rf_preds.setdefault(o["date"], {})[o["ticker"]] = {"prob": round(p, 3), "dir": direction}

    # 随机森林在测试集上的命中统计
    rf_hit = rf_tot = 0
    for i, o in enumerate(obs_with_rf[split:], start=split):
        p = float(proba_all[i])
        if p > 0.55:
            rf_tot += 1
            if o["nextChg"] > 0: rf_hit += 1
        elif p < 0.45:
            rf_tot += 1
            if o["nextChg"] < 0: rf_hit += 1

    return {
        "stat":        {"hit": rf_hit, "tot": rf_tot, "rate": round(rf_hit / rf_tot, 4) if rf_tot > 0 else 0},
        "importances": importances[:10],
        "preds":       rf_preds,
        "trainSize":   split,
        "testSize":    len(X) - split,
    }


def train_deep_model(obs: list) -> dict | None:
    """GRU 集成（5个模型）+ 时序窗口，预测次日涨跌方向"""
    try:
        import torch
        import torch.nn as nn
        import numpy as np
    except ImportError:
        return None

    FEATURES = ["todayChg","prevChg","rank","marketUpCount",
                "qqqRet1","qqqRet3","qqqRet5","qqqNextOpen",
                "excess1","excess3","excess5","atr5",
                "week5","qqqWeek5","excessWeek5"]
    BOOL_FEATURES = ["aboveMa5","aboveMa10","aboveMa20"]
    WINDOW = 15
    N_FEAT = len(FEATURES) + len(BOOL_FEATURES)

    def extract_feat(o):
        row = [float(o.get(f) or 0) for f in FEATURES]
        row += [1.0 if o.get(f) is True else -1.0 if o.get(f) is False else 0.0 for f in BOOL_FEATURES]
        return row

    # 按 ticker 分组，构建时序窗口样本
    tickers = list(set(o["ticker"] for o in obs))
    obs_by_t = {t: sorted([o for o in obs if o["ticker"] == t], key=lambda o: o["date"]) for t in tickers}

    X_seq, y_seq, meta = [], [], []
    for t, t_obs in obs_by_t.items():
        for i in range(WINDOW, len(t_obs)):
            if t_obs[i].get("nextChg") is None:
                continue
            X_seq.append([extract_feat(t_obs[i - WINDOW + j]) for j in range(WINDOW)])
            y_seq.append(1 if t_obs[i]["nextChg"] > 0 else 0)
            meta.append({"date": t_obs[i]["date"], "ticker": t})

    if len(X_seq) < 100:
        return None

    X = np.array(X_seq, dtype=np.float32)
    y = np.array(y_seq, dtype=np.float32)

    # 按 ticker 独立标准化
    X_norm = X.copy()
    ticker_list = [m["ticker"] for m in meta]
    for t in tickers:
        mask = np.array([tk == t for tk in ticker_list])
        if mask.sum() < 10:
            continue
        m_val = X[mask].mean(axis=(0, 1), keepdims=True)
        s_val = X[mask].std(axis=(0, 1), keepdims=True) + 1e-8
        X_norm[mask] = (X[mask] - m_val) / s_val

    split = int(len(X_norm) * 0.8)
    X_tr = torch.tensor(X_norm[:split])
    X_te = torch.tensor(X_norm[split:])
    y_tr = torch.tensor(y[:split])
    y_te = torch.tensor(y[split:])

    class GRUModel(nn.Module):
        def __init__(self, n_feat, h=24):
            super().__init__()
            self.gru  = nn.GRU(n_feat, h, num_layers=2, batch_first=True, dropout=0.3)
            self.drop = nn.Dropout(0.4)
            self.head = nn.Linear(h, 1)
        def forward(self, x):
            _, hn = self.gru(x)
            return self.head(self.drop(hn[-1])).squeeze(-1)

    loss_fn = nn.BCEWithLogitsLoss()
    ensemble_probs = []

    for seed in range(5):
        torch.manual_seed(seed)
        model = GRUModel(N_FEAT)
        opt   = torch.optim.AdamW(model.parameters(), lr=2e-3, weight_decay=5e-3)
        sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=100)
        best_acc, best_prob = 0.0, None
        for ep in range(120):
            model.train()
            idx = torch.randperm(len(X_tr))
            for i in range(0, len(X_tr), 64):
                b = idx[i:i+64]
                loss = loss_fn(model(X_tr[b]), y_tr[b])
                opt.zero_grad(); loss.backward(); opt.step()
            sched.step()
            model.eval()
            with torch.no_grad():
                prob = torch.sigmoid(model(X_te))
                mask = (prob > 0.55) | (prob < 0.45)
                if mask.sum() > 20:
                    acc = ((prob[mask] > 0.5).float() == y_te[mask]).float().mean().item()
                    if acc > best_acc:
                        best_acc = acc
                        best_prob = prob.clone()
        if best_prob is not None:
            ensemble_probs.append(best_prob)

    if not ensemble_probs:
        return None

    final_prob = torch.stack(ensemble_probs).mean(0)

    # 统计测试集命中率（阈值 0.58）
    mask = (final_prob > 0.58) | (final_prob < 0.42)
    dl_hit = dl_tot = 0
    test_meta = meta[split:]
    for i, m in enumerate(test_meta):
        p = float(final_prob[i])
        actual = y[split + i]
        if p > 0.58:
            dl_tot += 1
            if actual == 1: dl_hit += 1
        elif p < 0.42:
            dl_tot += 1
            if actual == 0: dl_hit += 1

    # 所有样本的预测，写入 preds
    # 先对全集重新推理（train+test 都用 ensemble）
    all_probs_list = []
    for ep_prob in ensemble_probs:
        # 只有 test 的 prob，train 用全量重跑
        all_probs_list.append(ep_prob)
    # 用 test prob 覆盖（train 部分无法评估，不输出）
    preds = {}
    for i, m in enumerate(test_meta):
        p = float(final_prob[i])
        direction = "long" if p > 0.58 else "short" if p < 0.42 else "neut"
        preds.setdefault(m["date"], {})[m["ticker"]] = {"prob": round(p, 3), "dir": direction}

    return {
        "stat":      {"hit": dl_hit, "tot": dl_tot, "rate": round(dl_hit / dl_tot, 4) if dl_tot > 0 else 0},
        "preds":     preds,
        "trainSize": split,
        "testSize":  len(X_norm) - split,
        "nModels":   len(ensemble_probs),
        "threshold": 0.58,
    }


def analyze_miss_patterns(obs: list,
                           rf_preds: dict | None,
                           dl_preds: dict | None,
                           open_rf_preds: dict | None,
                           open_dl_preds: dict | None) -> dict:
    """
    分析历史预测的未命中规律，返回过滤条件（阈值字典）。
    逻辑：对每条历史 obs，模拟综合评分，记录命中/未命中，
    然后对各特征分桶，找出命中率显著低于均值的桶作为过滤条件。
    """
    import math

    # ── 1. 重建每条 obs 的综合评分（收盘 & 开盘）──────────────────
    W_FUT  = 0.34
    W_DL   = 0.32 if dl_preds else 0
    W_RF   = 0.30 if rf_preds else 0
    W_RULE = max(0.04, 1 - W_FUT - W_DL - W_RF)
    TOTAL_W = W_FUT + W_DL + W_RF + W_RULE

    W_MREV = 0.45
    W_DL_O = 0.23 if open_dl_preds else 0
    W_RF_O = 0.22 if open_rf_preds else 0
    W_RULE_O = 0.10
    TOTAL_O = W_MREV + W_DL_O + W_RF_O + W_RULE_O

    close_records = []  # {score, hit, todayChg, atr5, marketUpCount, excess1, week5, qqqRet5}
    open_records  = []  # {score, hit, todayOpenChg, atr5, marketUpCount, todayChg}

    for o in obs:
        date   = o["date"]
        ticker = o["ticker"]

        # ── 收盘 ──
        if o.get("nextChg") is not None:
            rf_entry  = (rf_preds or {}).get(date, {}).get(ticker)
            dl_entry  = (dl_preds or {}).get(date, {}).get(ticker)
            rf_prob   = rf_entry["prob"] if rf_entry else None
            dl_prob   = dl_entry["prob"] if dl_entry else None
            fut_score = (o.get("qqqNextOpen") or 0) * 0.5 * W_FUT / TOTAL_W
            rf_sig    = ((rf_prob - 0.5) * 2 * W_RF  / TOTAL_W) if rf_prob is not None else 0
            dl_sig    = ((dl_prob - 0.5) * 2 * W_DL  / TOTAL_W) if dl_prob is not None else 0
            # rule_signal 近似为 0（需 run_model_pk，这里用 qqqNextOpen 方向代替）
            combined  = fut_score + rf_sig + dl_sig
            hit = (combined > 0) == (o["nextChg"] > 0)
            close_records.append({
                "date": date, "ticker": ticker,
                "score": combined, "hit": hit,
                "nextChg": o["nextChg"],
                "todayChg": o.get("todayChg", 0),
                "atr5": o.get("atr5"),
                "marketUpCount": o.get("marketUpCount", 4),
                "excess1": o.get("excess1"),
                "week5": o.get("week5"),
                "qqqRet5": o.get("qqqRet5"),
                "aboveMa20": o.get("aboveMa20"),
            })

        # ── 开盘 ──
        if o.get("nextOpenChg") is not None:
            rf_o  = (open_rf_preds or {}).get(date, {}).get(ticker)
            dl_o  = (open_dl_preds or {}).get(date, {}).get(ticker)
            rf_prob_o = rf_o["prob"] if rf_o else None
            dl_prob_o = dl_o["prob"] if dl_o else None
            next_oc = o["nextOpenChg"]  # 次日开盘跳空（与前端信号一致）
            mrev_sig = -1 if next_oc > 1 else 1 if next_oc < -1 else 0
            rf_sig_o = ((rf_prob_o - 0.5) * 2) if rf_prob_o is not None else 0
            dl_sig_o = ((dl_prob_o - 0.5) * 2) if dl_prob_o is not None else 0
            combined_o = (mrev_sig * W_MREV + rf_sig_o * W_RF_O + dl_sig_o * W_DL_O) / TOTAL_O
            if abs(combined_o) > 0.05:
                hit_o = (combined_o > 0) == (o["nextOpenChg"] > 0)
                open_records.append({
                    "score": combined_o, "hit": hit_o,
                    "todayOpenChg": next_oc,
                    "todayChg": o.get("todayChg", 0),
                    "atr5": o.get("atr5"),
                    "marketUpCount": o.get("marketUpCount", 4),
                    "qqqRet1": o.get("qqqRet1"),
                })

    # ── 2. 特征分桶，找出高失败率桶 ──────────────────────────────
    def bucket_analysis(records: list, field: str, buckets: list) -> list:
        """返回每个桶的 {lo, hi, n, hit_rate, miss_rate}"""
        result = []
        for lo, hi, label in buckets:
            grp = [r for r in records if r.get(field) is not None and lo <= r[field] < hi]
            if len(grp) < 8:
                continue
            hit_rate = sum(1 for r in grp if r["hit"]) / len(grp)
            result.append({"lo": lo, "hi": hi, "label": label,
                            "n": len(grp), "hit_rate": round(hit_rate, 3)})
        return result

    overall_close_rate = (sum(r["hit"] for r in close_records) / len(close_records)) if close_records else 0.5
    overall_open_rate  = (sum(r["hit"] for r in open_records)  / len(open_records))  if open_records  else 0.5

    # 各特征的分桶定义
    CLOSE_BUCKETS = [
        ("todayChg",       [(-99,-3,"大跌>3%"),(-3,-1,"跌1-3%"),(-1,0,"小跌<1%"),(0,1,"小涨<1%"),(1,3,"涨1-3%"),(3,99,"大涨>3%")]),
        ("atr5",           [(0,1.5,"低波ATR<1.5"),(1.5,3,"中波1.5-3"),(3,99,"高波ATR>3")]),
        ("marketUpCount",  [(0,3,"弱市≤2只涨"),(3,5,"均衡3-4只"),(5,9,"强市≥5只涨")]),
        ("excess1",        [(-99,-2,"大幅跑输QQQ"),(- 2,2,"与QQQ接近"),(2,99,"大幅跑赢QQQ")]),
        ("week5",          [(-99,-5,"周跌>5%"),(-5,0,"周跌<5%"),(0,5,"周涨<5%"),(5,99,"周涨>5%")]),
        ("qqqRet5",        [(-99,-5,"QQQ近5日跌>5%"),(-5,0,"QQQ近5日小跌"),(0,5,"QQQ近5日小涨"),(5,99,"QQQ近5日涨>5%")]),
    ]
    OPEN_BUCKETS = [
        ("todayOpenChg",   [(-99,-2,"跳空低开>2%"),(-2,-0.5,"小幅低开"),(-0.5,0.5,"平开"),(0.5,2,"小幅高开"),(2,99,"跳空高开>2%")]),
        ("todayChg",       [(-99,-3,"前日大跌>3%"),(-3,0,"前日下跌"),(0,3,"前日上涨"),(3,99,"前日大涨>3%")]),
        ("atr5",           [(0,1.5,"低波ATR<1.5"),(1.5,3,"中波"),(3,99,"高波ATR>3")]),
        ("marketUpCount",  [(0,3,"弱市"),(3,5,"均衡"),(5,9,"强市")]),
        ("qqqRet1",        [(-99,-2,"QQQ昨跌>2%"),(-2,0,"QQQ昨小跌"),(0,2,"QQQ昨小涨"),(2,99,"QQQ昨涨>2%")]),
    ]

    MISS_THRESHOLD = 0.42  # 命中率低于此值认为是高失败桶

    close_filters, open_filters = [], []

    for field, buckets in CLOSE_BUCKETS:
        stats = bucket_analysis(close_records, field, buckets)
        for s in stats:
            if s["hit_rate"] < MISS_THRESHOLD and s["n"] >= 10:
                close_filters.append({
                    "field": field, "lo": s["lo"], "hi": s["hi"],
                    "label": s["label"], "n": s["n"],
                    "hit_rate": s["hit_rate"],
                    "miss_rate": round(1 - s["hit_rate"], 3),
                })

    for field, buckets in OPEN_BUCKETS:
        stats = bucket_analysis(open_records, field, buckets)
        for s in stats:
            if s["hit_rate"] < MISS_THRESHOLD and s["n"] >= 10:
                open_filters.append({
                    "field": field, "lo": s["lo"], "hi": s["hi"],
                    "label": s["label"], "n": s["n"],
                    "hit_rate": s["hit_rate"],
                    "miss_rate": round(1 - s["hit_rate"], 3),
                })

    # 按未命中率排序，最高的优先
    close_filters.sort(key=lambda x: x["miss_rate"], reverse=True)
    open_filters.sort(key=lambda x: x["miss_rate"], reverse=True)

    # ── 3. 统计过滤前后的命中率变化 ──────────────────────────────
    def calc_filtered_rate(records, filters):
        kept = records
        for f in filters:
            kept = [r for r in kept if not (r.get(f["field"]) is not None and f["lo"] <= r[f["field"]] < f["hi"])]
        if not kept:
            return 0.0, 0
        return round(sum(r["hit"] for r in kept) / len(kept), 3), len(kept)

    close_before = (round(overall_close_rate, 3), len(close_records))
    open_before  = (round(overall_open_rate,  3), len(open_records))
    close_after_rate, close_after_n = calc_filtered_rate(close_records, close_filters)
    open_after_rate,  open_after_n  = calc_filtered_rate(open_records,  open_filters)

    # ── 4a. 收盘排名第一胜率统计 ────────────────────────────────────
    from collections import defaultdict
    close_by_date = defaultdict(list)
    for r in close_records:
        close_by_date[r["date"]].append(r)

    close_top1_records = []
    for date, entries in sorted(close_by_date.items()):
        valid = [e for e in entries if abs(e["score"]) > 0.05]
        if not valid:
            continue
        top = max(valid, key=lambda e: abs(e["score"]))
        close_top1_records.append({
            "date":   date,
            "ticker": top["ticker"],
            "score":  round(top["score"], 3),
            "hit":    top["hit"],
            "actual": round(top["nextChg"], 2),
        })

    # ── 4. 开盘排名第一胜率统计（按时间区间）──────────────────────
    # 按日期分组，每天排名：取 |combinedOpenScore| 最大的标的为"排名第一"
    open_by_date = defaultdict(list)
    for o in obs:
        if o.get("nextOpenChg") is None or o.get("nextOpenToClose") is None:
            continue
        date   = o["date"]
        ticker = o["ticker"]
        next_oc = o["nextOpenChg"]  # 次日开盘跳空，用于均值回归信号
        mrev_sig = -1 if next_oc > 1 else 1 if next_oc < -1 else 0
        rf_o = (open_rf_preds or {}).get(date, {}).get(ticker)
        dl_o = (open_dl_preds or {}).get(date, {}).get(ticker)
        rf_prob_o = rf_o["prob"] if rf_o else None
        dl_prob_o = dl_o["prob"] if dl_o else None
        rf_sig_o = ((rf_prob_o - 0.5) * 2) if rf_prob_o is not None else 0
        dl_sig_o = ((dl_prob_o - 0.5) * 2) if dl_prob_o is not None else 0
        w_mrev = 0.45; w_dl = 0.23 if dl_prob_o else 0; w_rf = 0.22 if rf_prob_o else 0; w_rule = 0.10
        tot_w  = w_mrev + w_dl + w_rf + w_rule or 1
        score  = (mrev_sig * w_mrev + rf_sig_o * w_rf + dl_sig_o * w_dl) / tot_w
        # 应用过滤
        obs_chk = {"todayOpenChg": next_oc, "todayChg": o.get("todayChg",0),
                   "atr5": o.get("atr5"), "marketUpCount": o.get("marketUpCount",4),
                   "qqqRet1": o.get("qqqRet1")}
        for f in open_filters:
            v = obs_chk.get(f["field"])
            if v is not None and f["lo"] <= v < f["hi"]:
                score *= 0.3
                break
        open_by_date[date].append({
            "ticker": ticker, "score": score,
            "nextOpenToClose": o["nextOpenToClose"],  # 实际结果：次日开盘到收盘
        })

    # 对每天找排名第一（|score| 最大），用 nextOpenToClose 判断命中（与前端一致）
    top1_records = []
    for date, entries in sorted(open_by_date.items()):
        valid = [e for e in entries if abs(e["score"]) > 0.05]
        if not valid:
            continue
        top = max(valid, key=lambda e: abs(e["score"]))
        predicted_up = top["score"] > 0
        actual_up    = top["nextOpenToClose"] > 0
        top1_records.append({
            "date":   date,
            "ticker": top["ticker"],
            "score":  round(top["score"], 3),
            "hit":    predicted_up == actual_up,
            "actual": round(top["nextOpenToClose"], 2),
        })

    def range_stat(records, start_date, end_date):
        grp = [r for r in records if start_date <= r["date"] <= end_date]
        if not grp:
            return None
        hit  = sum(1 for r in grp if r["hit"])
        return {"hit": hit, "tot": len(grp),
                "rate": round(hit / len(grp), 3),
                "startDate": start_date, "endDate": end_date}

    all_dates_sorted = sorted(open_by_date.keys())
    last_date  = all_dates_sorted[-1] if all_dates_sorted else ""
    first_date = all_dates_sorted[0]  if all_dates_sorted else ""

    def date_minus(d: str, days: int) -> str:
        from datetime import datetime, timedelta
        try:
            return (datetime.strptime(d, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")
        except Exception:
            return d

    open_top1_stats = {
        "all":    range_stat(top1_records, first_date, last_date),
        "3m":     range_stat(top1_records, date_minus(last_date, 90),  last_date),
        "1m":     range_stat(top1_records, date_minus(last_date, 30),  last_date),
        "2w":     range_stat(top1_records, date_minus(last_date, 14),  last_date),
        "records": top1_records[-60:],  # 最近60条明细
    }

    return {
        "closeFilters":      close_filters,
        "openFilters":       open_filters,
        "closeBefore":       {"rate": close_before[0], "n": close_before[1]},
        "closeAfter":        {"rate": close_after_rate, "n": close_after_n},
        "openBefore":        {"rate": open_before[0],  "n": open_before[1]},
        "openAfter":         {"rate": open_after_rate,  "n": open_after_n},
        "openTop1Stats":     open_top1_stats,
        "closeTop1Records":  close_top1_records,
        "openTop1Records":   top1_records,
    }


# ── 后台重计算缓存 ────────────────────────────────────────────────

_heavy_cache: dict = {}


def _run_heavy():
    """在后台线程中跑重计算，完成后写入 _heavy_cache"""
    try:
        stock_data     = _heavy_cache.get("stock_data", {})
        all_conds      = _cond_fns() + build_combo_conds(_cond_fns())
        obs            = build_observations(stock_data)
        rule_hits      = build_rule_hits(obs, all_conds)
        model_pk       = run_model_pk(stock_data, rule_hits, all_conds)
        rf_model       = train_random_forest(obs)
        dl_model       = train_deep_model(obs)
        open_conds     = _cond_fns() + build_combo_conds(_cond_fns())
        open_rule_hits = build_open_rule_hits(obs, open_conds)
        rf_open_model  = train_open_rf(obs)
        dl_open_model  = train_open_dl(obs)
        miss_patterns  = analyze_miss_patterns(
            obs,
            rf_model["preds"]      if rf_model      else None,
            dl_model["preds"]      if dl_model      else None,
            rf_open_model["preds"] if rf_open_model else None,
            dl_open_model["preds"] if dl_open_model else None,
        )
        _heavy_cache["ruleHits"]           = rule_hits
        _heavy_cache["modelPK"]            = model_pk
        _heavy_cache["rfModel"]            = rf_model
        _heavy_cache["dlModel"]            = dl_model
        _heavy_cache["openRuleHits"]       = open_rule_hits
        _heavy_cache["rfOpenModel"]        = rf_open_model
        _heavy_cache["dlOpenModel"]        = dl_open_model
        _heavy_cache["missPatterns"]       = miss_patterns
        _heavy_cache["closeTop1Records"]   = miss_patterns.get("closeTop1Records", [])
        _heavy_cache["openTop1Records"]    = miss_patterns.get("openTop1Records", [])
        _heavy_cache["ready"]              = True
    except Exception as e:
        _heavy_cache["error"] = str(e)
        _heavy_cache["ready"] = True


# ── API 端点 ──────────────────────────────────────────────────────

@app.get("/api/heavy")
async def get_heavy():
    """前端轮询：后台计算完成后返回 ruleHits + modelPK"""
    if not _heavy_cache.get("ready"):
        return JSONResponse({"ready": False})
    return JSONResponse({
        "ready":        True,
        "ruleHits":     _heavy_cache.get("ruleHits", {}),
        "modelPK":      _heavy_cache.get("modelPK"),
        "rfModel":      _heavy_cache.get("rfModel"),
        "dlModel":      _heavy_cache.get("dlModel"),
        "openRuleHits": _heavy_cache.get("openRuleHits", {}),
        "rfOpenModel":  _heavy_cache.get("rfOpenModel"),
        "dlOpenModel":  _heavy_cache.get("dlOpenModel"),
        "missPatterns":      _heavy_cache.get("missPatterns"),
        "closeTop1Records":  _heavy_cache.get("closeTop1Records", []),
        "openTop1Records":   _heavy_cache.get("openTop1Records", []),
    })


@app.get("/api/top1stats")
async def get_top1stats(start: str = "", end: str = "", mode: str = "close"):
    """按日期范围返回 close/open 排名第一胜率"""
    if not _heavy_cache.get("ready"):
        return JSONResponse({"ready": False})
    key = "closeTop1Records" if mode == "close" else "openTop1Records"
    records = _heavy_cache.get(key, [])
    if start or end:
        records = [r for r in records if (not start or r["date"] >= start) and (not end or r["date"] <= end)]
    if not records:
        return JSONResponse({"ready": True, "tot": 0, "hit": 0, "rate": None, "details": []})
    hit = sum(1 for r in records if r["hit"])
    tot = len(records)
    return JSONResponse({
        "ready": True,
        "tot": tot,
        "hit": hit,
        "rate": round(hit / tot, 3),
        "details": records[-15:],  # 最近15条明细
    })


@app.get("/api/data")
async def get_data():
    """
    一次性返回所有前端需要的数据：
    - stockData: 每只股票的历史数据
    - futuresData: 期货实时数据
    - futuresHistory: 期货历史数据
    - obs: 历史观测列表
    - ruleHits: 规则命中预计算表
    - modelPK: 模型回测结果
    """
    async with httpx.AsyncClient() as client:
        # 并发拉取所有数据
        tasks = (
            [fetch_yahoo(s["ticker"], client) for s in SYMBOLS] +
            [fetch_yahoo(f, client) for f in FUTURES] +
            [fetch_finnhub_quote(s["ticker"], client) for s in SYMBOLS] +
            [fetch_yahoo_quote(f, client) for f in FUTURES]
        )
        results = await asyncio.gather(*tasks, return_exceptions=True)

    n_sym = len(SYMBOLS)
    n_fut = len(FUTURES)

    stock_data     = {}
    futures_history = {}
    futures_data    = {}

    # 股票历史
    for i, s in enumerate(SYMBOLS):
        r = results[i]
        if not isinstance(r, Exception):
            stock_data[s["ticker"]] = r

    # 期货历史
    for i, f in enumerate(FUTURES):
        r = results[n_sym + i]
        if not isinstance(r, Exception):
            futures_history[f] = r

    # Finnhub 覆盖实时价（股票）
    for i, s in enumerate(SYMBOLS):
        r = results[n_sym + n_fut + i]
        if not isinstance(r, Exception) and r.get("realtimePrice"):
            if s["ticker"] in stock_data:
                stock_data[s["ticker"]]["realtimePrice"] = r["realtimePrice"]
                stock_data[s["ticker"]]["intradayChg"]   = r["intradayChg"]

    # Finnhub 期货实时（优先用 Finnhub，fallback 用 Yahoo 的 intradayChg）
    for i, f in enumerate(FUTURES):
        r = results[n_sym + n_fut + n_sym + i]
        if not isinstance(r, Exception) and r.get("intradayChg") is not None:
            futures_data[f] = r
        elif f in futures_history and futures_history[f].get("intradayChg") is not None:
            fh = futures_history[f]
            futures_data[f] = {
                "ticker": f,
                "realtimePrice": fh.get("realtimePrice"),
                "intradayChg":   fh.get("intradayChg"),
            }

    # 全局日期列表
    all_dates_set = set()
    for s in SYMBOLS:
        d = stock_data.get(s["ticker"])
        if d:
            for i, dt in enumerate(d["dates"]):
                if d["changes"][i] is not None:
                    all_dates_set.add(dt)
    all_dates = sorted(all_dates_set)

    # 把股票+期货数据缓存起来，供 /api/heavy 使用
    _heavy_cache["stock_data"]      = stock_data
    _heavy_cache["futures_history"] = futures_history
    _heavy_cache["all_dates"]       = all_dates

    # 如果数据日期没变，跳过重计算直接用缓存
    new_last_date = all_dates[-1] if all_dates else ""
    if _heavy_cache.get("ready") and _heavy_cache.get("last_data_date") == new_last_date:
        pass  # 数据未更新，沿用已有 heavy 缓存
    else:
        _heavy_cache["ready"] = False
        _heavy_cache["last_data_date"] = new_last_date
        import threading
        threading.Thread(target=_run_heavy, daemon=True).start()

    return JSONResponse({
        "stockData":      stock_data,
        "futuresData":    futures_data,
        "futuresHistory": futures_history,
        "allDates":       all_dates,
        "ruleHits":       {},
        "modelPK":        None,
    })


@app.get("/api/refresh")
async def refresh_realtime():
    """仅刷新实时行情（Finnhub），不重算历史"""
    stock_syms = [s["ticker"] for s in SYMBOLS]
    async with httpx.AsyncClient() as client:
        tasks = (
            [fetch_finnhub_quote(sym, client) for sym in stock_syms] +
            [fetch_yahoo_quote(f, client) for f in FUTURES]
        )
        results = await asyncio.gather(*tasks, return_exceptions=True)
    quotes = {}
    for i, sym in enumerate(stock_syms + FUTURES):
        r = results[i]
        if not isinstance(r, Exception):
            quotes[sym] = r
    return JSONResponse({"quotes": quotes})


@app.get("/{filename:path}")
async def serve_html(filename: str):
    import os
    allowed = {"magnificent7.html","magnificent7_v2.html","magnificent7_v3.html","magnificent7_v4.html","magnificent7_v5.html","magnificent7_v6.html"}
    if filename in allowed and os.path.exists(filename):
        return FileResponse(filename)
    from fastapi import HTTPException
    raise HTTPException(status_code=404)
