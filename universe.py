"""
universe.py — Dynamisk bevakningspool för TrendRadar.

get_omx_large_mid_cap()  → OMX Stockholm Large+Mid Cap (~180 aktier)
get_sp500()               → S&P 500 (~503 aktier)
get_full_universe()       → Kombinerad lista med metadata
apply_prefilter(universe) → Filtrerade aktier (1-månads momentum + volym)

All data cachas lokalt (7 dagar för listor, 1 dag för priser).
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

CACHE_DIR    = Path(__file__).parent / "cache"
OMX_CACHE    = CACHE_DIR / "omx_universe.json"
SP500_CACHE  = CACHE_DIR / "sp500.json"
FILTER_CACHE = CACHE_DIR / "filtered_universe.json"
RS_CACHE     = CACHE_DIR / "universe_rs.json"

CACHE_7D = 7  * 24 * 3600   # listcache
CACHE_1D = 1  * 24 * 3600   # priscache i förfilter

# ── OMX Large+Mid Cap — känd lista (källa: Nasdaq Nordic, uppdaterad 2025) ──
_OMX_LARGE_CAP = [
    "ABB.ST", "ALFA.ST", "ALIV-SDB.ST", "ASSA-B.ST", "ATCO-A.ST", "ATCO-B.ST",
    "AXFO.ST", "BEIJ-B.ST", "BETS-B.ST", "BOL.ST", "CAST.ST", "ELUX-B.ST",
    "EQT.ST", "ERIC-A.ST", "ERIC-B.ST", "ESSITY-A.ST", "ESSITY-B.ST", "FABG.ST",
    "GETI-B.ST", "HEXA-B.ST", "HM-B.ST", "HUSQ-A.ST", "HUSQ-B.ST",
    "INDU-A.ST", "INDU-C.ST", "INVE-A.ST", "INVE-B.ST", "KINV-A.ST", "KINV-B.ST",
    "LATO-B.ST", "LEVI.ST", "LIFCO-B.ST", "LUND-B.ST", "MTG-A.ST", "MTG-B.ST",
    "NCC-A.ST", "NCC-B.ST", "NDA-SE.ST", "NIBE-B.ST", "NOBIA.ST",
    "RATO-B.ST", "SAAB-B.ST", "SAND.ST", "SCA-A.ST", "SCA-B.ST",
    "SEB-A.ST", "SEB-C.ST", "SECU-B.ST", "SHB-A.ST", "SHB-B.ST",
    "SINCH.ST", "SKA-B.ST", "SKF-A.ST", "SKF-B.ST", "SOBI.ST",
    "SSAB-A.ST", "SSAB-B.ST", "SWED-A.ST", "TEL2-B.ST", "TELIA.ST",
    "THULE.ST", "TOBII.ST", "VOLV-A.ST", "VOLV-B.ST",
]

_OMX_MID_CAP = [
    "ADDV-B.ST", "AFG.ST", "AMBEA.ST", "ARCM.ST", "AXIC-A.ST",
    "BALD-B.ST", "BILI-A.ST", "BIOG-B.ST", "BTS-B.ST", "BUFAB.ST",
    "CARL-B.ST", "CLAS-B.ST", "COOR.ST", "DIOS.ST", "DUNI.ST",
    "ENEA.ST", "EPRO-B.ST", "FAGC.ST", "FPAR-A.ST", "GARO.ST",
    "HANZA.ST", "HEMF.ST", "HOLM-B.ST", "HPOL-B.ST",
    "IAR-B.ST", "INTRUM.ST", "ITAB.ST", "JM.ST",
    "LAGR-B.ST", "LUMI.ST", "MAHA-A.ST", "NEOBO.ST",
    "NILR-B.ST", "NOTE.ST", "OEM-B.ST", "ONPE.ST",
    "PEAB-B.ST", "PLED.ST", "PRFO.ST", "PRIC-B.ST",
    "SAGAX-A.ST", "SAGAX-B.ST", "SBBB-B.ST", "SDIP-B.ST", "SDIP-D.ST",
    "SWOL-B.ST", "SYSR.ST", "TRAD-B.ST", "VICT-B.ST",
    "VIT-B.ST", "WALL-B.ST", "WIHL.ST", "XANO-B.ST",
]

# ── Cache-hjälpare ─────────────────────────────────────────────────────────────

def _load(path: Path) -> dict | None:
    try:
        if path.exists():
            obj = json.loads(path.read_text())
            if time.time() - obj.get("ts", 0) < obj.get("ttl", CACHE_7D):
                return obj["data"]
    except Exception:
        pass
    return None


def _save(path: Path, data, ttl: int = CACHE_7D) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"ts": time.time(), "ttl": ttl, "data": data}))
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# 1. OMX Large+Mid Cap
# ═══════════════════════════════════════════════════════════════════════════════

def get_omx_large_mid_cap() -> list[dict]:
    """
    Returnerar OMX Stockholm Large+Mid Cap med metadata.
    Primär: Wikipedia-skrapning. Fallback: inbyggd lista (~115 aktier).
    Cacheas 7 dagar.
    """
    cached = _load(OMX_CACHE)
    if cached:
        return cached

    result: list[dict] = []

    # ── Försök 1: Wikipedia ───────────────────────────────────────────────────
    for url, cap in [
        ("https://en.wikipedia.org/wiki/OMX_Stockholm_Large_Cap", "Large Cap"),
        ("https://en.wikipedia.org/wiki/OMX_Stockholm_Mid_Cap",   "Mid Cap"),
    ]:
        try:
            tables = pd.read_html(url)
            for tbl in tables:
                cols_lower = {c.lower().strip(): c for c in tbl.columns}
                ticker_col = next(
                    (cols_lower[k] for k in ["ticker", "symbol", "kortnamn", "short name"]
                     if k in cols_lower), None
                )
                name_col = next(
                    (cols_lower[k] for k in ["company", "bolag", "name", "företag"]
                     if k in cols_lower), None
                )
                if ticker_col is None:
                    continue
                for _, row in tbl.iterrows():
                    raw = str(row[ticker_col]).strip()
                    if not raw or raw.lower() in ("nan", "-"):
                        continue
                    # Konvertera till yfinance-format: mellanslag → dash, lägg till .ST
                    tkr = raw.replace(" ", "-").upper()
                    if not tkr.endswith(".ST"):
                        tkr += ".ST"
                    name = str(row[name_col]).strip() if name_col else tkr
                    result.append({"ticker": tkr, "name": name,
                                   "region": "Sverige", "cap": cap})
        except Exception:
            pass

    # ── Fallback: inbyggd lista ────────────────────────────────────────────────
    if len(result) < 30:
        result = []
        for tkr in _OMX_LARGE_CAP:
            result.append({"ticker": tkr, "name": tkr.replace(".ST", ""),
                            "region": "Sverige", "cap": "Large Cap"})
        for tkr in _OMX_MID_CAP:
            result.append({"ticker": tkr, "name": tkr.replace(".ST", ""),
                            "region": "Sverige", "cap": "Mid Cap"})

    # Ta bort dubbletter
    seen: set[str] = set()
    unique = []
    for item in result:
        if item["ticker"] not in seen:
            seen.add(item["ticker"])
            unique.append(item)

    _save(OMX_CACHE, unique)
    return unique


# ═══════════════════════════════════════════════════════════════════════════════
# 2. S&P 500
# ═══════════════════════════════════════════════════════════════════════════════

_WIKI_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def get_sp500() -> list[dict]:
    """
    Hämtar S&P 500-listan från Wikipedia. Cacheas 7 dagar.
    Använder browser-headers för att undvika 403.
    """
    cached = _load(SP500_CACHE)
    if cached:
        return cached

    result: list[dict] = []
    try:
        import requests
        from io import StringIO
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        r   = requests.get(url, headers=_WIKI_HEADERS, timeout=10)
        r.raise_for_status()
        tbl = pd.read_html(StringIO(r.text))[0]
        for _, row in tbl.iterrows():
            raw     = str(row.get("Symbol", row.iloc[0])).strip()
            tkr     = raw.replace(".", "-").upper()   # BF.B → BF-B
            name    = str(row.get("Security", "")).strip()
            sector  = str(row.get("GICS Sector", "")).strip()
            result.append({"ticker": tkr, "name": name,
                            "region": "USA", "sector": sector, "cap": "Large Cap"})
    except Exception as e:
        print(f"  [universe] S&P 500 Wikipedia-fel: {e}")

    _save(SP500_CACHE, result)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Kombinerad universum
# ═══════════════════════════════════════════════════════════════════════════════

def get_full_universe() -> dict[str, dict]:
    """Kombinerar OMX + S&P 500 till en dict {ticker: metadata}."""
    universe: dict[str, dict] = {}
    for item in get_omx_large_mid_cap():
        universe[item["ticker"]] = item
    for item in get_sp500():
        universe[item["ticker"]] = item
    return universe


# ═══════════════════════════════════════════════════════════════════════════════
# 4. 30-dagars förfilter
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_series(raw, ticker: str, col: str, n_tickers: int):
    """Extraherar en kolumn ur multi-ticker yfinance-resultat."""
    try:
        if n_tickers == 1:
            s = raw[col]
        else:
            lvl0 = raw.columns.get_level_values(0)
            if ticker not in lvl0:
                return None
            s = raw[ticker][col]
        return (s.iloc[:, 0] if isinstance(s, pd.DataFrame) else s).dropna()
    except Exception:
        return None


# Hemmaindex per region — RS jämförs mot eget index, inte alltid OMXS30
_HOME_BENCHMARKS: dict[str, str] = {
    "Sverige": "^OMX",       # OMXS30
    "USA":     "^GSPC",      # S&P 500
    "Europa":  "^STOXX50E",  # Euro Stoxx 50
}


def _fetch_benchmark_3m(symbols: list[str], period: str = "6mo") -> dict[str, float]:
    """Hämtar 3M-avkastning (63 handelsdagar) för en lista index-symboler."""
    result: dict[str, float] = {}
    for sym in symbols:
        try:
            df = yf.download(sym, period=period, auto_adjust=True, progress=False)
            c  = df["Close"]
            if isinstance(c, pd.DataFrame): c = c.iloc[:, 0]
            c  = c.dropna()
            if len(c) >= 64:
                result[sym] = float(c.iloc[-1] / c.iloc[-64] - 1) * 100
        except Exception:
            pass
    return result


def apply_prefilter(universe: dict[str, dict],
                    top_rs_n: int = 20) -> tuple[dict[str, dict], list[dict]]:
    """
    ETT batch-nedladdningspass (6 månader) som:
      1. Filtrerar universum: behåll om (a) 1M avkastning > 0% ELLER (b) volym > median
      2. Beräknar RS (3M vs EGET hemmaindex) per region
      3. Returnerar (filtered_dict, top_rs_n_sorted_by_rs)

    Cacheas 1 dag. Returvärde: (filtered, rs_top20)
    """
    # ── Kontrollera cache ─────────────────────────────────────────────────────
    f_cached  = _load(FILTER_CACHE)
    rs_cached = _load(RS_CACHE)
    if f_cached is not None and rs_cached is not None:
        filtered = {tkr: universe[tkr] for tkr in f_cached if tkr in universe}
        return filtered, rs_cached[:top_rs_n]

    tickers = list(universe.keys())
    CHUNK   = 100
    PERIOD  = "6mo"   # Täcker both filter (1M) och RS (3M=63d)

    # ── Hämta regionspecifika benchmarks ─────────────────────────────────────
    bench_syms = list(_HOME_BENCHMARKS.values())
    bench_3m   = _fetch_benchmark_3m(bench_syms)
    # bench_3m exempel: {"^OMX": 2.9, "^GSPC": 3.6, "^STOXX50E": 1.2}
    print(f"  [universe] Benchmark 3M: " +
          "  ".join(f"{s}={bench_3m.get(s, 0):.1f}%" for s in bench_syms))

    # ── Batch-nedladdning ─────────────────────────────────────────────────────
    close_map:  dict[str, pd.Series] = {}
    volume_map: dict[str, pd.Series] = {}

    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i : i + CHUNK]
        try:
            raw = yf.download(chunk, period=PERIOD, auto_adjust=True,
                              progress=False, group_by="ticker")
            n = len(chunk)
            for tkr in chunk:
                c = _extract_series(raw, tkr, "Close",  n)
                v = _extract_series(raw, tkr, "Volume", n)
                if c is not None: close_map[tkr]  = c
                if v is not None: volume_map[tkr] = v
        except Exception as e:
            print(f"  [universe] Batch-fel chunk {i}: {e}")

    # ── Volym-median per marknad ───────────────────────────────────────────────
    swe_vols = [float(v.iloc[-5:].mean()) for t, v in volume_map.items()
                if universe.get(t, {}).get("region") == "Sverige" and len(v) >= 5]
    usa_vols = [float(v.iloc[-5:].mean()) for t, v in volume_map.items()
                if universe.get(t, {}).get("region") == "USA" and len(v) >= 5]
    med_swe  = float(np.median(swe_vols)) if swe_vols else 0.0
    med_usa  = float(np.median(usa_vols)) if usa_vols else 0.0

    # ── Filterkriterer + RS-beräkning i ett pass ──────────────────────────────
    N_MIN = 21    # ≈ 1 månad
    N_3M  = 63    # ≈ 3 månader

    passed:  list[str]  = []
    rs_all:  list[dict] = []

    for tkr, close in close_map.items():
        if len(close) < N_MIN:
            continue
        try:
            ret_1m = float(close.iloc[-1] / close.iloc[-N_MIN] - 1)
            ok = ret_1m > 0

            if not ok:
                vols = volume_map.get(tkr)
                if vols is not None and len(vols) >= 5:
                    vol_5d = float(vols.iloc[-5:].mean())
                    region = universe.get(tkr, {}).get("region", "")
                    median = med_swe if region == "Sverige" else med_usa
                    ok = (median > 0 and vol_5d > median)

            if ok:
                passed.append(tkr)
                # Beräkna RS vs eget hemmaindex om vi har tillräckligt med data
                if len(close) >= N_3M + 1:
                    region     = universe.get(tkr, {}).get("region", "USA")
                    bench_sym  = _HOME_BENCHMARKS.get(region, "^GSPC")
                    bench_ret  = bench_3m.get(bench_sym, 0.0)
                    ret_3m     = float(close.iloc[-1] / close.iloc[-N_3M - 1] - 1) * 100
                    rs_all.append({
                        "ticker":    tkr,
                        "name":      universe[tkr].get("name", tkr),
                        "region":    region,
                        "price":     round(float(close.iloc[-1]), 2),
                        "rs_pct":    round(ret_3m - bench_ret, 1),  # vs hemmaindex
                        "ret_1m":    round(ret_1m * 100, 1),
                        "ret_3m":    round(ret_3m, 1),
                        "benchmark": bench_sym,
                    })
        except Exception:
            pass

    rs_all.sort(key=lambda x: x["rs_pct"], reverse=True)

    _save(FILTER_CACHE, passed,  ttl=CACHE_1D)
    _save(RS_CACHE,     rs_all,  ttl=CACHE_1D)

    filtered = {tkr: universe[tkr] for tkr in passed if tkr in universe}
    return filtered, rs_all[:top_rs_n]


# ═══════════════════════════════════════════════════════════════════════════════
# Standalone test
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    def _timed(label: str):
        class _T:
            def __enter__(self): self.t = time.time(); return self
            def __exit__(self, *_): print(f"  [{label}] {time.time()-self.t:.1f}s")
        return _T()

    print("=== TrendRadar Universe ===\n")

    with _timed("OMX Large+Mid Cap"):
        omx = get_omx_large_mid_cap()
    print(f"  OMX-aktier: {len(omx)}\n")

    with _timed("S&P 500"):
        sp5 = get_sp500()
    print(f"  S&P 500-aktier: {len(sp5)}\n")

    with _timed("Full universum"):
        uni = get_full_universe()
    print(f"  Total universum: {len(uni)} aktier\n")

    with _timed("Förfilter (batch-nedladdning)"):
        fil = apply_prefilter(uni)
    print(f"  Efter förfilter: {len(fil)} aktier")
    print(f"  Filtreringskvot: {len(fil)/len(uni)*100:.0f}% godkänns\n")

    print(f"Sammanfattning: {len(uni)} → {len(fil)} aktier")
