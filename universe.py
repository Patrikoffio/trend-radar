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

def apply_prefilter(universe: dict[str, dict]) -> dict[str, dict]:
    """
    Filtrerar universum på MINST ETT kriterium:
      (a) 1-månads avkastning > 0%   (positiv momentum)
      (b) Volym de senaste 5d > median-volym för sin marknad

    Priser hämtas med batch-nedladdning i grupper om 100 och cachas 1 dag.
    """
    cached = _load(FILTER_CACHE)
    if cached:
        return {tkr: universe[tkr] for tkr in cached if tkr in universe}

    tickers = list(universe.keys())
    CHUNK   = 100
    PERIOD  = "3mo"

    # ── Batch-nedladdning ─────────────────────────────────────────────────────
    close_map:  dict[str, pd.Series] = {}
    volume_map: dict[str, pd.Series] = {}

    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i : i + CHUNK]
        try:
            raw = yf.download(
                chunk, period=PERIOD, auto_adjust=True,
                progress=False, group_by="ticker",
            )
            for tkr in chunk:
                try:
                    if len(chunk) == 1:
                        c = raw["Close"]
                        v = raw["Volume"]
                    else:
                        c = raw[tkr]["Close"] if tkr in raw.columns.get_level_values(0) else None
                        v = raw[tkr]["Volume"] if tkr in raw.columns.get_level_values(0) else None
                    if c is None:
                        continue
                    c = c.iloc[:, 0] if isinstance(c, pd.DataFrame) else c
                    v = v.iloc[:, 0] if isinstance(v, pd.DataFrame) else v
                    close_map[tkr]  = c.dropna()
                    volume_map[tkr] = v.dropna()
                except Exception:
                    pass
        except Exception as e:
            print(f"  [universe] Batch-fel {i}-{i+CHUNK}: {e}")

    # ── Volym-median per marknad ───────────────────────────────────────────────
    swe_vols = [float(v.iloc[-5:].mean()) for t, v in volume_map.items()
                if universe.get(t, {}).get("region") == "Sverige" and len(v) >= 5]
    usa_vols = [float(v.iloc[-5:].mean()) for t, v in volume_map.items()
                if universe.get(t, {}).get("region") == "USA" and len(v) >= 5]
    med_swe  = float(np.median(swe_vols)) if swe_vols else 0
    med_usa  = float(np.median(usa_vols)) if usa_vols else 0

    # ── Applicera kriterier ───────────────────────────────────────────────────
    passed: list[str] = []
    N_MIN  = 21   # ≈ 1 månad handelsdagar

    for tkr, close in close_map.items():
        if len(close) < N_MIN:
            continue
        try:
            # (a) 1-månads avkastning > 0%
            ret_1m = float(close.iloc[-1] / close.iloc[-N_MIN] - 1)
            if ret_1m > 0:
                passed.append(tkr)
                continue

            # (b) Volym de senaste 5d > marknadsmedian
            vols = volume_map.get(tkr)
            if vols is not None and len(vols) >= 5:
                vol_5d = float(vols.iloc[-5:].mean())
                region = universe.get(tkr, {}).get("region", "")
                median = med_swe if region == "Sverige" else med_usa
                if median > 0 and vol_5d > median:
                    passed.append(tkr)
        except Exception:
            pass

    _save(FILTER_CACHE, passed, ttl=CACHE_1D)
    return {tkr: universe[tkr] for tkr in passed if tkr in universe}


# ═══════════════════════════════════════════════════════════════════════════════
# Hämta RS för ett universum (3M avkastning vs OMXS30)
# ═══════════════════════════════════════════════════════════════════════════════

def get_universe_rs(filtered: dict[str, dict],
                    top_n: int = 20) -> list[dict]:
    """
    Beräknar relativ styrka (3M) för alla aktier i filtered vs OMXS30.
    Returnerar top_n aktier sorterade fallande på RS.
    Batch-nedladdning för hastighet.
    """
    tickers = list(filtered.keys())
    CHUNK   = 100

    # Hämta OMXS30 som benchmark
    try:
        omx = yf.download("^OMX", period="6mo", auto_adjust=True, progress=False)
        omx_c = omx["Close"]
        if isinstance(omx_c, pd.DataFrame): omx_c = omx_c.iloc[:, 0]
        omx_c = omx_c.dropna()
        n63   = min(63, len(omx_c) - 1)
        omx_3m = float(omx_c.iloc[-1] / omx_c.iloc[-n63 - 1] - 1) * 100
    except Exception:
        omx_3m = 0.0

    rs_list: list[dict] = []
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i : i + CHUNK]
        try:
            raw = yf.download(
                chunk, period="6mo", auto_adjust=True,
                progress=False, group_by="ticker",
            )
            for tkr in chunk:
                try:
                    c = raw[tkr]["Close"] if len(chunk) > 1 else raw["Close"]
                    if isinstance(c, pd.DataFrame): c = c.iloc[:, 0]
                    c = c.dropna()
                    if len(c) < 64:
                        continue
                    ret_3m = float(c.iloc[-1] / c.iloc[-64] - 1) * 100
                    rs = ret_3m - omx_3m
                    rs_list.append({
                        "ticker":   tkr,
                        "name":     filtered[tkr].get("name", tkr),
                        "region":   filtered[tkr].get("region", ""),
                        "price":    float(c.iloc[-1]),
                        "rs_pct":   round(rs, 1),
                        "ret_3m":   round(ret_3m, 1),
                    })
                except Exception:
                    pass
        except Exception:
            pass

    rs_list.sort(key=lambda x: x["rs_pct"], reverse=True)
    return rs_list[:top_n]


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
