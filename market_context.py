"""
Marknadskontext — tre komponenter för veckorapporten.

  get_regime()          → Marknadsregim 1–5
  get_fear_greed()      → CNN Fear & Greed med fallback
  get_forward_estimate() → 12-månadersprognos via Monte Carlo

Data cachas lokalt i 6 timmar (.market_cache.json) för att undvika
onödig nätverkstrafik mot CNN/yfinance/multpl.
"""

from __future__ import annotations

import json
import math
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

# ─── Konstanter ───────────────────────────────────────────────────────────────
CACHE_FILE = Path(__file__).parent / ".market_cache.json"
CACHE_TTL  = 6 * 3600   # sekunder

TICKERS_UNIVERSE = [
    "VOLV-B.ST", "ATCO-A.ST", "SAND.ST", "HEXA-B.ST", "INVE-B.ST", "ERIC-B.ST",
    "NVDA", "MSFT", "AAPL", "BRK-B",
    "ASML", "NOVO-B.CO", "MC.PA", "SAP",
]
SECTOR_ETFS = ["XLK", "XLI", "XLF", "XLV", "XLY"]

REGIME_LABELS = {5: "KÖR PÅ", 4: "POSITIVT", 3: "NEUTRAL", 2: "FÖRSIKTIGT", 1: "DEFENSIVT"}
REGIME_DEFAULTS = {5: 0.12, 4: 0.09, 3: 0.06, 2: 0.02, 1: -0.03}

# ─── Cache-hjälpare ───────────────────────────────────────────────────────────

def _load_cache() -> dict:
    try:
        if CACHE_FILE.exists():
            return json.loads(CACHE_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_cache(cache: dict) -> None:
    try:
        CACHE_FILE.write_text(json.dumps(cache, default=str))
    except Exception:
        pass


def _cache_get(key: str, cache: dict):
    entry = cache.get(key)
    if entry and (time.time() - entry.get("ts", 0)) < CACHE_TTL:
        return entry["v"]
    return None


def _cache_set(key: str, value, cache: dict) -> None:
    cache[key] = {"ts": time.time(), "v": value}


# ─── yfinance-hjälpare ────────────────────────────────────────────────────────

def _close(ticker: str, period: str = "14mo") -> pd.Series | None:
    """Hämtar stängningspriser för en ticker. Returnerar None vid fel."""
    try:
        df = yf.download(ticker, period=period, auto_adjust=True, progress=False)
        if df.empty:
            return None
        c = df["Close"]
        if isinstance(c, pd.DataFrame):
            c = c.iloc[:, 0]
        return c.dropna()
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# 1. MARKNADSREGIM
# ═══════════════════════════════════════════════════════════════════════════════

def _trend_regime(close: pd.Series | None) -> tuple[int, str, float]:
    """OMXS30 vs MA200 — TJUR / NEUTRAL / BJÖRN."""
    if close is None or len(close) < 200:
        return 2, "NEUTRAL", 0.0
    ma200 = float(close.rolling(200).mean().iloc[-1])
    pct   = (float(close.iloc[-1]) - ma200) / ma200 * 100
    if pct > 5:
        return 3, "TJUR", pct
    elif pct < -5:
        return 1, "BJÖRN", pct
    return 2, "NEUTRAL", pct


def _vol_regime(vix: pd.Series | None) -> tuple[int, str, float, str]:
    """VIX-nivå — LUGN / NORMAL / HÖGT / EXTREMT."""
    if vix is None or len(vix) < 2:
        return 2, "NORMAL", 20.0, "okänd"
    current = float(vix.iloc[-1])
    past    = float(vix.iloc[-min(20, len(vix)-1)])
    if current > past * 1.05:
        trend = "stigande"
    elif current < past * 0.95:
        trend = "fallande"
    else:
        trend = "stabil"
    if current < 15:
        return 3, "LUGN",   current, trend
    elif current < 22:
        return 3, "NORMAL", current, trend
    elif current < 30:
        return 2, "HÖGT",   current, trend
    return 1, "EXTREMT", current, trend


def _breadth_regime(all_signals: list[dict] | None) -> tuple[int, str, float]:
    """Andel aktier med MA50 > MA200."""
    if all_signals:
        valid = [s for s in all_signals if s.get("sma50") and s.get("sma200")]
        if valid:
            above = sum(1 for s in valid if s["sma50"] > s["sma200"])
            pct   = above / len(valid) * 100
        else:
            pct = 50.0
    else:
        above_n, total = 0, 0
        for tkr in TICKERS_UNIVERSE:
            c = _close(tkr)
            if c is None or len(c) < 200:
                continue
            total += 1
            if float(c.rolling(50).mean().iloc[-1]) > float(c.rolling(200).mean().iloc[-1]):
                above_n += 1
        pct = above_n / total * 100 if total else 50.0

    if pct > 60:
        return 3, "BRED",   pct
    elif pct > 40:
        return 2, "NORMAL", pct
    return 1, "SMAL", pct


def _sector_spread_regime(etf_returns: dict[str, float]) -> tuple[int, str, float]:
    """Spridning (max–min) mellan sektor-ETF:ernas 3M-avkastning."""
    if not etf_returns or len(etf_returns) < 2:
        return 2, "NORMAL", 15.0
    spread = max(etf_returns.values()) - min(etf_returns.values())
    if spread < 10:
        return 3, "JÄMNT",  spread
    elif spread < 25:
        return 2, "NORMAL", spread
    return 1, "EXTREM", spread


def get_regime(all_signals: list[dict] | None = None) -> dict:
    """
    Beräknar marknadsregim från fyra lager.
    Returnerar dict med score (1–5), label, summary_text och detaljinfo per lager.
    """
    cache  = _load_cache()
    cached = _cache_get("regime", cache)
    if cached:
        return cached

    omx = _close("^OMX", "14mo")
    vix = _close("^VIX", "3mo")

    # Sektor-ETF 3M-avkastningar
    etf_returns: dict[str, float] = {}
    for etf in SECTOR_ETFS:
        c = _close(etf, "6mo")
        if c is not None and len(c) >= 64:
            etf_returns[etf] = float(c.iloc[-1] / c.iloc[-64] - 1) * 100

    t_sc, t_st, t_val            = _trend_regime(omx)
    v_sc, v_st, v_val, v_trend   = _vol_regime(vix)
    b_sc, b_st, b_val            = _breadth_regime(all_signals)
    s_sc, s_st, s_val            = _sector_spread_regime(etf_returns)

    weighted = t_sc*0.30 + v_sc*0.25 + b_sc*0.25 + s_sc*0.20

    if weighted >= 2.8:     score = 5
    elif weighted >= 2.4:   score = 4
    elif weighted >= 1.8:   score = 3
    elif weighted >= 1.3:   score = 2
    else:                   score = 1

    # Åsidosättning: björnmarknad ELLER extrem volatilitet → defensivt
    if t_st == "BJÖRN" or v_st == "EXTREMT":
        score = 1

    summaries = {
        5: "Bred uppgång med låg volatilitet och jämn sektorstyrka.",
        4: "Mestadels positiva signaler — 3 av 4 lager stödjer.",
        3: "Blandade signaler — var selektiv med positionsstorlek.",
        2: "Flera varningssignaler — håll lägre riskexponering.",
        1: "Defensivt läge — kapitalskydd prioriteras.",
    }

    result = {
        "score":        score,
        "label":        REGIME_LABELS[score],
        "summary_text": summaries[score],
        "trend":      {"status": t_st, "value": round(t_val, 1),
                       "comment": f"OMXS30 {t_val:+.1f}% vs MA200"},
        "volatility": {"status": v_st, "value": round(v_val, 1),
                       "comment": f"VIX {v_val:.1f}, {v_trend}"},
        "breadth":    {"status": b_st, "value": round(b_val, 1),
                       "comment": f"{b_val:.0f}% av aktier MA50>MA200"},
        "sectors":    {"status": s_st, "value": round(s_val, 1),
                       "comment": f"Spridning {s_val:.1f}pp"},
    }

    _cache_set("regime", result, cache)
    _save_cache(cache)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 2. FEAR & GREED INDEX
# ═══════════════════════════════════════════════════════════════════════════════

def _fg_category(v: float) -> str:
    if v < 20:   return "Extrem rädsla"
    elif v < 40: return "Rädsla"
    elif v < 60: return "Neutral"
    elif v < 80: return "Girighet"
    return "Extrem girighet"


def _fg_signal(v: float) -> str:
    if v < 20:   return "KÖPLÄGE"
    elif v < 40: return "OBSERVATIONSLÄGE"
    elif v < 60: return "NEUTRAL"
    elif v < 80: return "VARSAMHET"
    return "ÖVERHETTAT"


def _cnn_api() -> dict | None:
    """Hämtar Fear & Greed direkt från CNN:s dataviz-API."""
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    try:
        r = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0 TrendRadar/1.0"})
        r.raise_for_status()
        data = r.json()
        current = data["fear_and_greed"]
        value   = float(current["score"])

        change_7d = None
        hist = data.get("fear_and_greed_historical", {}).get("data", [])
        if len(hist) >= 8:
            try:
                change_7d = value - float(hist[-8]["y"])
            except Exception:
                pass

        return {"value": value, "change_7d": change_7d, "source": "CNN"}
    except Exception:
        return None


def _vix_proxy() -> dict:
    """Proxy-beräkning via VIX och S&P500 125-dagars momentum."""
    vix_c = _close("^VIX", "1mo")
    spx_c = _close("^GSPC", "8mo")

    vix = float(vix_c.iloc[-1]) if vix_c is not None and len(vix_c) else 20.0
    vix_score = max(5.0, min(95.0, 100 - (vix - 10) * 3))

    mom_score = 50.0
    if spx_c is not None and len(spx_c) >= 126:
        ret = (float(spx_c.iloc[-1]) / float(spx_c.iloc[-126]) - 1) * 100
        mom_score = max(5.0, min(95.0, 50 + ret * 1.5))

    return {"value": 0.6 * vix_score + 0.4 * mom_score, "change_7d": None, "source": "Proxy (VIX + S&P500)"}


def get_fear_greed() -> dict:
    """Returnerar Fear & Greed-dict. Primär källa CNN, fallback proxy."""
    cache  = _load_cache()
    cached = _cache_get("fear_greed", cache)
    if cached:
        return cached

    raw   = _cnn_api() or _vix_proxy()
    value = float(raw["value"])

    result = {
        "value":     round(value, 1),
        "category":  _fg_category(value),
        "signal":    _fg_signal(value),
        "change_7d": round(raw["change_7d"], 1) if raw.get("change_7d") is not None else None,
        "source":    raw["source"],
    }

    _cache_set("fear_greed", result, cache)
    _save_cache(cache)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 3. 12-MÅNADERS PROGNOS
# ═══════════════════════════════════════════════════════════════════════════════

def _scrape_cape() -> float | None:
    """Skrapar Shiller CAPE från multpl.com/shiller-pe."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; TrendRadar/1.0)"}
        r = requests.get("https://multpl.com/shiller-pe", headers=headers, timeout=8)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Försök 1: div#current
        div = soup.find(id="current")
        if div:
            m = re.search(r"(\d{2}\.\d{2})", div.get_text())
            if m:
                v = float(m.group(1))
                if 10 <= v <= 60:
                    return v

        # Försök 2: sök i hela sidans text efter plausibelt CAPE
        page_text = soup.get_text(separator=" ")[:3000]
        for m in re.findall(r"\b(\d{2}\.\d{2})\b", page_text):
            v = float(m)
            if 15 <= v <= 55:
                return v
    except Exception:
        pass
    return None


def _cape_factor(cape: float) -> float:
    """
    CAPE-baserad förväntad avkastning: 0.07 − 0.5 × ln(CAPE/17).
    Clampas till [−15%, +12%] för rimliga Monte Carlo-driftar.
    """
    raw = 0.07 - 0.5 * math.log(max(cape, 1) / 17.0)
    return max(-0.15, min(0.12, raw))


def _rate_factor() -> tuple[float, float, float]:
    """10-årig ränta (^TNX proxy). Returnerar (faktor, nuläge, 12M-förändring)."""
    try:
        c = _close("^TNX", "15mo")
        if c is None or len(c) < 252:
            return 0.0, 4.5, 0.0
        current = float(c.iloc[-1])
        past    = float(c.iloc[-252])
        change  = current - past
        if change > 1.0:
            factor = -(0.02 + min(change - 1.0, 1.0) * 0.02)   # −2% till −4%
        else:
            factor = 0.0
        return factor, current, change
    except Exception:
        return 0.0, 4.5, 0.0


def _monte_carlo(drift: float, vol: float, n: int = 10_000, horizon: int = 252) -> np.ndarray:
    """GBM med n simuleringar, daglig steglängd, returnerar terminalavkastning."""
    dt = 1.0 / 252
    log_drift = (drift - 0.5 * vol**2) * dt
    log_vol   = vol * math.sqrt(dt)
    steps = np.random.normal(log_drift, log_vol, size=(n, horizon))
    return np.expm1(steps.sum(axis=1))   # terminal procentavkastning som decimal


def get_forward_estimate(regime: dict | None = None) -> dict:
    """
    Beräknar 12-månadersprognos.
    Faktorer: CAPE, ränteläge, marknadsregim.
    Returnerar percentiler, konfidensintervall och faktorlista.
    """
    cache  = _load_cache()
    cached = _cache_get("forecast", cache)
    if cached:
        return cached

    # ── Faktorer ──────────────────────────────────────────────────────────────
    cape = _scrape_cape() or 28.0
    cf   = _cape_factor(cape)

    rf, rate_current, rate_change = _rate_factor()

    r_score  = regime["score"] if regime else 3
    reg_ret  = REGIME_DEFAULTS[r_score]

    drift = (cf + rf + reg_ret) / 3.0

    # ── Volatilitet (OMXS30 60d realiserad) ──────────────────────────────────
    omx = _close("^OMX", "6mo")
    if omx is not None and len(omx) >= 60:
        vol = float(omx.pct_change().dropna().iloc[-60:].std() * math.sqrt(252))
    else:
        vol = 0.18

    # ── Monte Carlo ───────────────────────────────────────────────────────────
    np.random.seed(42)
    terminal = _monte_carlo(drift, vol) * 100   # i procent

    result = {
        "bear_case":       round(float(np.percentile(terminal, 10)),   1),
        "base_case":       round(float(np.percentile(terminal, 50)),   1),
        "bull_case":       round(float(np.percentile(terminal, 90)),   1),
        "ci_68":          (round(float(np.percentile(terminal, 16)),   1),
                           round(float(np.percentile(terminal, 84)),   1)),
        "ci_95":          (round(float(np.percentile(terminal, 2.5)),  1),
                           round(float(np.percentile(terminal, 97.5)), 1)),
        "expected_return": round(float(np.mean(terminal)), 1),
        "std_dev":         round(float(np.std(terminal)),  1),
        "factors": {
            "CAPE":          round(cf * 100,      1),
            "Ränteläge":     round(rf * 100,      1),
            "Marknadsregim": round(reg_ret * 100, 1),
        },
        "inputs": {
            "cape":         round(cape, 1),
            "rate_current": round(rate_current, 2),
            "rate_change":  round(rate_change, 2),
            "volatility":   round(vol * 100, 1),
            "regime_score": r_score,
        },
        "honesty_warning": (
            "Prognoser är aldrig säkra. Simuleringen bygger på historiska antaganden "
            "och kan inte förutse marknadsomvälvningar. Använd som riktlinje, inte som sanning."
        ),
    }

    _cache_set("forecast", result, cache)
    _save_cache(cache)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# STANDALONE TEST
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=== TrendRadar Marknadskontext ===\n")

    print("Hämtar marknadsregim...")
    regime = get_regime()
    print(f"  Betyg       : {regime['score']}/5 — {regime['label']}")
    print(f"  Sammanfattning: {regime['summary_text']}")
    for key in ("trend", "volatility", "breadth", "sectors"):
        d = regime[key]
        print(f"  {key.capitalize():11s}: {d['status']:8s} {d['comment']}")

    print("\nHämtar Fear & Greed...")
    fg = get_fear_greed()
    chg = f"{fg['change_7d']:+.1f}" if fg["change_7d"] is not None else "n/a"
    print(f"  Värde       : {fg['value']:.0f} — {fg['category']} ({fg['signal']})")
    print(f"  7d-förändring: {chg}")
    print(f"  Källa       : {fg['source']}")

    print("\nBeräknar 12-månadersprognos...")
    fwd = get_forward_estimate(regime)
    print(f"  Bear/Base/Bull: {fwd['bear_case']:.0f}% / {fwd['base_case']:.0f}% / {fwd['bull_case']:.0f}%")
    print(f"  68% KI      : [{fwd['ci_68'][0]:.0f}%, {fwd['ci_68'][1]:.0f}%]")
    print(f"  95% KI      : [{fwd['ci_95'][0]:.0f}%, {fwd['ci_95'][1]:.0f}%]")
    print(f"  Faktorer    :")
    for k, v in fwd["factors"].items():
        print(f"    {k}: {v:+.1f}%")
    print(f"  Indata: CAPE={fwd['inputs']['cape']}, Ränta={fwd['inputs']['rate_current']:.1f}%, "
          f"Vol={fwd['inputs']['volatility']:.0f}%")
    print(f"\n  ⚠  {fwd['honesty_warning']}")
