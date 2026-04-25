"""
Tre-lagers konfluenssystem för handelssignaler.

Lager 1 (Makro):    Hemmamarknadens trend — MA50 vs MA200 på hemmaindex
Lager 2 (Sektor):   Sektorns 3M-avkastning vs hemmaindex (+/-2% tröskel)
Lager 3 (Teknisk):  MA50 > MA200 + ADX ≥ 22

Konfluens = summa (-3 till +3)
Köpkrav  : konfluens == 3  OCH  ADX ≥ 22  OCH  RS vs OMXS30 ≥ 3%

Stop-loss : pris − 2 × ATR(14)
Mål       : pris + 3 × ATR(14)
Positionsstorlek: vol<20% → 30%, vol>35% → 15%, annars 20%
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import yfinance as yf

# --------------------------------------------------------------------------- #
# Konfiguration
# --------------------------------------------------------------------------- #

HOME_INDEX: dict[str, str] = {
    "Sverige": "^OMX",
    "USA":     "^GSPC",
    "Europa":  "^STOXX",
}
HOME_INDEX_FALLBACK: dict[str, str] = {
    "Europa": "^STOXX50E",   # Euro Stoxx 50 om STOXX 600 ej tillgänglig
}

SECTOR_ETF: dict[str, str] = {
    "VOLV-B.ST":  "XLI",   # Industrials
    "ATCO-A.ST":  "XLI",
    "SAND.ST":    "XLI",
    "HEXA-B.ST":  "XLK",   # Technology
    "INVE-B.ST":  "XLF",   # Financials
    "ERIC-B.ST":  "XLK",
    "NVDA":       "XLK",
    "MSFT":       "XLK",
    "AAPL":       "XLK",
    "BRK-B":      "XLF",
    "ASML":       "XLK",
    "NOVO-B.CO":  "XLV",   # Healthcare
    "MC.PA":      "XLY",   # Consumer Discretionary
    "SAP":        "XLK",
}

BENCHMARK     = "^OMX"   # RS mäts alltid mot OMXS30

MA_FAST       = 50
MA_SLOW       = 200
ADX_PERIOD    = 14
ATR_PERIOD    = 14
RS_DAYS       = 63        # ≈ 3 månader handelsdagar
VOL_DAYS      = 20

BUY_CONFLUENCE = 3
BUY_ADX_MIN    = 22
BUY_RS_MIN     = 3.0      # %
SIDEWAYS_PCT   = 1.0      # Makrolager: within ±1% = sidledes
SECTOR_DIFF    = 2.0      # Sektorlager: ±2% tröskel

MAX_POSITIONS   = 5
MAX_SECTOR_FRAC = 0.50
BASE_SIZE       = 0.20
LOW_VOL_THR     = 20      # % annualiserad
HIGH_VOL_THR    = 35
LOW_VOL_SIZE    = 0.30
HIGH_VOL_SIZE   = 0.15

# --------------------------------------------------------------------------- #
# Within-session data-cache (undviker dubbla nedladdningar av index/ETF)
# --------------------------------------------------------------------------- #

_cache: dict[str, pd.DataFrame] = {}


def _fetch(symbol: str, period: str = "14mo") -> pd.DataFrame | None:
    if symbol in _cache:
        return _cache[symbol]
    try:
        df = yf.download(symbol, period=period, auto_adjust=True, progress=False)
        if df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        _cache[symbol] = df
        return df
    except Exception:
        return None


def _close(df: pd.DataFrame) -> pd.Series:
    c = df["Close"]
    return c.iloc[:, 0] if isinstance(c, pd.DataFrame) else c


# --------------------------------------------------------------------------- #
# Tekniska indikatorer
# --------------------------------------------------------------------------- #

def _wilder(series: pd.Series, period: int) -> pd.Series:
    """Wilders utjämning — ekvivalent med EMA alpha=1/period."""
    return series.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def _atr_series(df: pd.DataFrame, period: int = ATR_PERIOD) -> pd.Series:
    high  = df["High"] if not isinstance(df["High"], pd.DataFrame) else df["High"].iloc[:, 0]
    low   = df["Low"]  if not isinstance(df["Low"],  pd.DataFrame) else df["Low"].iloc[:, 0]
    close = _close(df)
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low  - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    return _wilder(tr, period)


def _adx_value(df: pd.DataFrame, period: int = ADX_PERIOD) -> float:
    high  = df["High"] if not isinstance(df["High"], pd.DataFrame) else df["High"].iloc[:, 0]
    low   = df["Low"]  if not isinstance(df["Low"],  pd.DataFrame) else df["Low"].iloc[:, 0]
    close = _close(df)

    up   = high - high.shift(1)
    down = low.shift(1) - low

    plus_dm  = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)

    atr      = _wilder(pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low  - close.shift(1)).abs(),
    ], axis=1).max(axis=1), period)

    plus_di  = 100 * _wilder(plus_dm,  period) / atr.replace(0, np.nan)
    minus_di = 100 * _wilder(minus_dm, period) / atr.replace(0, np.nan)

    denom = (plus_di + minus_di).replace(0, np.nan)
    dx    = 100 * (plus_di - minus_di).abs() / denom
    adx   = _wilder(dx.fillna(0), period)
    return float(adx.iloc[-1])


def _return_3m(df: pd.DataFrame) -> float:
    close = _close(df).dropna()
    if len(close) < RS_DAYS + 1:
        return 0.0
    return float(close.iloc[-1] / close.iloc[-RS_DAYS - 1] - 1) * 100


def _vol_ann(close: pd.Series) -> float:
    returns = close.pct_change().dropna()
    if len(returns) < VOL_DAYS:
        return 25.0
    return float(returns.iloc[-VOL_DAYS:].std() * np.sqrt(252) * 100)


# --------------------------------------------------------------------------- #
# De tre lagren
# --------------------------------------------------------------------------- #

def _macro_layer(region: str) -> tuple[int, str]:
    sym = HOME_INDEX.get(region, "^GSPC")
    df  = _fetch(sym)
    if df is None:
        fb = HOME_INDEX_FALLBACK.get(region)
        if fb:
            df, sym = _fetch(fb), fb
    if df is None:
        return 0, f"{sym}: ej tillgänglig"

    close = _close(df).dropna()
    if len(close) < MA_SLOW + 5:
        return 0, f"{sym}: för lite data"

    ma50  = float(close.rolling(MA_FAST).mean().iloc[-1])
    ma200 = float(close.rolling(MA_SLOW).mean().iloc[-1])
    diff  = (ma50 - ma200) / ma200 * 100

    if diff > SIDEWAYS_PCT:
        return +1, f"{sym} MA50>MA200 ({diff:+.1f}%)"
    elif diff < -SIDEWAYS_PCT:
        return -1, f"{sym} MA50<MA200 ({diff:+.1f}%)"
    else:
        return  0, f"{sym} sidledes ({diff:+.1f}%)"


def _sector_layer(ticker: str, region: str) -> tuple[int, str]:
    etf = SECTOR_ETF.get(ticker)
    if not etf:
        return 0, "ingen sektor-ETF"

    etf_df = _fetch(etf)
    if etf_df is None:
        return 0, f"{etf}: ej tillgänglig"

    home_sym = HOME_INDEX.get(region, "^GSPC")
    home_df  = _fetch(home_sym)
    if home_df is None:
        fb = HOME_INDEX_FALLBACK.get(region)
        if fb:
            home_df, home_sym = _fetch(fb), fb
    if home_df is None:
        return 0, f"{home_sym}: ej tillgänglig"

    etf_3m  = _return_3m(etf_df)
    home_3m = _return_3m(home_df)
    diff    = etf_3m - home_3m

    if diff >= SECTOR_DIFF:
        return +1, f"{etf} {etf_3m:+.1f}% vs {home_sym} {home_3m:+.1f}% (Δ{diff:+.1f}%)"
    elif diff <= -SECTOR_DIFF:
        return -1, f"{etf} {etf_3m:+.1f}% vs {home_sym} {home_3m:+.1f}% (Δ{diff:+.1f}%)"
    else:
        return  0, f"{etf} {etf_3m:+.1f}% vs {home_sym} {home_3m:+.1f}% (Δ{diff:+.1f}%)"


def _tech_layer(df: pd.DataFrame) -> tuple[int, float, str]:
    close = _close(df).dropna()
    if len(close) < MA_SLOW + 5:
        return 0, 0.0, "för lite data"

    ma50  = float(close.rolling(MA_FAST).mean().iloc[-1])
    ma200 = float(close.rolling(MA_SLOW).mean().iloc[-1])
    adx   = _adx_value(df)

    if ma50 > ma200 and adx >= BUY_ADX_MIN:
        return +1, adx, f"MA50>MA200 + ADX={adx:.1f}"
    elif ma50 > ma200:
        return  0, adx, f"MA50>MA200 men ADX={adx:.1f} < {BUY_ADX_MIN}"
    else:
        diff = (ma50 - ma200) / ma200 * 100
        return -1, adx, f"MA50<MA200 ({diff:.1f}%) ADX={adx:.1f}"


# --------------------------------------------------------------------------- #
# Huvud-funktion
# --------------------------------------------------------------------------- #

def calculate_signals(ticker: str, df: pd.DataFrame, region: str) -> dict:
    close = _close(df).dropna()

    macro_score,  macro_reason          = _macro_layer(region)
    sector_score, sector_reason         = _sector_layer(ticker, region)
    tech_score,   adx,   tech_reason    = _tech_layer(df)

    confluence = macro_score + sector_score + tech_score

    # Relativ styrka mot OMXS30 (fast benchmark)
    bench_df = _fetch(BENCHMARK)
    stock_3m = _return_3m(df)
    rs_pct   = stock_3m - _return_3m(bench_df) if bench_df is not None else 0.0

    # ATR-baserade stop och mål
    atr_val   = float(_atr_series(df).iloc[-1])
    price     = float(close.iloc[-1])
    stop_loss = price - 2 * atr_val
    target    = price + 3 * atr_val

    # Positionsstorlek efter volatilitet
    vol = _vol_ann(close)
    if vol < LOW_VOL_THR:
        pos_pct = LOW_VOL_SIZE * 100
    elif vol > HIGH_VOL_THR:
        pos_pct = HIGH_VOL_SIZE * 100
    else:
        pos_pct = BASE_SIZE * 100

    qualified = (
        confluence == BUY_CONFLUENCE
        and adx >= BUY_ADX_MIN
        and rs_pct >= BUY_RS_MIN
    )

    # Kompakt sammanfattning för PDF-tabellen
    layer_str = f"M{macro_score:+d} S{sector_score:+d} T{tech_score:+d}"
    qual_tag  = "✓KVAL" if qualified else ""
    reasons   = [s for s in [layer_str, f"ADX={adx:.0f}", f"RS={rs_pct:+.1f}%", qual_tag] if s]

    # Legacy-fält som report.py använder
    sma50  = float(close.rolling(MA_FAST).mean().iloc[-1]) if len(close) >= MA_FAST else None
    sma200 = float(close.rolling(MA_SLOW).mean().iloc[-1]) if len(close) >= MA_SLOW else None

    delta      = close.diff()
    avg_gain   = delta.clip(lower=0).ewm(com=13, min_periods=14).mean()
    avg_loss   = (-delta.clip(upper=0)).ewm(com=13, min_periods=14).mean()
    rsi_val    = float(100 - 100 / (1 + avg_gain.iloc[-1] / avg_loss.iloc[-1]))

    ema12  = close.ewm(span=12, adjust=False).mean()
    ema26  = close.ewm(span=26, adjust=False).mean()
    macd_val = float((ema12 - ema26).iloc[-1])

    w52_high = float(close.rolling(252).max().iloc[-1])
    w52_low  = float(close.rolling(252).min().iloc[-1])

    return {
        # Ny konfluenslogik
        "ticker":          ticker,
        "region":          region,
        "price":           price,
        "macro_score":     macro_score,
        "macro_reason":    macro_reason,
        "sector_score":    sector_score,
        "sector_reason":   sector_reason,
        "tech_score":      tech_score,
        "tech_reason":     tech_reason,
        "confluence":      confluence,
        "adx":             adx,
        "rs_pct":          rs_pct,
        "stock_3m":        stock_3m,
        "atr":             atr_val,
        "stop_loss":       stop_loss,
        "target":          target,
        "vol_annualized":  vol,
        "position_pct":    pos_pct,
        "qualified":       qualified,
        # Bakåtkompatibla fält för report.py
        "signal":          _label(confluence),
        "score":           confluence,
        "rsi":             rsi_val,
        "macd":            macd_val,
        "sma50":           sma50,
        "sma200":          sma200,
        "week52_high":     w52_high,
        "week52_low":      w52_low,
        "pct_from_high":   (price - w52_high) / w52_high * 100,
        "reasons":         reasons,
    }


def _label(confluence: int) -> str:
    if confluence >= 3:    return "STARKT KÖP"
    if confluence == 2:    return "KÖP"
    if confluence <= -3:   return "STARKT SÄLJ"
    if confluence == -2:   return "SÄLJ"
    return "HÅLL"


# --------------------------------------------------------------------------- #
# Portföljallokering (anropas efter alla calculate_signals-körningar)
# --------------------------------------------------------------------------- #

def size_positions(all_signals: list[dict]) -> list[dict]:
    """
    Väljer max MAX_POSITIONS kvalificerade aktier, tillämpar sektorkapp (50%).
    Sorterar på konfluens → RS (bäst först).
    Returnerar lista med extra fält 'final_position_pct'.
    """
    qualified = sorted(
        [s for s in all_signals if s["qualified"]],
        key=lambda s: (s["confluence"], s["rs_pct"]),
        reverse=True,
    )

    selected: list[dict] = []
    sector_alloc: dict[str, float] = {}

    for stock in qualified:
        if len(selected) >= MAX_POSITIONS:
            break
        sector  = SECTOR_ETF.get(stock["ticker"], "OTHER")
        raw     = stock["position_pct"] / 100
        current = sector_alloc.get(sector, 0.0)
        capped  = min(raw, MAX_SECTOR_FRAC - current)
        if capped <= 0:
            continue
        sector_alloc[sector] = current + capped
        selected.append({**stock, "final_position_pct": round(capped * 100, 1)})

    return selected


# --------------------------------------------------------------------------- #
# Direkt testläge: python signals.py
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    TEST = [("SAND.ST", "Sverige"), ("NVDA", "USA")]

    for ticker, region in TEST:
        print(f"\n{'═' * 60}")
        print(f"  {ticker}  ({region})")
        print(f"{'═' * 60}")

        raw = yf.download(ticker, period="14mo", auto_adjust=True, progress=False)
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        r = calculate_signals(ticker, raw, region)

        print(f"  Pris               : {r['price']:.2f}")
        print(f"  3M-avkastning      : {r['stock_3m']:+.1f}%")
        print()
        print(f"  LAGER 1 – Makro    : {r['macro_score']:+d}   {r['macro_reason']}")
        print(f"  LAGER 2 – Sektor   : {r['sector_score']:+d}   {r['sector_reason']}")
        print(f"  LAGER 3 – Teknisk  : {r['tech_score']:+d}   {r['tech_reason']}")
        print(f"  ─────────────────────────────────────────")
        print(f"  Konfluens          : {r['confluence']:+d}  →  {r['signal']}")
        print(f"  ADX (14d)          : {r['adx']:.1f}  {'✓' if r['adx'] >= BUY_ADX_MIN else '✗'} (krav ≥ {BUY_ADX_MIN})")
        print(f"  RS vs OMXS30       : {r['rs_pct']:+.1f}%  {'✓' if r['rs_pct'] >= BUY_RS_MIN else '✗'} (krav ≥ +{BUY_RS_MIN}%)")
        print(f"  ─────────────────────────────────────────")
        print(f"  KVALIFICERAD       : {'✓  JA' if r['qualified'] else '✗  NEJ'}")
        print()
        print(f"  ATR (14d)          : {r['atr']:.2f}")
        print(f"  Stop-loss (2×ATR)  : {r['stop_loss']:.2f}")
        print(f"  Mål       (3×ATR)  : {r['target']:.2f}")
        print(f"  Risk/Reward        : 1 : 1.5")
        print(f"  Vol 20d ann.       : {r['vol_annualized']:.0f}%")
        print(f"  Positionsstorlek   : {r['position_pct']:.0f}%")
