"""
Trend Radar — huvudfil.

Kör: python main.py
Schemalägg varje söndag med cron (Mac/Linux):
  0 8 * * 0 cd /path/to/trend-radar && python main.py
"""

import sys
import time
from datetime import datetime

import pandas as pd
import yfinance as yf

from email_sender import send_email
from market_context import get_fear_greed, get_forward_estimate, get_regime
from report import generate_pdf
from signals import calculate_signals, size_positions
from universe import apply_prefilter, get_full_universe  # top-20 RS flödar till report

STOCKS: dict[str, list[str]] = {
    "Sverige": ["VOLV-B.ST", "ATCO-A.ST", "SAND.ST", "HEXA-B.ST", "INVE-B.ST", "ERIC-B.ST"],
    "USA": ["NVDA", "MSFT", "AAPL", "BRK-B"],
    "Europa": ["ASML", "NOVO-B.CO", "MC.PA", "SAP"],
}

# Hämta 14 månader för att säkra 200-dagars SMA med marginal
DATA_PERIOD = "14mo"


def fetch_data(ticker: str) -> pd.DataFrame:
    df = yf.download(ticker, period=DATA_PERIOD, auto_adjust=True, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


def run(send: bool = True) -> None:
    date_str = datetime.now().strftime("%Y-%m-%d")
    print(f"=== Trend Radar {date_str} ===\n")

    results: dict[str, list[dict]] = {}
    dataframes: dict[str, pd.DataFrame] = {}

    for region, tickers in STOCKS.items():
        print(f"[{region}]")
        results[region] = []
        for ticker in tickers:
            try:
                df = fetch_data(ticker)
                if df.empty or len(df) < 60:
                    print(f"  {ticker}: för lite data")
                    continue
                data = calculate_signals(ticker, df, region)
                results[region].append(data)
                dataframes[ticker] = df
                qual = "✓" if data["qualified"] else " "
                print(f"  {qual} {ticker:12s} {data['signal']:12s}  konf={data['confluence']:+d}  ADX={data['adx']:.1f}  RS={data['rs_pct']:+.1f}%")
            except Exception as exc:
                print(f"  {ticker}: FEL — {exc}")
        print()

    if not any(results.values()):
        print("Inga signaler beräknade. Avbryter.")
        sys.exit(1)

    # Portföljallokering — välj ut kvalificerade aktier med kappningar
    all_signals = [s for region_stocks in results.values() for s in region_stocks]
    portfolio   = size_positions(all_signals)

    print("=== Portföljrekommendation ===")
    if portfolio:
        for p in portfolio:
            print(f"  {p['ticker']:12s} {p['final_position_pct']:.0f}%  stop={p['stop_loss']:.2f}  mål={p['target']:.2f}")
    else:
        print("  Inga aktier kvalificerar sig denna vecka.")
    print()

    # ── Universum-statistik + RS top-20 ───────────────────────────────────
    print("=== Bevakningspool ===")
    _t0 = time.time()
    n_universe = n_filtered = 0
    universe_top20: list[dict] = []
    try:
        universe   = get_full_universe()
        n_universe = len(universe)
        print(f"  Hämtade {n_universe} aktier...", flush=True)
        filtered, rs_all = apply_prefilter(universe)   # returnerar HELA sorterade listan
        n_filtered = len(filtered)
        print(f"  {n_universe} → {n_filtered} efter förfilter  ({time.time()-_t0:.1f}s)")

        # ── Topp-10 per region som kandidater (filtreras ner till 5 med KONF≥1) ──
        by_region: dict[str, list[dict]] = {"Sverige": [], "USA": [], "Europa": []}
        for s in rs_all:
            r = s.get("region", "")
            if r in by_region and len(by_region[r]) < 10:   # tag topp-10 för headroom
                by_region[r].append(s)

        # ── Berika icke-kurerade stocks med fullständiga signaler ─────────
        curated_sigs = {s["ticker"]: s for s in all_signals}   # redan beräknade
        to_enrich = []
        seen_e: set[str] = set()
        for lst in by_region.values():
            for s in lst:
                t = s["ticker"]
                if t not in curated_sigs and t not in seen_e:
                    to_enrich.append(s)
                    seen_e.add(t)

        extra_sigs: dict[str, dict] = {}
        if to_enrich:
            _t1 = time.time()
            tickers_e = [s["ticker"] for s in to_enrich]
            print(f"  Berikar {len(tickers_e)} icke-kurerade aktier...", flush=True)
            try:
                raw_e = yf.download(tickers_e, period=DATA_PERIOD,
                                    auto_adjust=True, progress=False,
                                    group_by="ticker")
                for rs_item in to_enrich:
                    tkr    = rs_item["ticker"]
                    region = rs_item.get("region", "USA")
                    try:
                        df_tkr = (raw_e.copy() if len(tickers_e) == 1
                                  else raw_e[tkr].copy()
                                  if tkr in raw_e.columns.get_level_values(0) else None)
                        if df_tkr is None or df_tkr.empty or len(df_tkr) < 60:
                            extra_sigs[tkr] = rs_item; continue
                        if isinstance(df_tkr.columns, pd.MultiIndex):
                            df_tkr.columns = df_tkr.columns.get_level_values(0)
                        sig = calculate_signals(tkr, df_tkr, region)
                        sig["name"]  = rs_item.get("name", tkr)  # namn bevaras
                        sig["ret_1m"] = rs_item.get("ret_1m", 0.0)
                        # rs_pct och benchmark kommer redan korrekt från calculate_signals
                        extra_sigs[tkr] = sig
                    except Exception:
                        extra_sigs[tkr] = rs_item
                print(f"  Berikning klar ({time.time()-_t1:.1f}s)")
            except Exception as e:
                print(f"  Berikning FEL: {e}")

        def resolve(s: dict) -> dict:
            """Hämtar bästa tillgängliga signal för en aktie."""
            t = s["ticker"]
            if t in curated_sigs:
                return curated_sigs[t]
            return extra_sigs.get(t, s)

        # ── Bygg sektioner: topp-5 per region med KONF ≥ 1 ──────────────────
        def _top5_konf1(region_name: str) -> list[dict]:
            """Löser, filtrerar KONF≥1, returnerar topp-5 sorterat på RS."""
            resolved = [resolve(s) for s in by_region[region_name]]
            passed   = [s for s in resolved if s.get("confluence", 0) >= 1]
            # Sortera efter rs_pct (peer-RS) fallande
            passed.sort(key=lambda s: s.get("rs_pct", 0), reverse=True)
            return passed[:5]

        universe_sections: dict[str, list[dict]] = {
            "Sverige": _top5_konf1("Sverige"),
            "USA":     _top5_konf1("USA"),
            "Europa":  _top5_konf1("Europa"),
        }

        # Global Excellens: KONF ≥ 2 OCH RS ≥ 5% (fler kandidater än bara portföljen)
        all_enriched = list(curated_sigs.values()) + [
            v for v in extra_sigs.values() if v["ticker"] not in curated_sigs
        ]
        global_strong = sorted(
            [s for s in all_enriched
             if s.get("confluence", 0) >= 2 and s.get("rs_pct", 0) >= 5.0],
            key=lambda s: s.get("rs_pct", 0), reverse=True,
        )[:5]
        # Visa sektionen bara om den innehåller mer än portföljens aktier
        portfolio_tickers_set = {p["ticker"] for p in portfolio}
        has_new = any(s["ticker"] not in portfolio_tickers_set for s in global_strong)
        universe_sections["global_excellence"] = global_strong if global_strong else []

        # ── Diagnostik-logg ───────────────────────────────────────────────
        print(f"\n  {'Ticker':12s} {'Region':8s} {'Benchmark':10s} {'RS':>7s} {'ADX':>6s} {'KVAL':>4s}")
        print(f"  {'-'*55}")
        for debug_tkr in ["SAND.ST", "NVDA", "AAPL", "ERIC-B.ST"]:
            s = curated_sigs.get(debug_tkr) or extra_sigs.get(debug_tkr)
            if s:
                adx   = s.get("adx", 0)
                qual  = "✓" if s.get("qualified") else " "
                bench = s.get("benchmark", "?")
                print(f"  {debug_tkr:12s} {s.get('region','?'):8s} {bench:10s} "
                      f"{s.get('rs_pct',0):+7.1f}% {adx:6.1f} {qual:>4s}")
        print(f"\n  Sverige topp-5 (KONF≥1): " +
              "  ".join(f"{s['ticker']} RS={s.get('rs_pct',0):+.1f}% KONF={s.get('confluence',0)}"
                        for s in universe_sections["Sverige"]))
        print()

    except Exception as e:
        universe_sections = {}
        print(f"  FEL: {e}")
    print()

    universe_stats = {
        "n_universe":  n_universe,
        "n_filtered":  n_filtered,
        "n_qualified": sum(1 for s in all_signals if s.get("qualified")),
        "n_portfolio": len(portfolio),
    }

    # ── Marknadskontext ────────────────────────────────────────────────────
    print("=== Marknadskontext ===")
    mc: dict = {}

    try:
        mc["regime"] = get_regime(all_signals)
        r = mc["regime"]
        print(f"  Regim: {r['score']}/5 — {r['label']}  ({r['summary_text']})")
    except Exception as e:
        print(f"  Regim: FEL — {e}")

    try:
        mc["fear_greed"] = get_fear_greed()
        fg = mc["fear_greed"]
        print(f"  F&G:   {fg['value']:.0f} ({fg['category']})  källa={fg['source']}")
    except Exception as e:
        print(f"  F&G:   FEL — {e}")

    try:
        mc["forecast"] = get_forward_estimate(mc.get("regime"))
        fw = mc["forecast"]
        print(f"  12mån: bear={fw['bear_case']:.0f}%  bas={fw['base_case']:.0f}%  bull={fw['bull_case']:.0f}%")
    except Exception as e:
        print(f"  12mån: FEL — {e}")

    print()

    # ── Mejlsammanfattning ─────────────────────────────────────────────────
    def _mc_summary() -> str:
        lines = [f"Veckorapport TrendRadar — vecka {datetime.now().isocalendar()[1]}"]
        if "regime" in mc:
            r = mc["regime"]
            lines.append(f"Marknadsregim: {r['score']}/5 — {r['label']}")
        if "fear_greed" in mc:
            fg = mc["fear_greed"]
            lines.append(f"Fear & Greed:  {fg['value']:.0f} ({fg['category']})")
        if "forecast" in mc:
            fw = mc["forecast"]
            lines.append(
                f"12-mån prognos: bas {fw['base_case']:.0f}%  "
                f"(intervall {fw['bear_case']:.0f}% – {fw['bull_case']:.0f}%)"
            )
        return "\n".join(lines) + "\n"

    pdf_path = f"rapport_{date_str}.pdf"
    generate_pdf(results, pdf_path, dataframes, portfolio, mc,
                 universe_stats, universe_sections if universe_sections else None)

    if send:
        send_email(pdf_path, date_str, _mc_summary())
    else:
        print("(Mejlutskick hoppat över med --no-email)")


if __name__ == "__main__":
    send_mail = "--no-email" not in sys.argv
    run(send=send_mail)
