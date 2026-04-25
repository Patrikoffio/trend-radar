"""
Trend Radar — huvudfil.

Kör: python main.py
Schemalägg varje söndag med cron (Mac/Linux):
  0 8 * * 0 cd /path/to/trend-radar && python main.py
"""

import sys
from datetime import datetime

import pandas as pd
import yfinance as yf

from email_sender import send_email
from report import generate_pdf
from signals import calculate_signals, size_positions

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
                print(f"  {qual} {ticker:12s} {data['signal']:12s}  konf={data['confluence']:+d}  ADX={data['adx']:.0f}  RS={data['rs_pct']:+.1f}%")
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

    pdf_path = f"rapport_{date_str}.pdf"
    generate_pdf(results, pdf_path, dataframes, portfolio)

    if send:
        send_email(pdf_path, date_str)
    else:
        print("(Mejlutskick hoppat över med --no-email)")


if __name__ == "__main__":
    send_mail = "--no-email" not in sys.argv
    run(send=send_mail)
