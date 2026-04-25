"""
PDF-rapport med ReportLab (A4 portrait).

Sektioner:
  1  Header — datum, veckonummer, logga
  2  Marknadssammanfattning
  3  Strategiprestanda (YTD vs OMXS30)
  4  Aktiv portfölj
  5  Reserver och nära-lägen (konfluens +2)
  6  Komplett bevakningslista
  7  Riskvarning

YTD-data lagras i ytd.json och uppdateras manuellt.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import yfinance as yf

from reportlab.lib import colors
from reportlab.lib.colors import HexColor
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm, mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    HRFlowable,
    KeepTogether,
    LongTable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

# --------------------------------------------------------------------------- #
# Färgpalett
# --------------------------------------------------------------------------- #
ORANGE       = HexColor("#D97706")
ORANGE_LIGHT = HexColor("#FEF3C7")
ORANGE_MID   = HexColor("#FDE68A")
DARK         = HexColor("#1C1917")
GRAY         = HexColor("#78716C")
GRAY_LIGHT   = HexColor("#F5F5F4")
GRAY_RULE    = HexColor("#E7E5E4")
GREEN        = HexColor("#15803D")
GREEN_BG     = HexColor("#DCFCE7")
RED          = HexColor("#DC2626")
RED_BG       = HexColor("#FEE2E2")
HEADER_DARK  = HexColor("#292524")
WHITE        = colors.white

# --------------------------------------------------------------------------- #
# Layout
# --------------------------------------------------------------------------- #
PAGE_W, PAGE_H = A4
MARGIN  = 1.8 * cm
CW      = PAGE_W - 2 * MARGIN   # ~17.4 cm
ROW_H   = 6 * mm

# --------------------------------------------------------------------------- #
# Statiska mappings (kopierade hit för att undvika cirkulär import)
# --------------------------------------------------------------------------- #
COMPANY_NAMES: dict[str, str] = {
    "VOLV-B.ST":  "Volvo B",
    "ATCO-A.ST":  "Atlas Copco A",
    "SAND.ST":    "Sandvik",
    "HEXA-B.ST":  "Hexagon B",
    "INVE-B.ST":  "Investor B",
    "ERIC-B.ST":  "Ericsson B",
    "NVDA":       "NVIDIA",
    "MSFT":       "Microsoft",
    "AAPL":       "Apple",
    "BRK-B":      "Berkshire B",
    "ASML":       "ASML",
    "NOVO-B.CO":  "Novo Nordisk B",
    "MC.PA":      "LVMH",
    "SAP":        "SAP SE",
}

SECTOR_ETF: dict[str, str] = {
    "VOLV-B.ST": "XLI", "ATCO-A.ST": "XLI", "SAND.ST": "XLI",
    "HEXA-B.ST": "XLK", "ERIC-B.ST": "XLK",
    "INVE-B.ST": "XLF", "BRK-B": "XLF",
    "NVDA": "XLK", "MSFT": "XLK", "AAPL": "XLK", "ASML": "XLK", "SAP": "XLK",
    "NOVO-B.CO": "XLV",
    "MC.PA": "XLY",
}

SECTOR_NAMES: dict[str, str] = {
    "XLK": "Teknologi",
    "XLI": "Industri",
    "XLF": "Finans",
    "XLV": "Hälsovård",
    "XLY": "Konsument",
}

BUY_CONFLUENCE = 3
BUY_ADX_MIN    = 22
BUY_RS_MIN     = 3.0

MONTHS_SV = {
    1: "januari", 2: "februari", 3: "mars", 4: "april",
    5: "maj",     6: "juni",     7: "juli", 8: "augusti",
    9: "september", 10: "oktober", 11: "november", 12: "december",
}

YTD_FILE = Path(__file__).parent / "ytd.json"

# --------------------------------------------------------------------------- #
# Font-registrering — försök Georgia, faller tillbaka på Times-Roman
# --------------------------------------------------------------------------- #
_SERIF      = "Times-Roman"
_SERIF_BOLD = "Times-Bold"
_SANS       = "Helvetica"
_SANS_BOLD  = "Helvetica-Bold"
_SANS_ITAL  = "Helvetica-Oblique"

_GEORGIA_CANDIDATES = [
    ("/System/Library/Fonts/Supplemental/Georgia.ttf",
     "/System/Library/Fonts/Supplemental/Georgia Bold.ttf"),
    ("/Library/Fonts/Georgia.ttf",
     "/Library/Fonts/Georgia Bold.ttf"),
    ("C:/Windows/Fonts/georgia.ttf",
     "C:/Windows/Fonts/georgiab.ttf"),
]
for _r, _b in _GEORGIA_CANDIDATES:
    try:
        pdfmetrics.registerFont(TTFont("Georgia",      _r))
        pdfmetrics.registerFont(TTFont("Georgia-Bold", _b))
        _SERIF, _SERIF_BOLD = "Georgia", "Georgia-Bold"
        break
    except Exception:
        continue

# --------------------------------------------------------------------------- #
# Stilar
# --------------------------------------------------------------------------- #
def _ps(name: str, **kw) -> ParagraphStyle:
    return ParagraphStyle(name, **kw)

LOGO_S    = _ps("logo",    fontName=_SERIF_BOLD, fontSize=30, textColor=ORANGE, leading=36)
DATEW_S   = _ps("datew",   fontName=_SANS, fontSize=9, textColor=GRAY, alignment=TA_RIGHT, leading=13)
SECHEAD_S = _ps("sechead", fontName=_SANS_BOLD, fontSize=10, textColor=ORANGE,
                spaceBefore=2, spaceAfter=1, leading=14)
BODY_S    = _ps("body",    fontName=_SANS, fontSize=8.5, textColor=DARK, leading=13)
BODY_B    = _ps("bodyb",   fontName=_SANS_BOLD, fontSize=8.5, textColor=DARK, leading=13)
SMALL_S   = _ps("small",   fontName=_SANS_ITAL, fontSize=7, textColor=GRAY, leading=10)
DISC_S    = _ps("disc",    fontName=_SANS_ITAL, fontSize=7, textColor=GRAY, leading=10)

# Tabellcell-stilar
CN  = _ps("cn",  fontName=_SANS,      fontSize=8, textColor=DARK,  alignment=TA_LEFT,   leading=10)
CR  = _ps("cr",  fontName=_SANS,      fontSize=8, textColor=DARK,  alignment=TA_RIGHT,  leading=10)
CC  = _ps("cc",  fontName=_SANS,      fontSize=8, textColor=DARK,  alignment=TA_CENTER, leading=10)
CB  = _ps("cb",  fontName=_SANS_BOLD, fontSize=8, textColor=DARK,  alignment=TA_LEFT,   leading=10)
CBR = _ps("cbr", fontName=_SANS_BOLD, fontSize=8, textColor=DARK,  alignment=TA_RIGHT,  leading=10)
CBC = _ps("cbc", fontName=_SANS_BOLD, fontSize=8, textColor=DARK,  alignment=TA_CENTER, leading=10)
CH  = _ps("ch",  fontName=_SANS_BOLD, fontSize=8, textColor=WHITE, alignment=TA_CENTER, leading=10)
CHR = _ps("chr", fontName=_SANS_BOLD, fontSize=8, textColor=WHITE, alignment=TA_RIGHT,  leading=10)

# --------------------------------------------------------------------------- #
# Hjälpfunktioner
# --------------------------------------------------------------------------- #

def _load_ytd() -> dict:
    if YTD_FILE.exists():
        try:
            return json.loads(YTD_FILE.read_text())
        except Exception:
            pass
    return {"portfolio_ytd": 0.0, "benchmark_ytd": 0.0, "last_updated": "—"}


def _signed(val: float, fmt: str = ".1f") -> str:
    return f"+{val:{fmt}}" if val > 0 else f"{val:{fmt}}"


def _html_color(val: float, fmt: str = ".1f", bold: bool = True) -> str:
    """Returnerar HTML-färgat tal: grönt positivt, rött negativt."""
    text  = _signed(val, fmt)
    tag   = "b" if bold else "span"
    if val > 0:
        return f'<{tag}><font color="#15803D">{text}</font></{tag}>'
    elif val < 0:
        return f'<{tag}><font color="#DC2626">{text}</font></{tag}>'
    return f'<font color="#78716C">{text}</font>'


def _confluence_badge(conf: int) -> str:
    if conf >= 3:
        return f'<b><font color="#15803D">{_signed(conf, "d")}</font></b>'
    elif conf >= 1:
        return f'<font color="#D97706">{_signed(conf, "d")}</font>'
    elif conf <= -1:
        return f'<b><font color="#DC2626">{_signed(conf, "d")}</font></b>'
    return f'<font color="#78716C">{_signed(conf, "d")}</font>'


def _signal_label(s: dict) -> str:
    sig = s.get("signal", "")
    color_map = {
        "STARKT KÖP":  "#15803D",
        "KÖP":         "#65A30D",
        "HÅLL":        "#78716C",
        "SÄLJ":        "#DC2626",
        "STARKT SÄLJ": "#991B1B",
    }
    c = color_map.get(sig, "#1C1917")
    return f'<font color="{c}"><b>{sig}</b></font>'


def _what_is_missing(s: dict) -> str:
    """Förklarar varför en konfluens+2-aktie inte kvalificerar."""
    parts = []
    if s.get("confluence", 0) < BUY_CONFLUENCE:
        if s.get("tech_score", 0) < 1:
            adx = s.get("adx", 0)
            if adx < BUY_ADX_MIN:
                parts.append(f"ADX={adx:.0f} under tröskel (krav ≥ {BUY_ADX_MIN})")
            else:
                parts.append("MA50 < MA200")
        if s.get("macro_score", 0) < 1:
            parts.append("Makro neutral/neg.")
        if s.get("sector_score", 0) < 1:
            parts.append("Sektor neutral/neg.")
    if s.get("adx", 0) < BUY_ADX_MIN and not any("ADX" in p for p in parts):
        parts.append(f"ADX={s['adx']:.0f} < {BUY_ADX_MIN}")
    if s.get("rs_pct", 0) < BUY_RS_MIN:
        parts.append(f"RS={s['rs_pct']:.1f}% < {BUY_RS_MIN}%")
    return " · ".join(parts) if parts else "—"


def _sector_etf_returns() -> dict[str, float]:
    """Hämtar 3-månaders (63d) avkastning direkt från de fem sektor-ETF:erna."""
    result: dict[str, float] = {}
    for etf in SECTOR_NAMES:
        try:
            df = yf.download(etf, period="6mo", auto_adjust=True, progress=False)
            if df.empty:
                continue
            close = df["Close"]
            if isinstance(close, pd.DataFrame):
                close = close.iloc[:, 0]
            close = close.dropna()
            n = 63
            if len(close) >= n + 1:
                result[etf] = float(close.iloc[-1] / close.iloc[-n - 1] - 1) * 100
        except Exception:
            pass
    return result


def _base_table_style(header_color=HEADER_DARK) -> list:
    return [
        ("BACKGROUND",    (0, 0), (-1, 0),  header_color),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  WHITE),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [WHITE, GRAY_LIGHT]),
        ("GRID",          (0, 0), (-1, -1), 0.3, GRAY_RULE),
        ("LINEBELOW",     (0, 0), (-1, 0),  1,   header_color),
        ("LEFTPADDING",   (0, 0), (-1, -1), 5),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 5),
        ("TOPPADDING",    (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
    ]


def _p(text: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(str(text), style)


def _spacer(h: float = 5) -> Spacer:
    return Spacer(1, h * mm)


def _hr(space: float = 2) -> HRFlowable:
    return HRFlowable(width="100%", thickness=0.5, color=GRAY_RULE, spaceAfter=space * mm)


# --------------------------------------------------------------------------- #
# Sektionsbyggare
# --------------------------------------------------------------------------- #

def _build_header(date_label: str, week_num: int) -> list:
    logo = _p("TrendRadar", LOGO_S)
    date_block = _p(
        f'Vecka {week_num}<br/>'
        f'<font size="8" color="#78716C">{date_label}</font>',
        _ps("dw", fontName=_SANS_BOLD, fontSize=11, textColor=DARK,
            alignment=TA_RIGHT, leading=15),
    )
    t = Table([[logo, date_block]], colWidths=[CW * 0.65, CW * 0.35])
    t.setStyle(TableStyle([
        ("VALIGN",         (0, 0), (-1, -1), "BOTTOM"),
        ("TOPPADDING",     (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING",  (0, 0), (-1, -1), 2),
        ("LEFTPADDING",    (0, 0), (-1, -1), 0),
        ("RIGHTPADDING",   (0, 0), (-1, -1), 0),
    ]))
    return [
        t,
        HRFlowable(width="100%", thickness=2.5, color=ORANGE, spaceAfter=5 * mm),
    ]


def _build_summary(
    all_signals: list[dict],
    portfolio: list[dict],
    sector_returns: dict[str, float],
) -> list:
    n_q  = sum(1 for s in all_signals if s.get("qualified"))
    n_t  = len(all_signals)

    if n_q == 0:
        qual_line = f"Inga aktier kvalificerar för köp denna vecka ({n_t} bevakade)."
    elif n_q == 1:
        names = [COMPANY_NAMES.get(s["ticker"], s["ticker"]) for s in portfolio]
        qual_line = f"<b>1</b> av {n_t} aktier kvalificerar: {', '.join(names)}."
    else:
        names = [COMPANY_NAMES.get(s["ticker"], s["ticker"]) for s in portfolio]
        qual_line = f"<b>{n_q}</b> av {n_t} aktier kvalificerar: {', '.join(names)}."

    # Sektorprestanda från riktiga ETF-siffror
    ranked = sorted(sector_returns.items(), key=lambda x: x[1], reverse=True)
    strong = [f"{SECTOR_NAMES.get(e, e)} ({v:+.1f}%)" for e, v in ranked if v > 0][:2]
    weak   = [f"{SECTOR_NAMES.get(e, e)} ({v:+.1f}%)" for e, v in ranked if v < 0][-2:]
    strong_str = ", ".join(strong) if strong else "—"
    weak_str   = ", ".join(weak)   if weak   else "—"

    # Sektor-raden med alla ETF-siffror
    etf_parts = [f"{SECTOR_NAMES.get(e, e)} {_signed(v, '.1f')}%" for e, v in ranked]
    etf_line  = "  ·  ".join(etf_parts)

    rows = [
        [_p("Marknadsläge", _ps("mhdr", fontName=_SANS_BOLD, fontSize=9.5,
                                textColor=ORANGE, leading=13))],
        [_p(qual_line, BODY_S)],
        [_p(f"<b>Starka sektorer (3M):</b>  {strong_str}", BODY_S)],
        [_p(f"<b>Svaga sektorer (3M):</b>   {weak_str}",   BODY_S)],
        [_p(etf_line, SMALL_S)],
    ]
    t = Table(rows, colWidths=[CW])
    t.setStyle(TableStyle([
        ("BACKGROUND",   (0, 0), (-1, -1), ORANGE_LIGHT),
        ("LINEABOVE",    (0, 0), (-1, 0),  3,   ORANGE),
        ("LEFTPADDING",  (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING",   (0, 0), (-1, 0),  6),
        ("TOPPADDING",   (0, 1), (-1, -1), 3),
        ("BOTTOMPADDING",(0, -1),(-1, -1), 7),
        ("BOTTOMPADDING",(0, 0), (-1, -2), 2),
    ]))
    return [t, _spacer(5)]


def _build_performance(ytd: dict) -> list:
    p_ytd   = float(ytd.get("portfolio_ytd",  0.0))
    b_ytd   = float(ytd.get("benchmark_ytd",  0.0))
    updated = ytd.get("last_updated", "—")

    def _ytd_cell(val: float, label: str) -> list:
        v_color = GREEN if val > 0 else (RED if val < 0 else DARK)
        return [
            _p(label, _ps(f"yl_{label}", fontName=_SANS, fontSize=8,
                           textColor=GRAY, alignment=TA_CENTER, leading=11)),
            _p(_signed(val, ".1f") + "%",
               _ps(f"yv_{label}", fontName=_SERIF_BOLD, fontSize=34,
                   textColor=v_color, alignment=TA_CENTER, leading=40)),
        ]

    left  = _ytd_cell(p_ytd, "Portfölj YTD")
    right = _ytd_cell(b_ytd, "OMXS30 YTD")

    col_w = (CW - 2 * mm) / 2
    t = Table(
        [[left[0], right[0]], [left[1], right[1]]],
        colWidths=[col_w, col_w],
    )
    t.setStyle(TableStyle([
        ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("LINERIGHT",     (0, 0), (0, -1),  0.8, GRAY_RULE),
        ("TOPPADDING",    (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING",   (0, 0), (-1, -1), 0),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
    ]))

    return [
        _p("STRATEGIPRESTANDA", SECHEAD_S),
        _hr(),
        t,
        Spacer(1, 2 * mm),
        _p(f"Uppdatera <i>ytd.json</i> för att ändra prestationsdata  ·  Senast: {updated}", SMALL_S),
        _spacer(6),
    ]


def _build_portfolio(portfolio: list[dict]) -> list:
    if not portfolio:
        return [
            _p("AKTIV PORTFÖLJ", SECHEAD_S),
            _hr(),
            _p("Inga positioner kvalificerar denna vecka.", BODY_S),
            _spacer(6),
        ]

    # Kolumnbredder i mm → konverteras till pt via Table
    cw = [cm * x for x in [2.0, 3.2, 1.8, 1.8, 1.8, 1.8, 1.6, 1.6, 1.4]]

    header = [_p(h, CH) for h in
              ["TICKER", "BOLAG", "PRIS", "ENTRY", "STOP", "MÅL", "STORLEK", "RS", "KONF"]]
    rows = [header]
    for s in portfolio:
        rows.append([
            _p(s["ticker"],                                         CB),
            _p(COMPANY_NAMES.get(s["ticker"], s["ticker"]),        CN),
            _p(f"{s['price']:.2f}",                                CR),
            _p(f"{s['price']:.2f}",                                CR),  # entry = dagens pris
            _p(f'<font color="#DC2626">{s["stop_loss"]:.2f}</font>', CBR),
            _p(f'<font color="#15803D">{s["target"]:.2f}</font>',   CBR),
            _p(f"{s.get('final_position_pct', s.get('position_pct', 20)):.0f}%", CC),
            _p(_html_color(s["rs_pct"]),                            CBR),
            _p(_confluence_badge(s["confluence"]),                  CBC),
        ])

    t = Table(rows, colWidths=cw, repeatRows=1)
    style = _base_table_style(ORANGE)
    style += [("BACKGROUND", (0, 0), (-1, 0), ORANGE)]
    t.setStyle(TableStyle(style))

    return [KeepTogether([
        _p(f"AKTIV PORTFÖLJ  ({len(portfolio)} position{'er' if len(portfolio) != 1 else ''})", SECHEAD_S),
        _hr(),
        t,
        _spacer(6),
    ])]


def _build_reserves(overflow: list[dict], near_buys: list[dict]) -> list:
    # ------------------------------------------------------------------ #
    # Bygg tabellerna oberoende — håll sedan rubrik + tabell ihop med
    # KeepTogether så att sektionsrubriken aldrig hamnar ensam längst ner.
    # ------------------------------------------------------------------ #

    def _overflow_table() -> Table:
        cw = [cm * x for x in [2.0, 3.2, 1.5, 1.8, 1.8, 1.8, 1.6, 1.4]]
        rows = [[_p(h, CH) for h in
                 ["TICKER", "BOLAG", "PRIS", "STOP", "MÅL", "STORLEK", "RS", "KONF"]]]
        for s in overflow:
            rows.append([
                _p(s["ticker"],                                          CB),
                _p(COMPANY_NAMES.get(s["ticker"], s["ticker"]),          CN),
                _p(f"{s['price']:.2f}",                                  CR),
                _p(f'<font color="#DC2626">{s["stop_loss"]:.2f}</font>',  CBR),
                _p(f'<font color="#15803D">{s["target"]:.2f}</font>',     CBR),
                _p(f"{s['position_pct']:.0f}%",                          CC),
                _p(_html_color(s["rs_pct"]),                              CBR),
                _p(_confluence_badge(s["confluence"]),                    CBC),
            ])
        t = Table(rows, colWidths=cw, repeatRows=1)
        t.setStyle(TableStyle(_base_table_style()))
        return t

    def _near_buys_table() -> Table:
        amber = HexColor("#92400E")
        cw2 = [cm * x for x in [2.0, 3.0, 2.0, 1.4, 1.4, 1.6, 4.0]]
        rows = [[_p(h, CH) for h in
                 ["TICKER", "BOLAG", "PRIS", "KONF", "ADX", "RS", "VAD SAKNAS"]]]
        for s in near_buys:
            rows.append([
                _p(s["ticker"],                                      CB),
                _p(COMPANY_NAMES.get(s["ticker"], s["ticker"]),      CN),
                _p(f"{s['price']:.2f}",                              CR),
                _p(_confluence_badge(s["confluence"]),               CBC),
                _p(f"{s.get('adx', 0):.1f}",                        CC),
                _p(_html_color(s.get("rs_pct", 0)),                  CBR),
                _p(_what_is_missing(s),                              CN),
            ])
        t = Table(rows, colWidths=cw2, repeatRows=1)
        t.setStyle(TableStyle(
            _base_table_style(amber) + [("BACKGROUND", (0, 0), (-1, 0), amber)]
        ))
        return t

    if not overflow and not near_buys:
        return [KeepTogether([
            _p("RESERVER & NÄRA-LÄGEN", SECHEAD_S),
            _hr(),
            _p("Inga aktier i reserv eller nära-läge denna vecka.", BODY_S),
            _spacer(4),
        ])]

    elems: list = []

    if overflow:
        # Rubrik + overflow-tabell hålls ihop
        elems.append(KeepTogether([
            _p("RESERVER & NÄRA-LÄGEN", SECHEAD_S),
            _hr(),
            _p("<b>Kvalificerade — fick ej plats (sektorkapp)</b>", BODY_B),
            _spacer(2),
            _overflow_table(),
            _spacer(4),
        ]))
        if near_buys:
            # Andra sub-sektion hålls ihop för sig
            elems.append(KeepTogether([
                _p("<b>Konfluens +2 — behöver en bekräftelse</b>", BODY_B),
                _spacer(2),
                _near_buys_table(),
                _spacer(4),
            ]))
    else:
        # Enbart near_buys — rubrik + tabell hålls ihop
        elems.append(KeepTogether([
            _p("RESERVER & NÄRA-LÄGEN", SECHEAD_S),
            _hr(),
            _p("<b>Konfluens +2 — behöver en bekräftelse</b>", BODY_B),
            _spacer(2),
            _near_buys_table(),
            _spacer(4),
        ]))

    return elems


def _build_watchlist(all_signals: list[dict]) -> list:
    watchlist = sorted(all_signals, key=lambda s: s.get("rs_pct", 0), reverse=True)

    # 9 kolumner — totalt ~16.4 cm, passar i CW (~17.4 cm)
    cw = [cm * x for x in [1.8, 2.8, 1.3, 1.7, 1.2, 1.3, 1.5, 2.0, 2.8]]
    header = [_p(h, CH) for h in
              ["TICKER", "BOLAG", "REG.", "PRIS", "KONF", "ADX", "RS", "SIGNAL", "TREND"]]
    rows = [header]

    for s in watchlist:
        adx = s.get("adx", 0)
        region_abbr = {"Sverige": "SWE", "USA": "USA", "Europa": "EUR"}.get(
            s.get("region", ""), s.get("region", "")[:3].upper()
        )
        if adx >= BUY_ADX_MIN:
            trend_html = '<font color="#15803D"><b>Stark trend</b></font>'
        else:
            trend_html = '<font color="#78716C">Svag trend</font>'

        rows.append([
            _p(s["ticker"],                                      CB),
            _p(COMPANY_NAMES.get(s["ticker"], s["ticker"]),      CN),
            _p(region_abbr,                                      CC),
            _p(f"{s['price']:.2f}",                              CR),
            _p(_confluence_badge(s["confluence"]),               CBC),
            _p(f"{adx:.1f}",                                     CR),
            _p(_html_color(s.get("rs_pct", 0)),                  CBR),
            _p(_signal_label(s),                                 CN),
            _p(trend_html,                                       CN),
        ])

    # LongTable delar korrekt över sidbrytningar med upprepat headerrad
    t = LongTable(rows, colWidths=cw, repeatRows=1)
    t.setStyle(TableStyle(_base_table_style()))

    return [
        _p(f"KOMPLETT BEVAKNINGSLISTA  —  {len(watchlist)} aktier, sorterat efter RS fallande", SECHEAD_S),
        _hr(),
        t,
        _spacer(6),
    ]


def _build_disclaimer() -> list:
    text = (
        "<b>Riskvarning:</b> Denna rapport är inte finansiell rådgivning. "
        "Historisk avkastning är ingen garanti för framtida resultat. "
        "All handel med värdepapper innebär risk och du kan förlora hela ditt investerade kapital. "
        "Gör alltid din egen analys och konsultera en auktoriserad finansiell rådgivare "
        "innan du fattar investeringsbeslut."
    )
    t = Table([[_p(text, DISC_S)]], colWidths=[CW])
    t.setStyle(TableStyle([
        ("BACKGROUND",   (0, 0), (-1, -1), GRAY_LIGHT),
        ("LINEABOVE",    (0, 0), (-1, 0),  1, GRAY_RULE),
        ("LEFTPADDING",  (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING",   (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 6),
    ]))
    return [t]


# --------------------------------------------------------------------------- #
# Huvudfunktion
# --------------------------------------------------------------------------- #

def generate_pdf(
    results: Dict[str, List[dict]],
    output_path: str,
    dataframes: dict | None = None,
    portfolio: list[dict] | None = None,
) -> None:
    all_signals = [s for stocks in results.values() for s in stocks]
    portfolio   = portfolio or []
    portfolio_tickers = {s["ticker"] for s in portfolio}

    qualified_all = [s for s in all_signals if s.get("qualified")]
    overflow  = [s for s in qualified_all if s["ticker"] not in portfolio_tickers]
    near_buys = sorted(
        [s for s in all_signals if s.get("confluence") == 2 and not s.get("qualified")],
        key=lambda s: s.get("rs_pct", 0), reverse=True,
    )

    now       = datetime.now()
    week_num  = now.isocalendar()[1]
    date_lbl  = f"{now.day} {MONTHS_SV[now.month]} {now.year}"
    ytd       = _load_ytd()

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=MARGIN,  bottomMargin=MARGIN,
        title=f"TrendRadar — Vecka {week_num} {now.year}",
        author="TrendRadar",
    )

    print("Hämtar sektor-ETF-data...", flush=True)
    sector_returns = _sector_etf_returns()

    story: list = []
    story += _build_header(date_lbl, week_num)
    story += _build_summary(all_signals, portfolio, sector_returns)
    story += _build_performance(ytd)
    story += _build_portfolio(portfolio)
    story += _build_reserves(overflow, near_buys)
    story += _build_watchlist(all_signals)
    story += _build_disclaimer()

    doc.build(story)
    print(f"PDF skapad: {output_path}")
