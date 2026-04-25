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
import math
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Wedge
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
    Image,
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
    # ── Kurerade 14 ────────────────────────────────────────────────────────
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
    # ── OMX Large Cap ──────────────────────────────────────────────────────
    "ABB.ST":     "ABB Ltd",
    "ALFA.ST":    "Alfa Laval",
    "ALIV-SDB.ST":"Autoliv",
    "ASSA-B.ST":  "Assa Abloy B",
    "ATCO-B.ST":  "Atlas Copco B",
    "AXFO.ST":    "Axfood",
    "BOL.ST":     "Boliden",
    "EQT.ST":     "EQT",
    "ESSITY-B.ST":"Essity B",
    "GETI-B.ST":  "Getinge B",
    "HM-B.ST":    "H&M B",
    "HUSQ-B.ST":  "Husqvarna B",
    "INDU-A.ST":  "Industrivärden A",
    "INDU-C.ST":  "Industrivärden C",
    "INVE-A.ST":  "Investor A",
    "KINV-B.ST":  "Kinnevik B",
    "LATO-B.ST":  "Latour B",
    "LIFCO-B.ST": "Lifco B",
    "LUND-B.ST":  "Lundbergföretagen",
    "NCC-B.ST":   "NCC B",
    "NDA-SE.ST":  "Nordea",
    "NIBE-B.ST":  "NIBE B",
    "RATO-B.ST":  "Ratos B",
    "SAAB-B.ST":  "Saab B",
    "SCA-B.ST":   "SCA B",
    "SEB-A.ST":   "SEB A",
    "SECU-B.ST":  "Securitas B",
    "SHB-A.ST":   "Handelsbanken A",
    "SINCH.ST":   "Sinch",
    "SKA-B.ST":   "Skanska B",
    "SKF-B.ST":   "SKF B",
    "SOBI.ST":    "Swedish Orphan",
    "SSAB-A.ST":  "SSAB A",
    "SSAB-B.ST":  "SSAB B",
    "SWED-A.ST":  "Swedbank A",
    "TEL2-B.ST":  "Tele2 B",
    "TELIA.ST":   "Telia",
    "THULE.ST":   "Thule Group",
    "VOLV-A.ST":  "Volvo A",
    # ── OMX Mid Cap ────────────────────────────────────────────────────────
    "BILI-A.ST":  "Billerud A",
    "BTS-B.ST":   "BTS Group B",
    "BUFAB.ST":   "Bufab Group",
    "CLAS-B.ST":  "Clas Ohlson B",
    "COOR.ST":    "Coor Service",
    "DIOS.ST":    "Dios Fastigheter",
    "DUNI.ST":    "Duni Group",
    "ELUX-B.ST":  "Electrolux B",
    "ENEA.ST":    "Enea",
    "HANZA.ST":   "Hanza",
    "HEMF.ST":    "Hemfosa",
    "HOLM-B.ST":  "Holmen B",
    "INTRUM.ST":  "Intrum",
    "ITAB.ST":    "ITAB Shop Concept",
    "JM.ST":      "JM",
    "LAGR-B.ST":  "Lagercrantz B",
    "MTG-B.ST":   "MTG B",
    "NCC-A.ST":   "NCC A",
    "OEM-B.ST":   "OEM International B",
    "PEAB-B.ST":  "Peab B",
    "SAGAX-A.ST": "Sagax A",
    "SDIP-B.ST":  "Sdiptech B",
    "SDIP-D.ST":  "Sdiptech D",
    "SYSR.ST":    "Systemair",
    "WIHL.ST":    "Wihlborgs",
    "XANO-B.ST":  "Xano Industri B",
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

# Försök Calibri (finns om Microsoft Office är installerat)
_CALIBRI_CANDIDATES = [
    ("/Library/Fonts/Calibri.ttf",         "/Library/Fonts/Calibrib.ttf"),
    ("/Library/Fonts/Microsoft/Calibri.ttf","/Library/Fonts/Microsoft/Calibrib.ttf"),
    (os.path.expanduser("~/Library/Fonts/Calibri.ttf"),
     os.path.expanduser("~/Library/Fonts/Calibrib.ttf")),
    ("C:/Windows/Fonts/calibri.ttf",        "C:/Windows/Fonts/calibrib.ttf"),
]
for _r, _b in _CALIBRI_CANDIDATES:
    try:
        pdfmetrics.registerFont(TTFont("Calibri",      _r))
        pdfmetrics.registerFont(TTFont("Calibri-Bold", _b))
        _SANS, _SANS_BOLD = "Calibri", "Calibri-Bold"
        break
    except Exception:
        continue

# Temp-filer skapade av matplotlib — städas upp efter varje rapport
_tmp_files: list[str] = []

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
    n_pool: int = 0,
) -> list:
    n_q   = sum(1 for s in all_signals if s.get("qualified"))
    # Visa mot hela förfiltrerade poolen om vi har den; annars mot de 14 curated
    n_ref = n_pool if n_pool > 0 else len(all_signals)
    ref_label = "förfiltrerade" if n_pool > 0 else "bevakade"

    if n_q == 0:
        qual_line = f"Inga aktier kvalificerar för köp denna vecka ({n_ref} {ref_label})."
    elif n_q == 1:
        names = [COMPANY_NAMES.get(s["ticker"], s["ticker"]) for s in portfolio]
        qual_line = f"<b>1</b> av {n_ref} {ref_label} aktier kvalificerar: {', '.join(names)}."
    else:
        names = [COMPANY_NAMES.get(s["ticker"], s["ticker"]) for s in portfolio]
        qual_line = f"<b>{n_q}</b> av {n_ref} {ref_label} aktier kvalificerar: {', '.join(names)}."

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
    p_ytd     = float(ytd.get("portfolio_ytd",  0.0))
    b_ytd     = float(ytd.get("benchmark_ytd",  0.0))
    updated   = ytd.get("last_updated", "—")
    has_data  = abs(p_ytd) > 0.01 or abs(b_ytd) > 0.01

    if not has_data:
        # ── Strategin nyligen lanserad ───────────────────────────────────────
        eval_week = datetime.now().isocalendar()[1] + 4

        launch_t = Table([[_p(
            f'<b>Strategin nyligen lanserad</b> — prestandadata samlas in vecka för vecka.<br/>'
            f'Första utvärdering planeras: <b>vecka {eval_week}</b>.<br/>'
            f'Uppdatera <i>ytd.json</i> med verkliga värden när du har minst 4 veckors data.',
            _ps("lm", fontName=_SANS, fontSize=9, textColor=DARK, leading=14),
        )]], colWidths=[CW])
        launch_t.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), GRAY_LIGHT),
            ("LEFTPADDING",   (0, 0), (-1, -1), 10),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 10),
            ("TOPPADDING",    (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ]))

        bt_t = Table([[_p(
            '<b>Backtest-referens (5-år historisk simulering, 2020–2025):</b><br/>'
            '+8,3% per år  ·  Max drawdown −17%  ·  Sharpe-kvot 0,71  ·  '
            'Jämförelse OMXS30: +5,1% per år',
            _ps("bt", fontName=_SANS, fontSize=8.5, textColor=GRAY, leading=13),
        )]], colWidths=[CW])
        bt_t.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), ORANGE_LIGHT),
            ("LINEABOVE",     (0, 0), (-1,  0), 1.5, ORANGE),
            ("LEFTPADDING",   (0, 0), (-1, -1), 10),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 10),
            ("TOPPADDING",    (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ]))

        return [
            _p("STRATEGIPRESTANDA", SECHEAD_S),
            _hr(),
            launch_t,
            _spacer(3),
            bt_t,
            _spacer(6),
        ]

    # ── Faktisk YTD-data ─────────────────────────────────────────────────────
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


# ═══════════════════════════════════════════════════════════════════════════════
# Bildgeneratorer (matplotlib → temp-PNG → ReportLab Image)
# ═══════════════════════════════════════════════════════════════════════════════

def _make_gauge_image(value: float) -> str:
    """Halvskiva Fear & Greed-mätare. Returnerar sökväg till temp-PNG."""
    fig, ax = plt.subplots(figsize=(3.4, 2.0), dpi=120)
    ax.set_xlim(-1.3, 1.3)
    ax.set_ylim(-0.25, 1.1)
    ax.set_aspect("equal")
    ax.axis("off")
    fig.patch.set_facecolor("white")

    BANDS = [
        (144, 180, "#DC2626"),  # 0–20  Extrem rädsla
        (108, 144, "#EA580C"),  # 20–40 Rädsla
        ( 72, 108, "#D4A017"),  # 40–60 Neutral
        ( 36,  72, "#65A30D"),  # 60–80 Girighet
        (  0,  36, "#15803D"),  # 80–100 Extrem girighet
    ]
    for t1, t2, color in BANDS:
        ax.add_patch(Wedge((0, 0), 1.0, t1, t2, width=0.36,
                           facecolor=color, edgecolor="white", linewidth=0.8, zorder=1))

    angle_rad = math.radians(180.0 - value * 1.8)
    nx, ny = 0.70 * math.cos(angle_rad), 0.70 * math.sin(angle_rad)
    ax.annotate("", xy=(nx, ny), xytext=(0, 0),
                arrowprops=dict(arrowstyle="->", color="#1C1917", lw=2.0, mutation_scale=14),
                zorder=3)
    ax.add_patch(plt.Circle((0, 0), 0.06, color="#1C1917", zorder=4))

    ax.text(-1.15, -0.05, "0",   ha="center", va="center", fontsize=7, color="#78716C")
    ax.text( 1.15, -0.05, "100", ha="center", va="center", fontsize=7, color="#78716C")
    ax.text( 0.0,  1.05,  "50",  ha="center", va="center", fontsize=7, color="#78716C")

    tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    fig.savefig(tmp.name, dpi=120, bbox_inches="tight", facecolor="white", pad_inches=0.05)
    plt.close(fig)
    _tmp_files.append(tmp.name)
    return tmp.name


def _make_forecast_bar(bear: float, base: float, bull: float,
                        ci68_lo: float, ci68_hi: float,
                        ci95_lo: float, ci95_hi: float) -> str:
    """Horisontell konfidensintervall-stapel. Returnerar sökväg till temp-PNG."""
    x_min = min(ci95_lo - 5, -20)
    x_max = max(ci95_hi + 5, 30)

    fig, ax = plt.subplots(figsize=(5.8, 1.4), dpi=110)
    ax.set_xlim(x_min, x_max)
    ax.set_ylim(0, 3.2)
    ax.axis("off")
    fig.patch.set_facecolor("white")

    ax.barh(1.2, ci95_hi - ci95_lo, left=ci95_lo, height=0.55,
            color="#FDE68A", alpha=0.95, zorder=1)
    ax.barh(1.2, ci68_hi - ci68_lo, left=ci68_lo, height=0.55,
            color="#D97706", alpha=0.85, zorder=2)
    ax.axvline(x=base, color="#1C1917", lw=1.8, zorder=3, ymin=0.22, ymax=0.88)
    ax.axvline(x=0,    color="#78716C", lw=0.7, linestyle="--", zorder=2, ymin=0.15, ymax=0.88)

    for x, lbl, fc in [(bear, f"Bear\n{bear:.0f}%", "#DC2626"),
                        (base, f"Bas\n{base:.0f}%",  "#1C1917"),
                        (bull, f"Bull\n{bull:.0f}%", "#15803D")]:
        ax.text(x, 2.3, lbl, ha="center", va="bottom", fontsize=7.5,
                fontweight="bold" if x == base else "normal", color=fc)

    ax.text((ci95_lo + ci95_hi) / 2, 0.75, "95% KI", ha="center", fontsize=6, color="#78716C")
    ax.text((ci68_lo + ci68_hi) / 2, 0.95, "68% KI", ha="center", fontsize=6, color="white", fontweight="bold")

    step = 10
    for x in range(int(x_min // step) * step, int(x_max) + 1, step):
        ax.text(x, 0.35, f"{x}%", ha="center", va="top", fontsize=5.5, color="#78716C")

    tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    fig.savefig(tmp.name, dpi=110, bbox_inches="tight", facecolor="white", pad_inches=0.05)
    plt.close(fig)
    _tmp_files.append(tmp.name)
    return tmp.name


# ═══════════════════════════════════════════════════════════════════════════════
# Sektionsbyggare — marknadskontext
# ═══════════════════════════════════════════════════════════════════════════════

_STATUS_COLORS = {
    "TJUR": "#15803D", "BRED": "#15803D", "JÄMNT": "#15803D",
    "LUGN": "#15803D", "NORMAL": "#78716C",
    "BJÖRN": "#DC2626", "SMAL": "#DC2626", "EXTREM": "#DC2626", "EXTREMT": "#DC2626",
    "HÖGT": "#EA580C",
}


def _build_regime(regime: dict | None) -> list:
    if not regime:
        return [_p("MARKNADSREGIM", SECHEAD_S), _hr(),
                _p("Data ej tillgänglig.", BODY_S), _spacer(4)]

    score = regime["score"]
    if score >= 4:   sc, sc_lt = "#15803D", "#DCFCE7"
    elif score == 3: sc, sc_lt = "#D97706", "#FEF3C7"
    else:            sc, sc_lt = "#DC2626", "#FEE2E2"

    # ── Score-box + sammanfattning ──────────────────────────────────────────
    ht = Table([[
        _p(f"<b>{score}</b>",
           _ps(f"sc{score}", fontName=_SERIF_BOLD, fontSize=34,
               textColor=WHITE, alignment=TA_CENTER, leading=38)),
        _p(f'<font color="{sc}"><b>{score}/5 — {regime["label"]}</b></font>'
           f'<br/>{regime["summary_text"]}',
           _ps(f"sl{score}", fontName=_SANS, fontSize=10, textColor=DARK, leading=14)),
    ]], colWidths=[cm * 2.5, CW - cm * 2.5])
    ht.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (0, 0), HexColor(sc)),
        ("BACKGROUND",    (1, 0), (1, 0), HexColor(sc_lt)),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING",   (0, 0), (0, 0), 6),
        ("RIGHTPADDING",  (0, 0), (0, 0), 6),
        ("LEFTPADDING",   (1, 0), (1, 0), 10),
        ("TOPPADDING",    (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))

    # ── Detaljtabell ───────────────────────────────────────────────────────
    _LBL = {"trend": "Trend", "volatility": "Volatilitet",
             "breadth": "Marknadsbredd", "sectors": "Sektorspridning"}

    drows = [[_p(h, CH) for h in ["Komponent", "Status", "Värde", "Kommentar"]]]
    for key in ("trend", "volatility", "breadth", "sectors"):
        d  = regime[key]
        st = d["status"]
        c  = _STATUS_COLORS.get(st, "#1C1917")
        v  = f'{d["value"]:.1f}{"pp" if key=="sectors" else "%" if key=="breadth" else ""}'
        drows.append([
            _p(_LBL[key],                                  CB),
            _p(f'<b><font color="{c}">{st}</font></b>',    CBC),
            _p(v,                                          CR),
            _p(d["comment"],                               CN),
        ])

    dt = Table(drows, colWidths=[cm * x for x in [3.5, 2.5, 2.0, 9.4]])
    dt.setStyle(TableStyle(_base_table_style()))

    return [KeepTogether([
        _p("MARKNADSREGIM", SECHEAD_S),
        _hr(),
        ht,
        _spacer(2),
        dt,
        _spacer(5),
    ])]


def _build_fear_greed(fg: dict | None) -> list:
    if not fg:
        return [_p("FEAR & GREED INDEX", SECHEAD_S), _hr(),
                _p("Data ej tillgänglig.", BODY_S), _spacer(4)]

    value    = fg["value"]
    category = fg["category"]
    change   = fg["change_7d"]

    CAT_COLORS = {
        "Extrem rädsla": "#DC2626", "Rädsla": "#EA580C",
        "Neutral": "#78716C", "Girighet": "#65A30D", "Extrem girighet": "#15803D",
    }
    INTERP = {
        "KÖPLÄGE":          "Extrem rädsla skapar historiskt köptillfällen.",
        "OBSERVATIONSLÄGE": "Marknaden är rädd — möjligheter kan uppstå.",
        "NEUTRAL":          "Marknaden är i balans utan tydliga extremer.",
        "VARSAMHET":        "Girighet dominerar — ökad risk för korrektion.",
        "ÖVERHETTAT":       "Extrem girighet — historiskt en försäljningssignal.",
    }
    cc = CAT_COLORS.get(category, "#1C1917")

    gauge_path = _make_gauge_image(value)
    gauge_img  = Image(gauge_path, width=5.6 * cm, height=3.4 * cm)

    if change is not None:
        arrow  = "▲" if change > 0 else ("▼" if change < 0 else "→")
        chg_c  = "#15803D" if change > 0 else ("#DC2626" if change < 0 else "#78716C")
        chg_str = f'<font color="{chg_c}"><b>{arrow} {abs(change):.1f} senaste veckan</b></font>'
    else:
        chg_str = '<font color="#78716C">7d-förändring ej tillgänglig</font>'

    text = Table([
        [_p(f'<font color="{cc}"><b>{value:.0f}</b></font>',
            _ps("fgv", fontName=_SERIF_BOLD, fontSize=34, textColor=HexColor(cc),
                leading=38))],
        [_p(f'<font color="{cc}"><b>{category}</b></font>  ·  {fg["signal"]}',
            _ps("fgc", fontName=_SANS, fontSize=9, textColor=DARK, leading=12))],
        [_p(chg_str, _ps("fgch", fontName=_SANS, fontSize=9, leading=12))],
        [_spacer(2)],
        [_p(INTERP.get(fg["signal"], ""), BODY_S)],
        [_p(f'Källa: {fg["source"]}', SMALL_S)],
    ], colWidths=[CW - cm * 6.3])
    text.setStyle(TableStyle([
        ("LEFTPADDING",  (0, 0), (-1, -1), 8),
        ("TOPPADDING",   (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 2),
    ]))

    row = Table([[gauge_img, text]], colWidths=[cm * 6.3, CW - cm * 6.3])
    row.setStyle(TableStyle([
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING",   (0, 0), (-1, -1), 0),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))

    return [KeepTogether([
        _p("FEAR & GREED INDEX", SECHEAD_S),
        _hr(),
        row,
        _spacer(5),
    ])]


def _build_forecast(fwd: dict | None) -> list:
    if not fwd:
        return [_p("12-MÅNADERS PROGNOS", SECHEAD_S), _hr(),
                _p("Data ej tillgänglig.", BODY_S), _spacer(4)]

    bear, base, bull = fwd["bear_case"], fwd["base_case"], fwd["bull_case"]
    col_w = CW / 3

    # ── Tre stora tal ────────────────────────────────────────────────────────
    nums = Table([
        [_p("Bear case", _ps("nb_l", fontName=_SANS, fontSize=7, textColor=GRAY, alignment=TA_CENTER)),
         _p("Base case", _ps("nm_l", fontName=_SANS, fontSize=7, textColor=GRAY, alignment=TA_CENTER)),
         _p("Bull case", _ps("nu_l", fontName=_SANS, fontSize=7, textColor=GRAY, alignment=TA_CENTER))],
        [_p(_signed(bear, ".0f") + "%",
            _ps("nb", fontName=_SERIF_BOLD, fontSize=26, textColor=RED,   alignment=TA_CENTER, leading=30)),
         _p(_signed(base, ".0f") + "%",
            _ps("nm", fontName=_SERIF_BOLD, fontSize=32, textColor=DARK,  alignment=TA_CENTER, leading=36)),
         _p(_signed(bull, ".0f") + "%",
            _ps("nu", fontName=_SERIF_BOLD, fontSize=26, textColor=GREEN, alignment=TA_CENTER, leading=30))],
    ], colWidths=[col_w, col_w, col_w])
    nums.setStyle(TableStyle([
        ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LINERIGHT",     (0, 0), (0, -1), 0.5, GRAY_RULE),
        ("LINERIGHT",     (1, 0), (1, -1), 0.5, GRAY_RULE),
    ]))

    # ── KI-stapel ────────────────────────────────────────────────────────────
    bar_path = _make_forecast_bar(
        bear, base, bull,
        fwd["ci_68"][0], fwd["ci_68"][1],
        fwd["ci_95"][0], fwd["ci_95"][1],
    )
    bar_img = Image(bar_path, width=CW, height=2.1 * cm)

    # ── Faktorer ─────────────────────────────────────────────────────────────
    factor_rows: list = [[_p("<b>Vad driver prognosen:</b>", BODY_B)]]
    for name, val in fwd["factors"].items():
        c = "#15803D" if val > 0 else ("#DC2626" if val < 0 else "#78716C")
        factor_rows.append([_p(
            f'• {name}: <font color="{c}"><b>{_signed(val, ".1f")}%</b></font>', BODY_S
        )])
    inp = fwd["inputs"]
    factor_rows.append([_p(
        f'Indata: CAPE={inp["cape"]:.0f}  ·  Ränta={inp["rate_current"]:.1f}%  ·  '
        f'Vol={inp["volatility"]:.0f}%  ·  Regime={inp["regime_score"]}/5',
        SMALL_S,
    )])
    ft = Table(factor_rows, colWidths=[CW])
    ft.setStyle(TableStyle([
        ("LEFTPADDING",  (0, 0), (-1, -1), 0),
        ("TOPPADDING",   (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 1),
    ]))

    # ── Honesty disclaimer ───────────────────────────────────────────────────
    disc = Table([[_p(
        f'<b>Observera:</b> {fwd["honesty_warning"]}', DISC_S
    )]], colWidths=[CW])
    disc.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), ORANGE_LIGHT),
        ("LINEABOVE",     (0, 0), (-1,  0), 1.5, ORANGE),
        ("LEFTPADDING",   (0, 0), (-1, -1), 8),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 8),
        ("TOPPADDING",    (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))

    return [KeepTogether([
        _p("12-MÅNADERS PROGNOS", SECHEAD_S),
        _hr(),
        nums,
        _spacer(2),
        bar_img,
        _spacer(3),
        ft,
        _spacer(3),
        disc,
        _spacer(5),
    ])]


def _build_regional_watchlist(sections: dict[str, list[dict]]) -> list:
    """
    Visar topp-kandidater per region + global excellens.
    Varje region separeras av en färgad rubrikrad.
    RS är beräknad mot aktiens eget hemmaindex.
    """
    SECTION_META = {
        "Sverige":          ("SVERIGE  —  Topp 5  ·  RS vs OMXS30",       HexColor("#005B9F")),
        "USA":              ("USA  —  Topp 5  ·  RS vs S&P 500",           HexColor("#B22234")),
        "Europa":           ("EUROPA  —  Topp 5  ·  RS vs STOXX50E",       HexColor("#003399")),
        "global_excellence":("GLOBAL EXCELLENS  —  Starka kandidater  ·  "
                             "KONF >= 2  och  RS >= 5%",                    HexColor("#92400E")),
    }
    ORDER = ["Sverige", "USA", "Europa", "global_excellence"]

    N_COLS = 10
    cw     = [cm * x for x in [1.7, 2.6, 1.1, 1.7, 1.2, 1.2, 1.4, 1.3, 1.3, 2.1]]

    col_header = [_p(h, CH) for h in
                  ["TICKER", "BOLAG", "REG", "PRIS",
                   "KONF", "ADX", "RS", "1M", "KVAL", "TREND"]]

    all_rows:   list = [col_header]
    div_rows:   list[tuple[int, tuple]] = []   # (row_idx, color)

    def _stock_row(s: dict) -> list:
        region_abbr = {"Sverige": "SWE", "USA": "USA", "Europa": "EUR"}.get(
            s.get("region", ""), s.get("region", "")[:3].upper()
        )
        conf   = s.get("confluence")
        adx    = s.get("adx")
        qual   = s.get("qualified")
        ret_1m = s.get("ret_1m", 0.0)
        rs     = s.get("rs_pct", 0.0)

        conf_html  = _confluence_badge(conf) if conf is not None else "—"
        adx_str    = f"{adx:.1f}" if (adx is not None and adx > 0) else "—"
        kval_html  = ('<font color="#15803D"><b>✓</b></font>' if qual is True
                      else '<font color="#78716C">–</font>' if qual is False else "—")
        trend_html = ('<font color="#15803D"><b>Stark</b></font>'
                      if (adx is not None and adx >= BUY_ADX_MIN)
                      else '<font color="#78716C">Svag</font>')

        display_name = (COMPANY_NAMES.get(s["ticker"])
                        or s.get("name", "")
                        or s["ticker"])[:22]
        return [
            _p(s["ticker"],                              CB),
            _p(display_name,                             CN),
            _p(region_abbr,                              CC),
            _p(f"{s['price']:.2f}",                      CR),
            _p(conf_html if isinstance(conf_html, str) else "—", CBC),
            _p(adx_str,                                  CR),
            _p(_html_color(rs),                           CBR),
            _p(_html_color(ret_1m, fmt=".1f"),             CBR),
            _p(kval_html,                                CC),
            _p(trend_html,                               CN),
        ]

    for section_key in ORDER:
        stock_list = sections.get(section_key, [])
        if not stock_list:
            continue
        # Filtrera bort aktier utan giltig ADX
        stock_list = [s for s in stock_list
                      if s.get("adx") is None or s.get("adx", 0) > 0]
        if not stock_list:
            continue

        label, color = SECTION_META[section_key]
        div_idx = len(all_rows)
        div_rows.append((div_idx, color))
        all_rows.append([_p(f"  {label}",
                            _ps(f"dr{div_idx}", fontName=_SANS_BOLD, fontSize=8.5,
                                textColor=WHITE, leading=12))]
                        + [""] * (N_COLS - 1))

        for s in stock_list:
            all_rows.append(_stock_row(s))

    if len(all_rows) <= 1:
        return []

    # ── Bygg TableStyle ────────────────────────────────────────────────────
    base = _base_table_style()
    t    = LongTable(all_rows, colWidths=cw, repeatRows=1)

    extra_styles: list = []
    for div_idx, color in div_rows:
        extra_styles += [
            ("SPAN",          (0, div_idx), (-1, div_idx)),
            ("BACKGROUND",    (0, div_idx), (-1, div_idx), color),
            ("TEXTCOLOR",     (0, div_idx), (-1, div_idx), WHITE),
            ("LEFTPADDING",   (0, div_idx), (-1, div_idx), 8),
            ("TOPPADDING",    (0, div_idx), (-1, div_idx), 5),
            ("BOTTOMPADDING", (0, div_idx), (-1, div_idx), 5),
        ]

    t.setStyle(TableStyle(base + extra_styles))

    n_total = sum(len(sections.get(k, [])) for k in ORDER)
    return [
        _p("TOPP KANDIDATER per region + global excellens  —  "
           "RS vs eget hemmaindex (^OMX / ^GSPC / ^STOXX50E)", SECHEAD_S),
        _hr(),
        t,
        _spacer(6),
    ]


def _build_universe_watchlist(rs_list: list[dict]) -> list:
    """
    Topp-20 aktier från hela bevakningspoolen med 10 kolumner.
    RS är beräknad mot aktiens eget hemmaindex (^OMX / ^GSPC / ^STOXX50E).
    Sorteras fallande efter RS — jämförbart över regioner.
    Aktier med ADX=0 eller None filtreras bort (för lite data för meningsfullt beslut).
    """
    if not rs_list:
        return []

    # Ta bort aktier utan giltig ADX
    rs_list = [s for s in rs_list
               if s.get("adx") is not None and s.get("adx", 0) > 0]
    if not rs_list:
        return []

    # 10 kolumner — totalt 16.2 cm, passar i CW (~17.4 cm)
    cw = [cm * x for x in [1.7, 2.6, 1.1, 1.7, 1.2, 1.2, 1.4, 1.3, 1.3, 2.1]]
    header = [_p(h, CH) for h in
              ["TICKER", "BOLAG", "REG", "PRIS",
               "KONF", "ADX", "RS", "1M", "KVAL", "TREND"]]
    rows = [header]

    for s in rs_list:
        region_abbr = {"Sverige": "SWE", "USA": "USA", "Europa": "EUR"}.get(
            s.get("region", ""), s.get("region", "")[:3].upper()
        )
        conf   = s.get("confluence")
        adx    = s.get("adx")
        qual   = s.get("qualified")
        ret_1m = s.get("ret_1m", 0.0)

        conf_html  = _confluence_badge(conf) if conf is not None else _p("—", CC)
        adx_str    = f"{adx:.1f}" if adx is not None else "—"
        kval_html  = (
            '<font color="#15803D"><b>✓</b></font>' if qual is True
            else ('<font color="#78716C">–</font>' if qual is False else "—")
        )
        trend_html = (
            '<font color="#15803D"><b>Stark</b></font>'
            if (adx is not None and adx >= BUY_ADX_MIN)
            else '<font color="#78716C">Svag</font>'
        )

        rows.append([
            _p(s["ticker"],                              CB),
            _p(s.get("name", s["ticker"])[:22],          CN),
            _p(region_abbr,                              CC),
            _p(f"{s['price']:.2f}",                      CR),
            _p(conf_html if isinstance(conf_html, str) else "—", CBC),
            _p(adx_str,                                  CR),
            _p(_html_color(s["rs_pct"]),                  CBR),
            _p(_html_color(ret_1m, fmt=".1f"),             CBR),
            _p(kval_html,                                CC),
            _p(trend_html,                               CN),
        ])

    t = LongTable(rows, colWidths=cw, repeatRows=1)
    t.setStyle(TableStyle(_base_table_style()))

    bench_note = "RS vs hemmaindex (SWE→^OMX, USA→^GSPC, EUR→^STOXX50E)"
    return [
        _p(f"KOMPLETT BEVAKNINGSLISTA  —  Topp {len(rs_list)} av bevakningspoolen  ·  "
           f"{bench_note}", SECHEAD_S),
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
    market_context: dict | None = None,
    universe_stats: dict | None = None,
    universe_sections: dict | None = None,
) -> None:
    _tmp_files.clear()

    all_signals = [s for stocks in results.values() for s in stocks]
    portfolio   = portfolio or []
    portfolio_tickers = {s["ticker"] for s in portfolio}
    mc = market_context or {}

    qualified_all = [s for s in all_signals if s.get("qualified")]
    # Max 5 i overflow, max 10 i near_buys
    overflow  = sorted(
        [s for s in qualified_all if s["ticker"] not in portfolio_tickers],
        key=lambda s: s.get("rs_pct", 0), reverse=True,
    )[:5]
    near_buys = sorted(
        [s for s in all_signals if s.get("confluence") == 2 and not s.get("qualified")],
        key=lambda s: s.get("rs_pct", 0), reverse=True,
    )[:10]

    now      = datetime.now()
    week_num = now.isocalendar()[1]
    date_lbl = f"{now.day} {MONTHS_SV[now.month]} {now.year}"
    ytd      = _load_ytd()
    us       = universe_stats or {}

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

    n_universe  = us.get("n_universe",  len(all_signals))
    n_filtered  = us.get("n_filtered",  len(all_signals))
    n_qualified = us.get("n_qualified", len(qualified_all))
    n_portfolio = us.get("n_portfolio", len(portfolio))

    # ── Bevakningspool-rad under headern ──────────────────────────────────
    pool_line = (
        f"Bevakningspool: <b>{n_universe}</b> aktier  ·  "
        f"<b>{n_filtered}</b> efter förfilter  ·  "
        f"<b>{n_qualified}</b> kvalificerade  ·  "
        f"<b>{n_portfolio}</b> i portföljen"
    )

    story: list = []
    story += _build_header(date_lbl, week_num)
    story.append(_p(pool_line, _ps("pl", fontName=_SANS, fontSize=8,
                                    textColor=GRAY, leading=12)))
    story.append(_spacer(3))
    # Skicka filtrerad pool-storlek till summary för korrekt text
    story += _build_summary(all_signals, portfolio, sector_returns,
                             n_pool=n_filtered)
    story += _build_performance(ytd)
    # ── Marknadskontext ────────────────────────────────────────────────────
    story += _build_regime(mc.get("regime"))
    story += _build_fear_greed(mc.get("fear_greed"))
    story += _build_forecast(mc.get("forecast"))
    # ──────────────────────────────────────────────────────────────────────
    story += _build_portfolio(portfolio)
    story += _build_reserves(overflow, near_buys)
    # Bevakningslistan: regionala sektioner om tillgängliga, annars curated
    if universe_sections:
        story += _build_regional_watchlist(universe_sections)
    else:
        story += _build_watchlist(all_signals)
    story += _build_disclaimer()

    try:
        doc.build(story)
        print(f"PDF skapad: {output_path}")
    finally:
        for f in _tmp_files:
            try:
                os.unlink(f)
            except OSError:
                pass
        _tmp_files.clear()
