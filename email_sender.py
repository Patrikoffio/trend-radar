"""
Skickar PDF-rapporten via Resend API.

Konfigureras via .env-filen:
  RESEND_API_KEY  — API-nyckel från resend.com/api-keys
  RECIPIENT_EMAIL — mottagarens e-postadress
"""

import base64
import os
from pathlib import Path

import resend
from dotenv import load_dotenv

# Ladda .env lokalt om filen finns — på GitHub Actions används miljövariabler direkt
if Path(".env").exists():
    load_dotenv()

SENDER = "onboarding@resend.dev"


def send_email(pdf_path: str, date_str: str, summary: str = "") -> None:
    api_key   = os.getenv("RESEND_API_KEY", "")
    recipient = os.getenv("RECIPIENT_EMAIL", "")

    if not api_key:
        raise ValueError("Miljövariabeln RESEND_API_KEY saknas.")
    if not recipient:
        raise ValueError("Miljövariabeln RECIPIENT_EMAIL saknas.")

    resend.api_key = api_key

    with open(pdf_path, "rb") as f:
        pdf_b64 = base64.b64encode(f.read()).decode("utf-8")

    body = f"Hej,\n\n{summary}\nPDF-rapporten finns bifogad.\n\n—\nTrendRadar\n(Ej finansiell rådgivning)"

    resend.Emails.send({
        "from":    SENDER,
        "to":      [recipient],
        "subject": f"TrendRadar — Handelssignaler {date_str}",
        "text":    body,
        "attachments": [{
            "filename": Path(pdf_path).name,
            "content":  pdf_b64,
        }],
    })

    print(f"Mejl skickat till {recipient}")
