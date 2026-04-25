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

load_dotenv()

SENDER = "onboarding@resend.dev"


def send_email(pdf_path: str, date_str: str) -> None:
    api_key   = os.getenv("RESEND_API_KEY", "")
    recipient = os.getenv("RECIPIENT_EMAIL", "")

    if not api_key:
        raise ValueError("Saknar RESEND_API_KEY i .env-filen.")
    if not recipient:
        raise ValueError("Saknar RECIPIENT_EMAIL i .env-filen.")

    resend.api_key = api_key

    with open(pdf_path, "rb") as f:
        pdf_b64 = base64.b64encode(f.read()).decode("utf-8")

    resend.Emails.send({
        "from":    SENDER,
        "to":      [recipient],
        "subject": f"TrendRadar — Handelssignaler {date_str}",
        "text": (
            f"Hej,\n\n"
            f"Din veckorapport för handelssignaler ({date_str}) finns bifogad.\n\n"
            f"—\nTrendRadar\n(Ej finansiell rådgivning)"
        ),
        "attachments": [{
            "filename": Path(pdf_path).name,
            "content":  pdf_b64,
        }],
    })

    print(f"Mejl skickat till {recipient}")
