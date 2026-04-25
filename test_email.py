"""Skickar senaste rapporten som testmejl via Resend."""
import glob
import sys
from email_sender import send_email

reports = sorted(glob.glob("rapport_*.pdf"))
if not reports:
    print("Ingen rapport hittades. Kör main.py --no-email först.")
    sys.exit(1)

latest = reports[-1]
date_str = latest.removeprefix("rapport_").removesuffix(".pdf")
print(f"Skickar: {latest}")
send_email(latest, date_str)
