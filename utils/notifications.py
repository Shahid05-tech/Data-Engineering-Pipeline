"""
notifications.py
================
Sends HTML email alerts via the Resend API.

Environment variables (loaded from .env automatically):
    RESEND_API_KEY  – Your Resend secret key     (required)
    OWNER_EMAIL     – Recipient email address     (required)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

# ── Load .env from the project root ──────────────────────────────────────────
def _load_env() -> None:
    """Try python-dotenv; if missing, fall back to a manual parser."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return

    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(dotenv_path=env_path, override=False)
    except ImportError:
        # Manual fallback: parse KEY=VALUE lines
        with open(env_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)

_load_env()

# ── Status helpers ────────────────────────────────────────────────────────────
_STATUS_COLOR = {
    "SUCCESS": "#22c55e",
    "ERROR":   "#ef4444",
    "WARNING": "#f97316",
    "START":   "#3b82f6",
    "TEST":    "#8b5cf6",
}

_STATUS_ICON = {
    "SUCCESS": "✅",
    "ERROR":   "❌",
    "WARNING": "⚠️",
    "START":   "🚀",
    "TEST":    "🧪",
}


def _build_html(status: str, message: str, details: str) -> str:
    color = _STATUS_COLOR.get(status.upper(), "#64748b")
    icon  = _STATUS_ICON.get(status.upper(), "ℹ️")
    ts    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    details_block = ""
    if details:
        details_block = f"""
        <div style="margin-top:16px; background:#f8fafc; border:1px solid #e2e8f0;
                    border-radius:6px; padding:12px;">
            <p style="margin:0 0 6px; font-weight:600; color:#475569;">Details / Logs</p>
            <pre style="margin:0; white-space:pre-wrap; word-break:break-all;
                        font-size:13px; color:#334155;">{details}</pre>
        </div>"""

    return f"""
<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr><td align="center" style="padding:32px 16px;">
      <table width="600" cellpadding="0" cellspacing="0"
             style="background:#ffffff;border-radius:12px;
                    box-shadow:0 4px 24px rgba(0,0,0,0.08);overflow:hidden;">

        <!-- Header banner -->
        <tr>
          <td style="background:{color};padding:24px 32px;">
            <h1 style="margin:0;color:#ffffff;font-size:22px;font-weight:700;">
              {icon}&nbsp; Data Pipeline — {status}
            </h1>
          </td>
        </tr>

        <!-- Body -->
        <tr>
          <td style="padding:28px 32px;">
            <p style="margin:0 0 12px;font-size:15px;color:#1e293b;">
              <strong>Message:</strong> {message}
            </p>
            <p style="margin:0 0 4px;font-size:13px;color:#64748b;">
              <strong>Timestamp:</strong> {ts}
            </p>
            <p style="margin:0;font-size:13px;color:#64748b;">
              <strong>Status:</strong>
              <span style="display:inline-block;padding:2px 10px;
                           background:{color};color:#fff;border-radius:12px;
                           font-size:12px;font-weight:600;">{status}</span>
            </p>
            {details_block}
          </td>
        </tr>

        <!-- Footer -->
        <tr>
          <td style="background:#f8fafc;padding:16px 32px;
                     border-top:1px solid #e2e8f0;">
            <p style="margin:0;font-size:12px;color:#94a3b8;">
              Sent automatically by <strong>Data-Engineering-Pipeline</strong>
              via Resend · {ts}
            </p>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


def send_pipeline_notification(
    status: str,
    message: str,
    details: str = "",
) -> bool:
    """
    Send an HTML email via Resend.

    Parameters
    ----------
    status  : 'SUCCESS' | 'ERROR' | 'WARNING' | 'START' | 'TEST'
    message : One-line summary shown in the subject and body.
    details : Optional multi-line tech details / stack trace.

    Returns
    -------
    True on success, False on failure.
    """
    api_key     = os.environ.get("RESEND_API_KEY", "").strip()
    owner_email = os.environ.get("OWNER_EMAIL", "").strip()

    if not api_key:
        print("⚠️  RESEND_API_KEY is not set – email skipped.", file=sys.stderr)
        return False
    if not owner_email:
        print("⚠️  OWNER_EMAIL is not set – email skipped.", file=sys.stderr)
        return False

    try:
        import resend  # lazy import so the module loads even without the package
        resend.api_key = api_key
    except ImportError:
        print("⚠️  'resend' package not found. Run: pip install resend", file=sys.stderr)
        return False

    subject = (
        f"[{status}] Data Pipeline Alert – "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )

    params: resend.Emails.SendParams = {
        "from":    "Pipeline-Bot <onboarding@resend.dev>",
        "to":      [owner_email],
        "subject": subject,
        "html":    _build_html(status, message, details),
    }

    try:
        resp = resend.Emails.send(params)
        print(f"✅ Email sent → id={resp.get('id')}  to={owner_email}")
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"❌ Resend send failed: {exc}", file=sys.stderr)
        return False


# ── Quick smoke-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    ok = send_pipeline_notification(
        status  = "TEST",
        message = "Smoke test from notifications.py – setup is working correctly.",
        details = "API key loaded from .env\nRecipient: " + os.environ.get("OWNER_EMAIL", "NOT SET"),
    )
    sys.exit(0 if ok else 1)
