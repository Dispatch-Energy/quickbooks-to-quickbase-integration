"""
Acumatica Invoice Webhook
=========================
FastAPI router that receives webhooks from QuickBase when an invoice record
is modified.  If the record meets the forwarding criteria, it downloads the
attachment, appends an invoice-summary page (for PDFs), and emails everything
to the configured recipient via Microsoft Graph.

QuickBase webhook payload (configured in QB Automations):
    POST /webhook/invoice
    {
        "record_id": 12345,
        "table_id": "bqr..."   // optional — falls back to env var
    }

Forwarding criteria (all must be true):
    - Invoice Approval = "Invoice Approved for Payment"
    - Accounting Responsibility = "BKY Financial"
    - Invoice Payment Approval in:
        "Approved - Pay in Full"
        "Approved - Partial Payment"
        "Approved - Pay with Credit Card"
"""

import os
import io
import json
import base64
import logging
from datetime import datetime, timezone
from html import escape
from typing import Any, Dict, List, Optional

import requests as http_requests
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

try:
    import msal
except ImportError:
    msal = None

try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import inch
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
except ImportError:
    SimpleDocTemplate = None

log = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class _Cfg:
    # QuickBase
    QB_REALM = os.environ.get("QUICKBASE_REALM", "dispatchenergy")
    QB_TOKEN = os.environ.get("QUICKBASE_TOKEN", "")
    QB_TABLE_ID = os.environ.get("QB_VENDOR_INVOICES_TABLE", "")
    QB_BASE_URL = "https://api.quickbase.com/v1"

    # Microsoft Graph / Email
    AZURE_TENANT_ID = os.environ.get("AZURE_TENANT_ID", "")
    AZURE_CLIENT_ID = os.environ.get("AZURE_CLIENT_ID", "")
    AZURE_CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET", "")
    SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "acumatica_ap@dispatchenergy.com")
    RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL", "")

    # --- Field IDs (Vendor Invoices table) ---
    FID_RECORD_ID = 3
    FID_VENDOR_NAME = 6
    FID_INVOICE_DESC = 7
    FID_INVOICE_DATE = 8
    FID_INVOICE_DUE_DATE = 9
    FID_INVOICE_NUMBER = 10
    FID_INVOICE_AMOUNT = 11
    FID_PO_NUMBER = 12
    FID_INVOICE_ATTACHMENT = 13
    FID_INVOICE_UPLOADED_DATE = 14
    FID_INVOICE_APPROVAL = 25
    FID_INVOICE_PAYMENT_APPROVAL = 28
    FID_INVOICE_PAID = 30
    FID_PO_BUSINESS_ENTITY = 36
    FID_ACCOUNTING_RESPONSIBILITY = 50
    FID_PO_LEAD_NAME = 64
    FID_PO_PROJECT_NAME = 65
    FID_PO_OPERATING_PROJECT_NAME = 66
    FID_PO_GL_CATEGORY = 75
    FID_PO_GL_DEPARTMENT = 76
    FID_PO_GL_SUBCATEGORY = 77
    FID_PO_RESPONSIBLE_ACCOUNTING = 44

    # Criteria values
    INVOICE_APPROVAL_VALUE = "Invoice Approved for Payment"
    ACCOUNTING_RESPONSIBILITY_VALUE = "BKY Financial"
    VALID_PAYMENT_APPROVALS = {
        "Approved - Pay in Full",
        "Approved - Partial Payment",
        "Approved - Pay with Credit Card",
    }


Cfg = _Cfg()

# ---------------------------------------------------------------------------
# Request model
# ---------------------------------------------------------------------------

class InvoiceWebhookPayload(BaseModel):
    record_id: int
    table_id: Optional[str] = None

# ---------------------------------------------------------------------------
# QuickBase helpers
# ---------------------------------------------------------------------------

def _qb_headers(table_id: Optional[str] = None) -> Dict[str, str]:
    return {
        "QB-Realm-Hostname": f"{Cfg.QB_REALM}.quickbase.com",
        "Authorization": f"QB-USER-TOKEN {Cfg.QB_TOKEN}",
        "Content-Type": "application/json",
    }


def _field(record: Dict, fid: int, default: Any = "") -> Any:
    return record.get(str(fid), {}).get("value", default)


def _fetch_record(record_id: int, table_id: str) -> Optional[Dict]:
    select_fields = [
        Cfg.FID_RECORD_ID, Cfg.FID_VENDOR_NAME, Cfg.FID_INVOICE_DESC,
        Cfg.FID_INVOICE_DATE, Cfg.FID_INVOICE_DUE_DATE, Cfg.FID_INVOICE_NUMBER,
        Cfg.FID_INVOICE_AMOUNT, Cfg.FID_PO_NUMBER, Cfg.FID_INVOICE_ATTACHMENT,
        Cfg.FID_INVOICE_UPLOADED_DATE, Cfg.FID_INVOICE_APPROVAL,
        Cfg.FID_INVOICE_PAYMENT_APPROVAL, Cfg.FID_INVOICE_PAID,
        Cfg.FID_PO_BUSINESS_ENTITY, Cfg.FID_ACCOUNTING_RESPONSIBILITY,
        Cfg.FID_PO_LEAD_NAME, Cfg.FID_PO_PROJECT_NAME,
        Cfg.FID_PO_OPERATING_PROJECT_NAME, Cfg.FID_PO_GL_CATEGORY,
        Cfg.FID_PO_GL_DEPARTMENT, Cfg.FID_PO_GL_SUBCATEGORY,
        Cfg.FID_PO_RESPONSIBLE_ACCOUNTING,
    ]
    body = {
        "from": table_id,
        "select": select_fields,
        "where": f"{{{Cfg.FID_RECORD_ID}.EX.{record_id}}}",
    }
    resp = http_requests.post(
        f"{Cfg.QB_BASE_URL}/records/query",
        headers=_qb_headers(),
        json=body,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json().get("data", [])
    return data[0] if data else None


def _meets_criteria(record: Dict) -> bool:
    approval = _field(record, Cfg.FID_INVOICE_APPROVAL)
    acct_resp = _field(record, Cfg.FID_ACCOUNTING_RESPONSIBILITY)
    payment_approval = _field(record, Cfg.FID_INVOICE_PAYMENT_APPROVAL)

    if approval != Cfg.INVOICE_APPROVAL_VALUE:
        return False
    if acct_resp != Cfg.ACCOUNTING_RESPONSIBILITY_VALUE:
        return False
    if payment_approval not in Cfg.VALID_PAYMENT_APPROVALS:
        return False
    return True


def _download_attachment(record_id: int, table_id: str, field_id: int) -> Optional[Dict]:
    url = f"{Cfg.QB_BASE_URL}/files/{table_id}/{record_id}/{field_id}/0"
    headers = _qb_headers()
    headers.pop("Content-Type", None)

    resp = http_requests.get(url, headers=headers, timeout=60)
    if resp.status_code != 200:
        log.warning(f"Attachment download failed for record {record_id}: {resp.status_code}")
        return None

    content_disp = resp.headers.get("Content-Disposition", "")
    filename = "invoice_attachment"
    if "filename=" in content_disp:
        parts = content_disp.split("filename=")
        if len(parts) > 1:
            filename = parts[1].strip().strip('"').strip("'")

    content_type = resp.headers.get("Content-Type", "application/octet-stream")

    try:
        file_data = base64.b64decode(resp.content)
    except Exception:
        file_data = resp.content

    return {"filename": filename, "content_type": content_type, "data": file_data}


# ---------------------------------------------------------------------------
# Email building
# ---------------------------------------------------------------------------

def _fmt_date(value: Any) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.strftime("%m-%d-%Y")
    except Exception:
        return str(value)


def _fmt_currency(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        return f"${float(value):,.2f}"
    except (ValueError, TypeError):
        return str(value)


def _html(value: Any) -> str:
    if value in (None, ""):
        return ""
    return "<br>".join(escape(line) for line in str(value).splitlines())


def _gl_code_html(record: Dict) -> str:
    parts = []
    for label, fid in [
        ("Department", Cfg.FID_PO_GL_DEPARTMENT),
        ("Category", Cfg.FID_PO_GL_CATEGORY),
        ("Sub-Category", Cfg.FID_PO_GL_SUBCATEGORY),
    ]:
        val = _field(record, fid)
        if val:
            parts.append(f"<b>{escape(label)}:</b> {_html(val)}")
    return "<br>".join(parts)


def _summary_rows(record: Dict) -> List[Dict[str, str]]:
    return [
        {"label": "Vendor Name", "value_html": _html(_field(record, Cfg.FID_VENDOR_NAME))},
        {"label": "Invoice Description", "value_html": _html(_field(record, Cfg.FID_INVOICE_DESC))},
        {"label": "Invoice Date", "value_html": _html(_fmt_date(_field(record, Cfg.FID_INVOICE_DATE)))},
        {"label": "Invoice Uploaded Date", "value_html": _html(_fmt_date(_field(record, Cfg.FID_INVOICE_UPLOADED_DATE)))},
        {"label": "Invoice Due Date", "value_html": _html(_fmt_date(_field(record, Cfg.FID_INVOICE_DUE_DATE)))},
        {"label": "Invoice Number", "value_html": _html(_field(record, Cfg.FID_INVOICE_NUMBER))},
        {"label": "PO Number", "value_html": _html(_field(record, Cfg.FID_PO_NUMBER))},
        {"label": "Business Entity", "value_html": _html(_field(record, Cfg.FID_PO_BUSINESS_ENTITY))},
        {"label": "Business Entity Responsible for Accounting", "value_html": _html(_field(record, Cfg.FID_PO_RESPONSIBLE_ACCOUNTING))},
        {"label": "Lead Name", "value_html": _html(_field(record, Cfg.FID_PO_LEAD_NAME))},
        {"label": "Project Name", "value_html": _html(_field(record, Cfg.FID_PO_PROJECT_NAME))},
        {"label": "Operating Project Name", "value_html": _html(_field(record, Cfg.FID_PO_OPERATING_PROJECT_NAME))},
        {"label": "GL Code", "value_html": _gl_code_html(record)},
        {"label": "Invoice Amount", "value_html": _html(_fmt_currency(_field(record, Cfg.FID_INVOICE_AMOUNT)))},
    ]


def _summary_table_html(record: Dict) -> str:
    rows_html = []
    for row in _summary_rows(record):
        val = row["value_html"] or "&nbsp;"
        rows_html.append(
            "<tr>"
            f'<td style="background:#f5f5f5; width:45%; padding:8px; vertical-align:top; font-weight:600;">{escape(row["label"])}</td>'
            f'<td style="padding:8px; vertical-align:top; word-break:break-word;">{val}</td>'
            "</tr>"
        )
    return (
        '<table border="1" cellpadding="0" cellspacing="0" '
        'style="border-collapse:collapse; border-color:#c7cdd4; width:100%; max-width:700px; table-layout:fixed;">'
        f"{''.join(rows_html)}</table>"
    )


def _build_email_body(record: Dict) -> str:
    payment_approval = _field(record, Cfg.FID_INVOICE_PAYMENT_APPROVAL)
    record_id = _field(record, Cfg.FID_RECORD_ID)
    qb_link = (
        f"https://{Cfg.QB_REALM}.quickbase.com/db/{Cfg.QB_TABLE_ID}"
        f"?a=dr&rid={record_id}"
    )
    return f"""
    <html>
    <body style="font-family: Arial, sans-serif; font-size: 14px; color: #333;">
        <h3 style="color: #2c5aa0;">Quickbase Invoice - Payment Approved</h3>
        <p>Payment approval status: <b>{_html(payment_approval)}</b></p>
        {_summary_table_html(record)}
        <p style="margin-top: 12px; font-size: 12px; color: #888;">
            <a href="{escape(qb_link, quote=True)}">View record in QuickBase</a> &nbsp;|&nbsp;
            Record #{escape(str(record_id))}
        </p>
    </body>
    </html>
    """


# ---------------------------------------------------------------------------
# PDF summary page (appended to invoice attachment)
# ---------------------------------------------------------------------------

def _is_pdf(attachment: Dict) -> bool:
    fn = str(attachment.get("filename", "")).lower()
    ct = str(attachment.get("content_type", "")).lower()
    data = attachment.get("data", b"")
    return fn.endswith(".pdf") or "application/pdf" in ct or bytes(data[:5]) == b"%PDF-"


def _build_summary_pdf(record: Dict) -> bytes:
    if SimpleDocTemplate is None:
        return b""

    buffer = io.BytesIO()
    margin = 0.55 * inch
    doc = SimpleDocTemplate(buffer, pagesize=letter,
                            leftMargin=margin, rightMargin=margin,
                            topMargin=margin, bottomMargin=margin)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("T", parent=styles["Heading1"],
                                  fontName="Helvetica-Bold", fontSize=18, leading=22,
                                  textColor=colors.HexColor("#2c5aa0"), spaceAfter=6)
    meta_style = ParagraphStyle("M", parent=styles["BodyText"],
                                 fontName="Helvetica", fontSize=9, leading=12,
                                 textColor=colors.HexColor("#5d6673"), spaceAfter=14)
    lbl = ParagraphStyle("L", parent=styles["BodyText"],
                          fontName="Helvetica-Bold", fontSize=10, leading=12)
    val = ParagraphStyle("V", parent=styles["BodyText"],
                          fontName="Helvetica", fontSize=10, leading=12)

    rid = _field(record, Cfg.FID_RECORD_ID)
    vendor = _field(record, Cfg.FID_VENDOR_NAME)
    inv_num = _field(record, Cfg.FID_INVOICE_NUMBER)
    meta_parts = []
    if rid:
        meta_parts.append(f"QuickBase Record #{escape(str(rid))}")
    if vendor:
        meta_parts.append(escape(str(vendor)))
    if inv_num:
        meta_parts.append(f"Invoice #{escape(str(inv_num))}")

    rows = []
    for r in _summary_rows(record):
        markup = (r["value_html"] or "&nbsp;").replace("<br>", "<br/>")
        rows.append([Paragraph(escape(r["label"]), lbl), Paragraph(markup, val)])

    avail = letter[0] - 2 * margin
    tbl = Table(rows, colWidths=[avail * 0.45, avail * 0.55], hAlign="LEFT")
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f5f5f5")),
        ("GRID", (0, 0), (-1, -1), 0.75, colors.HexColor("#c7cdd4")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))

    doc.build([
        Paragraph("Invoice Summary", title_style),
        Paragraph(" | ".join(meta_parts), meta_style),
        Spacer(1, 4),
        tbl,
    ])
    return buffer.getvalue()


def _append_summary_page(record: Dict, attachment: Dict) -> Dict:
    if not _is_pdf(attachment):
        return attachment
    try:
        from pypdf import PdfReader, PdfWriter
    except ImportError:
        log.warning("pypdf not installed — skipping summary page append")
        return attachment

    try:
        summary_pdf = _build_summary_pdf(record)
        if not summary_pdf:
            return attachment

        writer = PdfWriter()
        for page in PdfReader(io.BytesIO(attachment["data"])).pages:
            writer.add_page(page)
        for page in PdfReader(io.BytesIO(summary_pdf)).pages:
            writer.add_page(page)

        out = io.BytesIO()
        writer.write(out)
        return {**attachment, "data": out.getvalue(), "content_type": "application/pdf"}
    except Exception as exc:
        log.warning(f"Failed to append summary page: {exc}")
        return attachment


# ---------------------------------------------------------------------------
# Microsoft Graph email
# ---------------------------------------------------------------------------

def _get_graph_token() -> str:
    if msal is None:
        raise RuntimeError("msal package not installed")
    authority = f"https://login.microsoftonline.com/{Cfg.AZURE_TENANT_ID}"
    app = msal.ConfidentialClientApplication(
        client_id=Cfg.AZURE_CLIENT_ID,
        client_credential=Cfg.AZURE_CLIENT_SECRET,
        authority=authority,
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" in result:
        return result["access_token"]
    raise RuntimeError(f"Graph token acquisition failed: {result.get('error_description')}")


def _send_email(graph_token: str, record: Dict, attachment: Optional[Dict]) -> bool:
    vendor = _field(record, Cfg.FID_VENDOR_NAME)
    inv_num = _field(record, Cfg.FID_INVOICE_NUMBER)
    po_num = _field(record, Cfg.FID_PO_NUMBER)
    rid = _field(record, Cfg.FID_RECORD_ID)

    subject = f"Quickbase Invoice Created - {vendor} - {inv_num} (PO: {po_num})"

    message: Dict[str, Any] = {
        "subject": subject,
        "body": {"contentType": "HTML", "content": _build_email_body(record)},
        "toRecipients": [{"emailAddress": {"address": Cfg.RECIPIENT_EMAIL}}],
    }

    if attachment:
        message["attachments"] = [{
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": attachment["filename"],
            "contentType": attachment["content_type"],
            "contentBytes": base64.b64encode(attachment["data"]).decode("utf-8"),
        }]

    url = f"https://graph.microsoft.com/v1.0/users/{Cfg.SENDER_EMAIL}/sendMail"
    resp = http_requests.post(
        url,
        headers={"Authorization": f"Bearer {graph_token}", "Content-Type": "application/json"},
        json={"message": message, "saveToSentItems": "true"},
        timeout=30,
    )

    if resp.status_code == 202:
        log.info(f"Email sent for Record #{rid}: {subject}")
        return True
    log.error(f"Email send failed for Record #{rid}: {resp.status_code} - {resp.text[:300]}")
    return False


# ---------------------------------------------------------------------------
# Webhook endpoint
# ---------------------------------------------------------------------------

@router.post("/webhook/invoice")
async def handle_invoice_webhook(request: Request):
    """
    Receive a webhook from QuickBase when an invoice record is modified.

    Accepts either:
      - JSON body: {"record_id": 12345, "table_id": "bqr..."}
      - Form/query params from QB Automations
    """
    # Parse payload — QB automations can send JSON or form data
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        body = await request.json()
    else:
        form = await request.form()
        body = dict(form)

    record_id = body.get("record_id")
    if record_id is None:
        # QB may nest under different keys — try common patterns
        record_id = body.get("rid") or body.get("Record ID#") or body.get("recordId")

    if record_id is None:
        raise HTTPException(status_code=400, detail="record_id is required")

    try:
        record_id = int(record_id)
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="record_id must be an integer")

    table_id = body.get("table_id") or Cfg.QB_TABLE_ID
    if not table_id:
        raise HTTPException(status_code=400, detail="table_id not provided and QB_VENDOR_INVOICES_TABLE not set")

    log.info(f"Invoice webhook received: Record #{record_id}, Table {table_id}")

    # 1. Fetch the full record from QuickBase
    try:
        record = _fetch_record(record_id, table_id)
    except Exception as exc:
        log.error(f"Failed to fetch record #{record_id}: {exc}")
        raise HTTPException(status_code=502, detail=f"QuickBase query failed: {exc}")

    if not record:
        raise HTTPException(status_code=404, detail=f"Record #{record_id} not found")

    # 2. Check criteria
    if not _meets_criteria(record):
        approval = _field(record, Cfg.FID_INVOICE_APPROVAL)
        acct_resp = _field(record, Cfg.FID_ACCOUNTING_RESPONSIBILITY)
        pay_appr = _field(record, Cfg.FID_INVOICE_PAYMENT_APPROVAL)
        log.info(
            f"Record #{record_id} does not meet criteria — "
            f"Approval={approval!r}, AcctResp={acct_resp!r}, PayApproval={pay_appr!r}"
        )
        return {
            "status": "skipped",
            "record_id": record_id,
            "reason": "Record does not meet forwarding criteria",
        }

    # 3. Download attachment (if any)
    attachment = None
    if _field(record, Cfg.FID_INVOICE_ATTACHMENT):
        attachment = _download_attachment(record_id, table_id, Cfg.FID_INVOICE_ATTACHMENT)
        if attachment:
            attachment = _append_summary_page(record, attachment)

    # 4. Send email
    try:
        graph_token = _get_graph_token()
    except Exception as exc:
        log.error(f"Graph token error: {exc}")
        raise HTTPException(status_code=502, detail=f"Email auth failed: {exc}")

    success = _send_email(graph_token, record, attachment)
    if not success:
        raise HTTPException(status_code=502, detail="Email send failed")

    vendor = _field(record, Cfg.FID_VENDOR_NAME)
    inv_num = _field(record, Cfg.FID_INVOICE_NUMBER)
    log.info(f"Record #{record_id} forwarded successfully: {vendor} / {inv_num}")

    return {
        "status": "forwarded",
        "record_id": record_id,
        "vendor": vendor,
        "invoice_number": inv_num,
    }
