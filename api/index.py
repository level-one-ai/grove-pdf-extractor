"""
Grove PDF Extractor
===================
Serverless Python function deployed on Vercel.

Accepts a PDF file via HTTP POST from Make.com, extracts structured data
from Grove Bedding delivery orders, and returns JSON.

Flow:
  1. Receive POST with multipart/form-data containing a PDF file
  2. Detect whether PDF has a text layer (digital) or is a scanned image
  3. Digital PDF  -> extract text with pdfplumber (free, instant)
     Scanned PDF  -> send to OCR.space API to get plain text (free, 500 req/day)
  4. Gatekeeper: if "Delivery Order" not in text -> return {document_type: "other"}
  5. Extraction: regex-based parsing that handles OCR spacing variations
  6. Return structured JSON matching the Grove PDF Router schema

Environment variables required on Vercel:
  OCR_SPACE_API_KEY  -- your free API key from https://ocr.space/ocrapi/freekey
"""

import json
import io
import re
import cgi
import os
import urllib.request
import urllib.error
import pdfplumber
from http.server import BaseHTTPRequestHandler


# ── Text extraction ────────────────────────────────────────────────────────────

def is_scanned_pdf(pdf_bytes):
    """Return True if PDF has no meaningful text layer (i.e. it is a scanned image)."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            total_chars = sum(len(page.chars) for page in pdf.pages)
            return total_chars < 20
    except Exception:
        return True


def extract_text_with_pdfplumber(pdf_bytes):
    """Extract text from a digital PDF using pdfplumber."""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        pages = []
        for page in pdf.pages:
            text = page.extract_text() or ""
            pages.append(text)
        return "\n".join(pages)


def extract_text_with_ocrspace(pdf_bytes):
    """
    Send a single-page PDF to OCR.space and return plain text.
    Free tier: 500 requests/day, no per-second rate limit.
    Each page arrives already split by the Grove PDF Router.
    """
    api_key = os.environ.get("OCR_SPACE_API_KEY", "helloworld")

    boundary = "----GrovePDFBoundary"
    body_parts = []

    def add_field(name, value):
        body_parts.append(("--" + boundary).encode())
        body_parts.append(('Content-Disposition: form-data; name="' + name + '"').encode())
        body_parts.append(b"")
        body_parts.append(value if isinstance(value, bytes) else value.encode())

    add_field("apikey", api_key)
    add_field("language", "eng")
    add_field("OCREngine", "2")
    add_field("scale", "true")
    # NOTE: isTable NOT used — it changes output format in ways that break parsing
    # Plain text output is more reliable for our regex-based extraction

    body_parts.append(("--" + boundary).encode())
    body_parts.append(b'Content-Disposition: form-data; name="file"; filename="page.pdf"')
    body_parts.append(b"Content-Type: application/pdf")
    body_parts.append(b"")
    body_parts.append(pdf_bytes)
    body_parts.append(("--" + boundary + "--").encode())

    body = b"\r\n".join(body_parts)

    req = urllib.request.Request(
        "https://api.ocr.space/parse/image",
        data=body,
        headers={"Content-Type": "multipart/form-data; boundary=" + boundary},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=55) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    parsed = result.get("ParsedResults", [])
    if not parsed:
        error_msg = result.get("ErrorMessage", ["Unknown OCR error"])
        if isinstance(error_msg, list):
            error_msg = " ".join(error_msg)
        raise ValueError("OCR.space error: " + error_msg)

    return "\n".join(p.get("ParsedText", "") for p in parsed)


def get_pdf_text(pdf_bytes):
    """Route to correct text extractor based on PDF type."""
    if is_scanned_pdf(pdf_bytes):
        return extract_text_with_ocrspace(pdf_bytes)
    return extract_text_with_pdfplumber(pdf_bytes)


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean(value):
    """Strip whitespace and collapse internal spaces."""
    if value is None:
        return ""
    v = str(value).strip()
    v = re.sub(r'\s+', ' ', v)
    return v.strip()


def find_by_regex(text, *patterns):
    """
    Try each regex pattern in order, return first captured group found.
    All patterns should have exactly one capture group for the value.
    """
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return clean(m.group(1))
    return ""


# ── Header extraction ─────────────────────────────────────────────────────────

def _extract_etd_ref(text):
    """
    Handle columnar header layout where labels are on one line and
    values are on the next. OCR collapses multi-spaces so we use
    regex on the values line rather than character positions.
    Pattern: line with 2+ of [ETD, Ref, Invoice, Customer PO]
             followed by a line starting with a date or code.
    """
    etd = ""
    ref = ""
    lines = text.split("\n")
    label_kws = ["etd", "ref", "invoice", "customer po"]
    for i, line in enumerate(lines):
        ll = line.lower()
        if sum(1 for kw in label_kws if kw in ll) >= 2:
            # Find next non-empty line — that is the values line
            for j in range(i + 1, min(i + 4, len(lines))):
                vline = lines[j].strip()
                if not vline:
                    continue
                # Values line should NOT be another header
                if sum(1 for kw in label_kws if kw in vline.lower()) >= 2:
                    continue
                # Extract date (ETD)
                dm = re.search(r'(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})', vline)
                if dm:
                    etd = dm.group(1).strip()
                    # Ref is whatever comes after the date
                    after_date = vline[dm.end():].strip()
                    rm = re.search(r'([A-Z]{2,}\d+[\w\-]*)', after_date)
                    if rm:
                        ref = rm.group(1)
                # Try date as dd/mm/yyyy
                if not etd:
                    dm2 = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', vline)
                    if dm2:
                        etd = dm2.group(1).strip()
                        after = vline[dm2.end():].strip()
                        rm2 = re.search(r'([A-Z]{2,}\d+[\w\-]*)', after)
                        if rm2:
                            ref = rm2.group(1)
                break
        if etd:
            break
    return etd, ref


def extract_header(text):
    """
    Extract header fields using regex patterns that work regardless of
    column spacing, line breaks, or OCR spacing variations.
    """

    # Brand name: first non-empty line that isn't "Delivery Order"
    title = ""
    for line in text.split("\n"):
        stripped = clean(line)
        if stripped and "delivery order" not in stripped.lower():
            title = stripped
            break

    # ETD + Ref: detect the header label row then parse values from next line
    # OCR collapses column spacing, so we find the values line and parse it
    etd, ref = _extract_etd_ref(text)
    if not etd:
        etd = find_by_regex(
            text,
            r'ETD[\s:]+(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})',
            r'ETD[\s:]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        )
    if not ref:
        ref = find_by_regex(
            text,
            r'Ref[\s:]+([A-Z]{2,}\d+[\w\-]*)',
        )

    # Invoice Number — must contain at least one digit to avoid matching label words
    inv_no = find_by_regex(
        text,
        r'Invoice\s+(?:Number|No)[\s:]+((?=[A-Z0-9\-/]*\d)[A-Z0-9\-/]+)',
        r'Inv(?:oice)?[\s.#:]+((?=[A-Z0-9\-/]*\d)[A-Z0-9\-/]+)',
    )

    # Customer PO Number — must contain at least one digit
    customer_po_no = find_by_regex(
        text,
        r'Customer\s+PO\s+No[\s:]+((?=[A-Z0-9\-/]*\d)[A-Z0-9\-/]+)',
        r'Customer\s+PO[\s:]+((?=[A-Z0-9\-/]*\d)[A-Z0-9\-/]+)',
        r'PO\s+No[\s:]+((?=[A-Z0-9\-/]*\d)[A-Z0-9\-/]+)',
        r'Purchase\s+Order[\s:]+((?=[A-Z0-9\-/]*\d)[A-Z0-9\-/]+)',
    )

    return {
        "title": title,
        "etd": etd,
        "ref": ref,
        "inv_no": inv_no,
        "customer_po_no": customer_po_no,
    }


# ── Address extraction ────────────────────────────────────────────────────────

UK_POSTCODE_RE = re.compile(
    r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b', re.IGNORECASE
)


def parse_address_lines(lines):
    """
    Parse a list of address lines into structured fields.
    Finds UK postcode by regex and works outward from it.
    """
    # Filter out phone/mobile/empty lines
    addr_lines = []
    phone = ""
    mobile = ""
    for line in lines:
        l = clean(line)
        if not l:
            continue
        if re.search(r'\bphone\b|\btel\b', l, re.IGNORECASE):
            m = re.search(r'[\d][\d\s\-\+\(\)]{6,}', l)
            if m:
                phone = clean(m.group())
            continue
        if re.search(r'\bmobile\b|\bmob\b', l, re.IGNORECASE):
            m = re.search(r'[\d][\d\s\-\+\(\)]{6,}', l)
            if m:
                mobile = clean(m.group())
            continue
        addr_lines.append(l)

    postcode = ""
    postcode_idx = None
    for i, line in enumerate(addr_lines):
        m = UK_POSTCODE_RE.search(line)
        if m:
            postcode = clean(m.group(1))
            postcode_idx = i
            break

    street = ""
    city = ""
    region = ""
    country = "United Kingdom"

    if postcode_idx is not None:
        pre = addr_lines[:postcode_idx]
        post = addr_lines[postcode_idx + 1:]

        # Last pre-postcode line is "City, Region" or just "City"
        if pre:
            # Check if postcode is on the same line as city/region
            pc_line = addr_lines[postcode_idx]
            before_pc = UK_POSTCODE_RE.split(pc_line)[0].strip().rstrip(",").strip()
            if before_pc:
                # "Abingdon, Oxfordshire" or "Abingdon"
                if "," in before_pc:
                    parts = before_pc.split(",", 1)
                    city = clean(parts[0])
                    region = clean(parts[1])
                else:
                    city = clean(before_pc)
                street = ", ".join(pre)
            else:
                city_line = pre[-1]
                if "," in city_line:
                    parts = city_line.split(",", 1)
                    city = clean(parts[0])
                    region = clean(parts[1])
                else:
                    city = clean(city_line)
                street = ", ".join(pre[:-1])
        country = clean(post[0]) if post else "United Kingdom"
    else:
        street = ", ".join(addr_lines[:-1]) if len(addr_lines) > 1 else (addr_lines[0] if addr_lines else "")
        country = addr_lines[-1] if len(addr_lines) > 1 else ""

    return {
        "street": street,
        "city": city,
        "region": region,
        "postcode": postcode,
        "country": country,
    }, phone, mobile


def extract_section(text, start_pattern, end_pattern):
    """
    Extract lines between start_pattern and end_pattern using regex boundaries.
    Returns list of non-empty lines.
    """
    m_start = re.search(start_pattern, text, re.IGNORECASE)
    if not m_start:
        return []

    after_start = text[m_start.end():]

    m_end = re.search(end_pattern, after_start, re.IGNORECASE)
    section_text = after_start[:m_end.start()] if m_end else after_start

    lines = [clean(l) for l in section_text.split("\n") if clean(l)]
    return lines


# ── Customer and Ship To extraction ──────────────────────────────────────────

def extract_customer(text):
    """
    Extract customer details.
    Handles two-column OCR layouts where Customer and Ship To appear
    side-by-side on the same lines.
    """
    # Find the section between "Customer:" and "Ship To:" or "Product Selection"
    lines = extract_section(
        text,
        r'Customer\s*:',
        r'Ship\s+To\s*:|Product\s+Selection|Handwritten'
    )

    if not lines:
        return None, "", {"street": "", "city": "", "region": "", "postcode": "", "country": ""}, "", ""

    # If OCR merged Customer and Ship To columns, lines may contain both
    # e.g. "Abingdon Beds Ltd Abingdon Beds Ltd" — clean by taking first half
    cleaned = []
    for line in lines:
        # Detect duplicated content (OCR reads two-column layout left-to-right)
        words = line.split()
        half = len(words) // 2
        if half > 1 and words[:half] == words[half:]:
            # Line is exactly duplicated — take first half only
            line = " ".join(words[:half])
        cleaned.append(clean(line))

    company_name = cleaned[0] if cleaned else None
    name = cleaned[1] if len(cleaned) > 1 else ""
    addr_lines = cleaned[2:] if len(cleaned) > 2 else []

    address, phone, mobile = parse_address_lines(addr_lines)

    # If phone not found in block, search full text
    if not phone:
        m = re.search(r'Phone[\s:]+(\d[\d\s\-\+\(\)]{6,})', text, re.IGNORECASE)
        if m:
            phone = clean(m.group(1))
    if not mobile:
        m = re.search(r'Mobile[\s:]+(\d[\d\s\-\+\(\)]{6,})', text, re.IGNORECASE)
        if m:
            mobile = clean(m.group(1))

    return company_name, name, address, phone, mobile


def extract_ship_to(text):
    """
    Extract Ship To details.
    Handles two-column OCR where Ship To appears to the right of Customer.
    """
    lines = extract_section(
        text,
        r'Ship\s+To\s*:',
        r'Product\s+Selection|Handwritten|Balance\s+owing|Authorisation'
    )

    if not lines:
        return "", {"street": "", "city": "", "region": "", "postcode": "", "country": ""}

    # Remove duplicated column content same as customer
    cleaned = []
    for line in lines:
        words = line.split()
        half = len(words) // 2
        if half > 1 and words[:half] == words[half:]:
            line = " ".join(words[:half])
        cleaned.append(clean(line))

    name = cleaned[0] if cleaned else ""
    addr_lines = cleaned[1:] if len(cleaned) > 1 else []
    address, _, _ = parse_address_lines(addr_lines)

    return name, address


# ── Product extraction ────────────────────────────────────────────────────────

SKU_RE = re.compile(r'^\d{5}/\d{5}', re.IGNORECASE)
SECTION_END_RE = re.compile(
    r'Balance\s+owing|Authorisation|Authorization|VAT\s+NO|Total\b',
    re.IGNORECASE
)
PRODUCT_HEADER_RE = re.compile(r'\bItem\b|\bOptions\b|\bQty\b', re.IGNORECASE)


def extract_products(text):
    """
    Extract product rows from the Product Selection section.
    Handles plain text table output from OCR.space (no isTable flag).

    Each product row looks like:
      "Ocean Dream                    Mattress Size: 5'0 King              1"
    Followed by optional SKU lines:
      "15373/65000 (X1)"

    Strategy: find "Product Selection", read lines until end marker,
    skip header and SKU lines, parse each data row by splitting off
    the trailing integer (qty) and detecting the options separator.
    """
    lines = extract_section(
        text,
        r'Product\s+Selection',
        r'Balance\s+owing|Authorisation|Authorization|VAT\s+NO'
    )

    products = []
    for line in lines:
        if not line:
            continue
        # Skip the column header row
        if PRODUCT_HEADER_RE.search(line) and len(line.split()) <= 6:
            continue
        # Skip SKU lines like "15373/65000 (X1)"
        if SKU_RE.match(line):
            continue
        # Skip separator lines
        if re.match(r'^[\-\s\.]+$', line):
            continue

        # Try to split into item / options / qty
        # Qty is always a trailing integer (1, 2, 3...)
        qty_match = re.search(r'\s+(\d+)\s*$', line)
        if not qty_match:
            continue

        qty = qty_match.group(1)
        remainder = line[:qty_match.start()].strip()

        # Options typically start with "Mattress Size:" or similar keyword
        # Split on the first occurrence of a known options pattern
        # First try keyword-based split (works even when OCR collapses spaces)
        options_match = re.search(
            r'(Mattress\s+Size[:\s]|Colour[:\s]|Color[:\s]|Size[:\s]|Type[:\s]|Option[:\s])',
            remainder, re.IGNORECASE
        )
        if options_match:
            item = remainder[:options_match.start()].strip()
            options = remainder[options_match.start():].strip()
        else:
            # Fallback: split on 2+ spaces
            parts = re.split(r'\s{2,}', remainder, maxsplit=1)
            item = parts[0].strip()
            options = parts[1].strip() if len(parts) > 1 else ""

        if item:
            products.append({
                "item": item,
                "options": options,
                "qty": qty,
            })

    return products


# ── Response templates ────────────────────────────────────────────────────────

def other_response():
    return {"document_type": "other", "document": None}


def build_response(header, company_name, cust_name, cust_addr, phone, mobile,
                   ship_name, ship_addr, products):
    return {
        "document_type": "delivery_order",
        "document": {
            "header": header,
            "customer": {
                "company_name": company_name,
                "name": cust_name,
                "address": cust_addr,
                "phone": phone,
                "mobile": mobile,
            },
            "ship_to": {
                "name": ship_name,
                "address": ship_addr,
            },
            "handwritten": {},
            "product_selection": products,
        },
    }


# ── Main extraction ───────────────────────────────────────────────────────────

def extract_delivery_order(pdf_bytes):
    full_text = get_pdf_text(pdf_bytes)

    # Gatekeeper
    if "delivery order" not in full_text.lower():
        return other_response()

    header = extract_header(full_text)
    company_name, cust_name, cust_addr, phone, mobile = extract_customer(full_text)
    ship_name, ship_addr = extract_ship_to(full_text)
    products = extract_products(full_text)

    return build_response(
        header, company_name, cust_name, cust_addr, phone, mobile,
        ship_name, ship_addr, products
    )


# ── HTTP Handler ──────────────────────────────────────────────────────────────

class handler(BaseHTTPRequestHandler):

    def do_POST(self):
        try:
            content_type = self.headers.get("Content-Type", "")
            pdf_bytes = None

            if "multipart/form-data" in content_type:
                form = cgi.FieldStorage(
                    fp=self.rfile,
                    headers=self.headers,
                    environ={"REQUEST_METHOD": "POST", "CONTENT_TYPE": content_type},
                )
                for field_name in ["file", "pdf", "document"]:
                    if field_name in form and hasattr(form[field_name], "file"):
                        pdf_bytes = form[field_name].file.read()
                        break
                if pdf_bytes is None:
                    for key in form.keys():
                        item = form[key]
                        if hasattr(item, "file") and item.file:
                            pdf_bytes = item.file.read()
                            break

            elif "application/pdf" in content_type or "application/octet-stream" in content_type:
                content_length = int(self.headers.get("Content-Length", 0))
                pdf_bytes = self.rfile.read(content_length)

            else:
                self._send_json(400, {"error": "Send PDF as multipart/form-data (field: 'file') or application/pdf body."})
                return

            if not pdf_bytes:
                self._send_json(400, {"error": "No PDF file found in request."})
                return

            result = extract_delivery_order(pdf_bytes)
            self._send_json(200, result)

        except Exception as e:
            self._send_json(500, {"error": str(e)})

    def do_GET(self):
        self._send_json(200, {
            "status": "ok",
            "service": "Grove PDF Extractor",
            "note": "POST a PDF as multipart/form-data (field: 'file'). Set OCR_SPACE_API_KEY on Vercel.",
        })

    def _send_json(self, status_code, data):
        body = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass
