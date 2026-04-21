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
  5. Extraction: parse header, customer, ship_to, product_selection table
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
import urllib.parse
import urllib.error
import pdfplumber
from http.server import BaseHTTPRequestHandler


# ── Text extraction ────────────────────────────────────────────────────────────

def is_scanned_pdf(pdf_bytes):
    """Return True if PDF has no meaningful text layer (i.e. it is a scanned image)."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            total_chars = sum(len(page.chars) for page in pdf.pages)
            return total_chars < 20  # fewer than 20 chars = scanned
    except Exception:
        return True


def extract_text_with_pdfplumber(pdf_bytes):
    """Extract text from a digital PDF using pdfplumber."""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        lines = []
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines.append(text)
        return "\n".join(lines)


def extract_text_with_ocrspace(pdf_bytes):
    """
    Send a single-page PDF to OCR.space API and return extracted plain text.
    Free tier: 500 requests/day, no per-second rate limit, no page size limit.
    Each page arrives here already split by the Grove PDF Router — always 1 page.

    Environment variable: OCR_SPACE_API_KEY
    Get a free key at: https://ocr.space/ocrapi/freekey
    """
    api_key = os.environ.get("OCR_SPACE_API_KEY", "helloworld")
    # Note: "helloworld" is OCR.space's public test key — severely rate limited.
    # Always set OCR_SPACE_API_KEY in Vercel environment variables.

    # OCR.space accepts multipart/form-data with the file as a field named "file"
    boundary = "----GrovePDFBoundary"
    body_parts = []

    # -- apikey field
    body_parts.append(f"--{boundary}".encode())
    body_parts.append(b'Content-Disposition: form-data; name="apikey"')
    body_parts.append(b"")
    body_parts.append(api_key.encode())

    # -- language field (English)
    body_parts.append(f"--{boundary}".encode())
    body_parts.append(b'Content-Disposition: form-data; name="language"')
    body_parts.append(b"")
    body_parts.append(b"eng")

    # -- OCREngine field (Engine 2 — better accuracy, auto language detection)
    body_parts.append(f"--{boundary}".encode())
    body_parts.append(b'Content-Disposition: form-data; name="OCREngine"')
    body_parts.append(b"")
    body_parts.append(b"2")

    # -- isTable field — improves table recognition for product grids
    body_parts.append(f"--{boundary}".encode())
    body_parts.append(b'Content-Disposition: form-data; name="isTable"')
    body_parts.append(b"")
    body_parts.append(b"true")

    # -- scale field — upscale small/low-res scans for better accuracy
    body_parts.append(f"--{boundary}".encode())
    body_parts.append(b'Content-Disposition: form-data; name="scale"')
    body_parts.append(b"")
    body_parts.append(b"true")

    # -- file field — the PDF bytes
    body_parts.append(f"--{boundary}".encode())
    body_parts.append(b'Content-Disposition: form-data; name="file"; filename="page.pdf"')
    body_parts.append(b"Content-Type: application/pdf")
    body_parts.append(b"")
    body_parts.append(pdf_bytes)

    # Closing boundary
    body_parts.append(f"--{boundary}--".encode())

    # Join with CRLF as required by multipart spec
    body = b"\r\n".join(body_parts)

    req = urllib.request.Request(
        "https://api.ocr.space/parse/image",
        data=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=55) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    # OCR.space returns ParsedResults — extract plain text from each page result
    parsed = result.get("ParsedResults", [])
    if not parsed:
        error_msg = result.get("ErrorMessage", ["Unknown OCR error"])
        if isinstance(error_msg, list):
            error_msg = " ".join(error_msg)
        raise ValueError(f"OCR.space error: {error_msg}")

    # Join all parsed page text (will be 1 page since router pre-splits)
    full_text = "\n".join(
        p.get("ParsedText", "") for p in parsed
    )
    return full_text


def get_pdf_text(pdf_bytes):
    """
    Smart text extraction:
    - Digital PDFs: pdfplumber reads the text layer directly (free, instant)
    - Scanned PDFs: OCR.space API reads the image and returns plain text (free)
    Since the Grove PDF Router pre-splits all PDFs into single pages before
    calling this extractor, every PDF received here is always 1 page.
    """
    if is_scanned_pdf(pdf_bytes):
        return extract_text_with_ocrspace(pdf_bytes)
    else:
        return extract_text_with_pdfplumber(pdf_bytes)


# ── Helpers ──────────────────────────────────────────────────────────────────

def clean(value):
    """Strip whitespace and return empty string if None."""
    if value is None:
        return ""
    v = str(value).strip()
    v = re.sub(r'\s+', ' ', v)   # collapse multiple spaces
    return v.strip()


def find_value_after_label(lines, *labels):
    """
    Search through lines for a label and return the text that follows it.
    Handles both same-line ("ETD: 15 Apr 2026") and next-line values.
    Also handles markdown table format (| ETD | 15 Apr 2026 |).
    """
    for i, line in enumerate(lines):
        line_stripped = clean(line)
        line_upper = line_stripped.upper()
        for label in labels:
            label_upper = label.upper()
            if label_upper in line_upper:
                # Try same line: "ETD: 15 Apr 2026" or "ETD 15 Apr 2026"
                after = re.split(re.escape(label), line_stripped, flags=re.IGNORECASE, maxsplit=1)
                if len(after) > 1:
                    value = clean(after[1].lstrip(":").lstrip("|").strip())
                    # If value looks like another label, skip it
                    if value and not any(lbl.upper() in value.upper() for lbl in ["REF", "INVOICE", "CUSTOMER", "ETD", "SHIP"]):
                        return value
                # Try next line
                if i + 1 < len(lines):
                    next_val = clean(lines[i + 1])
                    if next_val and not next_val.startswith("|"):
                        return next_val
    return ""


def extract_block(lines, start_label, end_labels):
    """
    Extract lines starting after start_label, ending before any end_label.
    Handles both plain text and markdown-formatted output from Mistral OCR.
    """
    block = []
    in_block = False
    for line in lines:
        stripped = clean(line)
        if not stripped:
            continue
        # Skip markdown table separator lines
        if re.match(r'^[\|\-\s]+$', stripped):
            continue
        if start_label.upper() in stripped.upper():
            in_block = True
            # Check if value is on the same line after the label
            after = re.split(re.escape(start_label), stripped, flags=re.IGNORECASE, maxsplit=1)
            if len(after) > 1 and clean(after[1].lstrip(":")):
                block.append(clean(after[1].lstrip(":")))
            continue
        if in_block:
            if any(lbl.upper() in stripped.upper() for lbl in end_labels):
                break
            # Strip markdown pipe chars from table cells
            stripped = stripped.strip("|").strip()
            if stripped:
                block.append(stripped)
    return block


def parse_address_block(block):
    """
    Parse address lines into structured fields.
    Identifies UK postcode with regex, works backward from there.
    """
    UK_POSTCODE = re.compile(
        r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b', re.IGNORECASE
    )

    street_lines = []
    city = ""
    region = ""
    postcode = ""
    country = ""

    # Find the postcode line
    postcode_idx = None
    for i, line in enumerate(block):
        m = UK_POSTCODE.search(line)
        if m:
            postcode = clean(m.group(1))
            postcode_idx = i
            break

    if postcode_idx is not None:
        pre = block[:postcode_idx]
        post = block[postcode_idx + 1:]

        if pre:
            # Last pre-postcode line is city or "City, Region"
            city_line = pre[-1]
            if "," in city_line:
                parts = city_line.split(",", 1)
                city = clean(parts[0])
                region = clean(parts[1])
            else:
                # Check if postcode line itself has "City Region" before the postcode
                postcode_line = block[postcode_idx]
                before_pc = UK_POSTCODE.split(postcode_line)[0].strip().rstrip(",").strip()
                if before_pc:
                    if "," in before_pc:
                        parts = before_pc.split(",", 1)
                        city = clean(parts[0])
                        region = clean(parts[1])
                    else:
                        city = clean(before_pc)
                else:
                    city = clean(city_line)
            street_lines = pre[:-1]
        country = clean(post[0]) if post else "United Kingdom"
    else:
        # No postcode found — use last line as country guess
        street_lines = block[:-1] if len(block) > 1 else block
        country = clean(block[-1]) if len(block) > 1 else ""

    return {
        "street": ", ".join(street_lines),
        "city": city,
        "region": region,
        "postcode": postcode,
        "country": country,
    }


def extract_contact(lines):
    """Extract phone and mobile numbers from lines."""
    phone = ""
    mobile = ""
    for line in lines:
        if re.search(r'phone|tel(?![\w])', line, re.IGNORECASE):
            m = re.search(r'[\d][\d\s\-\+\(\)]{6,}', line)
            if m:
                phone = clean(m.group())
        if re.search(r'mobile|mob(?![\w])', line, re.IGNORECASE):
            m = re.search(r'[\d][\d\s\-\+\(\)]{6,}', line)
            if m:
                mobile = clean(m.group())
    return phone, mobile


def extract_product_table_from_text(lines):
    """
    Extract product selection from text lines when table extraction isn't available.
    Looks for the Product Selection section and parses item/options/qty rows.
    Works with both plain text and Mistral OCR markdown table output.
    """
    products = []
    in_table = False
    header_found = False

    # Column indices from header
    item_col = 0
    opt_col = 1
    qty_col = -1

    for line in lines:
        stripped = clean(line)
        if not stripped:
            continue

        # Detect start of product section
        if "product selection" in stripped.lower():
            in_table = True
            continue

        if not in_table:
            continue

        # Detect end of product section
        if any(kw in stripped.lower() for kw in ["balance owing", "authorisation", "authorization", "vat no", "total"]):
            break

        # Parse markdown table rows: | Item | Options | Qty |
        if "|" in stripped:
            cells = [c.strip() for c in stripped.split("|") if c.strip()]
            if not cells:
                continue

            # Header row
            if not header_found:
                row_lower = " ".join(cells).lower()
                if any(kw in row_lower for kw in ["item", "product", "qty", "options"]):
                    header_found = True
                    for i, h in enumerate(cells):
                        h_lower = h.lower()
                        if "item" in h_lower or "product" in h_lower or "desc" in h_lower:
                            item_col = i
                        elif "option" in h_lower or "colour" in h_lower or "size" in h_lower:
                            opt_col = i
                        elif "qty" in h_lower or "quantity" in h_lower:
                            qty_col = i
                    continue
                # Separator row --- skip
                if re.match(r'^[\-\s\|]+$', stripped):
                    continue

            # Data row
            if header_found and len(cells) >= 2:
                def safe(idx):
                    if idx is None or idx < 0:
                        return cells[idx] if idx == -1 and cells else ""
                    return cells[idx] if idx < len(cells) else ""

                item = safe(item_col)
                options = safe(opt_col) if opt_col is not None else ""
                qty = safe(qty_col)

                # Skip separator rows (all dashes)
                if re.match(r'^[-\s]+$', item) and re.match(r'^[-\s]+$', options or ""):
                    continue
                # Skip rows that look like sub-items (SKU lines like "15373/65000 (X1)")
                if re.match(r'^\d{5}/\d{5}', item):
                    continue

                if item or qty:
                    products.append({"item": item, "options": options, "qty": qty})

        elif header_found:
            # Plain text line after header — try simple split
            # Skip SKU lines
            if re.match(r'^\d{5}/\d{5}', stripped):
                continue
            parts = stripped.rsplit(None, 1)
            if len(parts) == 2 and re.match(r'^\d+$', parts[1]):
                products.append({"item": parts[0], "options": "", "qty": parts[1]})

    return products


def extract_product_table_pdfplumber(pdf_bytes):
    """Use pdfplumber table extraction for digital PDFs."""
    products = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 2:
                    continue
                header_idx = None
                for i, row in enumerate(table):
                    row_text = " ".join(str(c or "").lower() for c in row)
                    if any(kw in row_text for kw in ["item", "product", "qty", "options"]):
                        header_idx = i
                        break
                if header_idx is None and len(table[0]) >= 3:
                    header_idx = 0
                if header_idx is None:
                    continue
                headers = [clean(str(h or "")).lower() for h in table[header_idx]]
                item_col = next((i for i, h in enumerate(headers) if "item" in h or "product" in h), 0)
                opt_col = next((i for i, h in enumerate(headers) if "option" in h or "colour" in h or "size" in h), 1 if len(headers) > 1 else None)
                qty_col = next((i for i, h in enumerate(headers) if "qty" in h or "quantity" in h), len(headers) - 1)
                for row in table[header_idx + 1:]:
                    if not row or all(c is None or clean(str(c)) == "" for c in row):
                        continue
                    def safe_get(idx):
                        if idx is None or idx >= len(row):
                            return ""
                        return clean(str(row[idx] or ""))
                    item = safe_get(item_col)
                    options = safe_get(opt_col) if opt_col is not None else ""
                    qty = safe_get(qty_col)
                    if item or qty:
                        products.append({"item": item, "options": options, "qty": qty})
                if products:
                    return products
    return products


# ── Empty response templates ──────────────────────────────────────────────────

def other_response():
    return {"document_type": "other", "document": None}


def empty_delivery_order():
    return {
        "document_type": "delivery_order",
        "document": {
            "header": {"title": "", "etd": "", "ref": "", "inv_no": "", "customer_po_no": ""},
            "customer": {
                "company_name": None,
                "name": "",
                "address": {"street": "", "city": "", "region": "", "postcode": "", "country": ""},
                "phone": "",
                "mobile": "",
            },
            "ship_to": {
                "name": "",
                "address": {"street": "", "city": "", "region": "", "postcode": "", "country": ""},
            },
            "handwritten": {},
            "product_selection": [],
        },
    }


def parse_header_fields(lines):
    """
    Parse ETD, Ref, Invoice Number, Customer PO No from delivery order header.
    Handles three layouts Mistral OCR may produce:
      A) Markdown table: | ETD | Ref | Invoice Number | ... |
                         | 15 Apr 2026 | ABED19631-11 | ... |
      B) Columnar text: labels on one line, values on next (character-position aligned)
      C) Inline:        ETD: 15 Apr 2026 (each on its own line)
    """
    etd = ref = inv_no = customer_po_no = ""
    header_keywords = ["etd", "ref", "invoice", "customer po"]

    for i, line in enumerate(lines):
        line_lower = line.lower()

        # ── Layout A: Markdown table header row ─────────────────────────────
        if "|" in line and any(kw in line_lower for kw in header_keywords):
            headers = [c.strip().lower() for c in line.split("|") if c.strip()]
            # Find values on next line
            if i + 1 < len(lines) and "|" in lines[i + 1]:
                next_line = lines[i + 1]
                # Skip separator lines (|---|---|)
                if re.match(r"^[\|\-\s]+$", next_line):
                    if i + 2 < len(lines):
                        next_line = lines[i + 2]
                values = [c.strip() for c in next_line.split("|") if c.strip()]
                for hi, header in enumerate(headers):
                    val = values[hi] if hi < len(values) else ""
                    if "etd" in header and not etd:
                        etd = val
                    elif header == "ref" and not ref:
                        ref = val
                    elif "invoice" in header and not inv_no:
                        inv_no = val
                    elif "customer po" in header and not customer_po_no:
                        customer_po_no = val
                if etd or ref:
                    return etd, ref, inv_no, customer_po_no

        # ── Layout B: Columnar text (2+ labels on same line) ────────────────
        matches = sum(1 for kw in header_keywords if kw in line_lower)
        if matches >= 2 and "|" not in line:
            # Values are on the next non-header line
            for j in range(i + 1, min(i + 3, len(lines))):
                candidate = lines[j]
                if not any(kw in candidate.lower() for kw in header_keywords):
                    values_line = candidate
                    # Use character positions from header line
                    label_pos = []
                    for kw in ["etd", "ref", "invoice number", "customer po no", "customer po"]:
                        idx = line_lower.find(kw)
                        if idx >= 0 and not any(abs(idx - p[0]) < 3 for p in label_pos):
                            label_pos.append((idx, kw))
                    label_pos.sort()
                    for pi, (pos, lbl) in enumerate(label_pos):
                        end_pos = label_pos[pi+1][0] if pi+1 < len(label_pos) else len(values_line)+50
                        s = min(pos, len(values_line))
                        e = min(end_pos, len(values_line))
                        val = values_line[s:e].strip() if s < len(values_line) else ""
                        if "etd" in lbl and not etd:
                            etd = val
                        elif lbl == "ref" and not ref:
                            ref = val
                        elif "invoice" in lbl and not inv_no:
                            inv_no = val
                        elif "customer po" in lbl and not customer_po_no:
                            customer_po_no = val
                    break
            if etd or ref:
                return etd, ref, inv_no, customer_po_no

        # ── Layout C: Inline format ──────────────────────────────────────────
        if not etd and "etd" in line_lower and ":" in line:
            etd = find_value_after_label([line], "ETD")
        if not ref and re.search(r"\bref\b", line_lower) and ":" in line:
            ref = find_value_after_label([line], "Ref")
        if not inv_no and "invoice" in line_lower and ":" in line:
            inv_no = find_value_after_label([line], "Invoice Number", "Invoice No")
        if not customer_po_no and "customer po" in line_lower and ":" in line:
            customer_po_no = find_value_after_label([line], "Customer PO No", "Customer PO")

    return etd, ref, inv_no, customer_po_no

# ── Main extraction ───────────────────────────────────────────────────────────

def extract_delivery_order(pdf_bytes):
    """
    Extract structured data from a delivery order PDF.
    Handles both digital and scanned PDFs transparently.
    """
    scanned = is_scanned_pdf(pdf_bytes)
    full_text = get_pdf_text(pdf_bytes)

    # ── Gatekeeper ────────────────────────────────────────────────────────────
    if "Delivery Order" not in full_text and "DELIVERY ORDER" not in full_text and "delivery order" not in full_text.lower():
        return other_response()

    # ── Parse lines ───────────────────────────────────────────────────────────
    lines = [clean(l) for l in full_text.split("\n") if clean(l)]

    # ── Header ────────────────────────────────────────────────────────────────
    # Brand name: first meaningful non-header line (skip lines that are just "Delivery Order")
    title = ""
    for line in lines:
        if "delivery order" in line.lower():
            continue
        if len(line) > 2:
            title = line
            break

    # Header fields: detect the columnar header line then read values from next line
    etd, ref, inv_no, customer_po_no = parse_header_fields(lines)

    # ── Customer block ────────────────────────────────────────────────────────
    customer_block = extract_block(
        lines, "Customer:",
        ["Ship To", "Deliver To", "Product Selection", "Handwritten"]
    )
    if not customer_block:
        customer_block = extract_block(
            lines, "Customer",
            ["Ship To", "Deliver To", "Product Selection"]
        )

    company_name = clean(customer_block[0]) if customer_block else None
    customer_name = clean(customer_block[1]) if len(customer_block) > 1 else ""

    # Separate address lines from phone/mobile
    address_lines = []
    phone = ""
    mobile = ""
    for line in (customer_block[2:] if len(customer_block) > 2 else []):
        if re.search(r'phone|tel(?!\w)|mobile|mob(?!\w)', line, re.IGNORECASE):
            p, m = extract_contact([line])
            if p:
                phone = p
            if m:
                mobile = m
        else:
            address_lines.append(line)

    # Also scan all lines for phone/mobile if not found in block
    if not phone and not mobile:
        phone, mobile = extract_contact(lines)

    customer_address = parse_address_block(address_lines) if address_lines else {
        "street": "", "city": "", "region": "", "postcode": "", "country": ""
    }

    # ── Ship To block ─────────────────────────────────────────────────────────
    ship_block = extract_block(
        lines, "Ship To:",
        ["Product Selection", "Handwritten", "Balance", "Authorisation"]
    )
    if not ship_block:
        ship_block = extract_block(
            lines, "Ship To",
            ["Product Selection", "Handwritten", "Balance"]
        )

    ship_name = clean(ship_block[0]) if ship_block else ""
    ship_address_lines = ship_block[1:] if len(ship_block) > 1 else []
    ship_address = parse_address_block(ship_address_lines) if ship_address_lines else {
        "street": "", "city": "", "region": "", "postcode": "", "country": ""
    }

    # ── Product Selection ─────────────────────────────────────────────────────
    if scanned:
        # Use text-based table parser for Mistral OCR markdown output
        products = extract_product_table_from_text(lines)
    else:
        # Use pdfplumber's structural table extraction for digital PDFs
        products = extract_product_table_pdfplumber(pdf_bytes)
        if not products:
            products = extract_product_table_from_text(lines)

    # ── Assemble response ─────────────────────────────────────────────────────
    result = empty_delivery_order()
    result["document"]["header"] = {
        "title": title,
        "etd": etd,
        "ref": ref,
        "inv_no": inv_no,
        "customer_po_no": customer_po_no,
    }
    result["document"]["customer"] = {
        "company_name": company_name,
        "name": customer_name,
        "address": customer_address,
        "phone": phone,
        "mobile": mobile,
    }
    result["document"]["ship_to"] = {
        "name": ship_name,
        "address": ship_address,
    }
    result["document"]["product_selection"] = products
    result["document"]["handwritten"] = {}

    return result


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
            "note": "POST a PDF as multipart/form-data (field: 'file'). Set OCR_SPACE_API_KEY env var on Vercel.",
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
# This line intentionally left blank — patches applied below via sed
