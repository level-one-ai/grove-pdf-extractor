"""
Grove PDF Extractor
===================
Serverless Python function deployed on Vercel.

Accepts a PDF file via HTTP POST from Make.com, extracts structured data
from Grove Bedding delivery orders, and returns JSON.

Flow:
  1. Receive POST with multipart/form-data containing a PDF file
  2. Gatekeeper: if "Delivery Order" not in text → return {document_type: "other"}
  3. Extraction: parse header, customer, ship_to, and product_selection table
  4. Return structured JSON matching the Grove PDF Router schema
"""

import json
import io
import re
import cgi
import pdfplumber
from http.server import BaseHTTPRequestHandler


# ── Helpers ──────────────────────────────────────────────────────────────────

def clean(value):
    """Strip whitespace and return empty string if None."""
    if value is None:
        return ""
    return str(value).strip()


def find_value_after_label(lines, *labels):
    """
    Search through lines for a label and return the text that follows it.
    Handles both same-line values ("ETD: 01/01/2025") and next-line values.
    """
    for i, line in enumerate(lines):
        line_upper = line.upper()
        for label in labels:
            label_upper = label.upper()
            if label_upper in line_upper:
                # Try same line first — "ETD: 01/01/2025"
                after_label = re.split(re.escape(label), line, flags=re.IGNORECASE, maxsplit=1)
                if len(after_label) > 1:
                    value = clean(after_label[1].lstrip(":").strip())
                    if value:
                        return value
                # Try next line
                if i + 1 < len(lines):
                    value = clean(lines[i + 1])
                    if value:
                        return value
    return ""


def extract_block(lines, start_label, end_labels):
    """
    Extract a block of lines starting after start_label and ending before
    any of the end_labels. Used for Customer and Ship To address blocks.
    """
    block = []
    in_block = False
    for line in lines:
        if start_label.upper() in line.upper():
            in_block = True
            continue
        if in_block:
            if any(lbl.upper() in line.upper() for lbl in end_labels):
                break
            stripped = clean(line)
            if stripped:
                block.append(stripped)
    return block


def parse_address_block(block):
    """
    Parse a list of address lines into street, city, region, postcode, country.
    UK postcode pattern used to locate the postcode line.
    Heuristic: last line = country, postcode line found by regex,
    line before postcode = city/region split.
    """
    UK_POSTCODE = re.compile(
        r'\b[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}\b', re.IGNORECASE
    )

    street_lines = []
    city = ""
    region = ""
    postcode = ""
    country = ""

    postcode_idx = None
    for i, line in enumerate(block):
        if UK_POSTCODE.search(line):
            postcode = clean(UK_POSTCODE.search(line).group())
            postcode_idx = i
            break

    if postcode_idx is not None:
        # Everything before postcode line is street (possibly city/region mixed in)
        pre = block[:postcode_idx]
        post = block[postcode_idx + 1:]

        if pre:
            # Last pre-line is city (or "City, Region")
            city_line = pre[-1]
            if "," in city_line:
                parts = city_line.split(",", 1)
                city = clean(parts[0])
                region = clean(parts[1])
            else:
                city = clean(city_line)
            street_lines = pre[:-1]

        country = clean(post[0]) if post else ""
    else:
        # No postcode found — best effort
        street_lines = block[:-1] if len(block) > 1 else block
        country = clean(block[-1]) if len(block) > 1 else ""

    street = ", ".join(street_lines)
    return {
        "street": street,
        "city": city,
        "region": region,
        "postcode": postcode,
        "country": country,
    }


def extract_contact(block):
    """Extract phone and mobile from a block of lines."""
    phone = ""
    mobile = ""
    for line in block:
        if re.search(r'phone|tel', line, re.IGNORECASE):
            match = re.search(r'[\d\s\-\+\(\)]{7,}', line)
            if match:
                phone = clean(match.group())
        elif re.search(r'mobile|mob', line, re.IGNORECASE):
            match = re.search(r'[\d\s\-\+\(\)]{7,}', line)
            if match:
                mobile = clean(match.group())
    return phone, mobile


def extract_product_table(pdf):
    """
    Use pdfplumber table extraction to find the Product Selection table.
    Returns a list of {item, options, qty} dicts.
    Searches all pages and uses the first table with 3+ columns.
    """
    products = []

    for page in pdf.pages:
        tables = page.extract_tables()
        for table in tables:
            if not table or len(table) < 2:
                continue

            # Find header row — look for a row containing item/product/description
            header_idx = None
            for i, row in enumerate(table):
                row_text = " ".join(str(c or "").lower() for c in row)
                if any(kw in row_text for kw in ["item", "product", "description", "qty", "quantity"]):
                    header_idx = i
                    break

            if header_idx is None:
                # Use first row as header if table has 3+ columns
                if len(table[0]) >= 3:
                    header_idx = 0
                else:
                    continue

            headers = [clean(str(h or "")).lower() for h in table[header_idx]]

            # Map column indices
            item_col = next((i for i, h in enumerate(headers) if "item" in h or "product" in h or "desc" in h), 0)
            opt_col = next((i for i, h in enumerate(headers) if "option" in h or "colour" in h or "color" in h or "size" in h or "variant" in h), 1 if len(headers) > 1 else None)
            qty_col = next((i for i, h in enumerate(headers) if "qty" in h or "quantity" in h or "amount" in h), len(headers) - 1)

            for row in table[header_idx + 1:]:
                if not row or all(c is None or clean(str(c)) == "" for c in row):
                    continue  # skip empty rows

                def safe_get(idx):
                    if idx is None or idx >= len(row):
                        return ""
                    return clean(str(row[idx] or ""))

                item = safe_get(item_col)
                options = safe_get(opt_col) if opt_col is not None else ""
                qty = safe_get(qty_col)

                if item or qty:
                    products.append({
                        "item": item,
                        "options": options,
                        "qty": qty,
                    })

            if products:
                return products  # Return after first successful table

    return products


# ── Empty response templates ──────────────────────────────────────────────────

def other_response():
    return {"document_type": "other", "document": None}


def empty_delivery_order():
    return {
        "document_type": "delivery_order",
        "document": {
            "header": {
                "title": "",
                "etd": "",
                "ref": "",
                "inv_no": "",
                "customer_po_no": "",
            },
            "customer": {
                "company_name": None,
                "name": "",
                "address": {
                    "street": "",
                    "city": "",
                    "region": "",
                    "postcode": "",
                    "country": "",
                },
                "phone": "",
                "mobile": "",
            },
            "ship_to": {
                "name": "",
                "address": {
                    "street": "",
                    "city": "",
                    "region": "",
                    "postcode": "",
                    "country": "",
                },
            },
            "handwritten": {},
            "product_selection": [],
        },
    }


# ── Main extraction ───────────────────────────────────────────────────────────

def extract_delivery_order(pdf_bytes):
    """
    Extract structured data from a delivery order PDF.
    Returns a dict matching the Grove PDF Router JSON schema.
    """
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:

        # Collect all text from all pages
        full_text = ""
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            full_text += page_text + "\n"

        # ── Gatekeeper ──────────────────────────────────────────────────────
        if "Delivery Order" not in full_text and "DELIVERY ORDER" not in full_text:
            return other_response()

        # ── Parse lines ─────────────────────────────────────────────────────
        lines = [l for l in full_text.split("\n") if clean(l)]

        # ── Header ──────────────────────────────────────────────────────────
        # Brand name: typically first non-empty line at top of document
        title = clean(lines[0]) if lines else ""

        etd = find_value_after_label(lines, "ETD", "Estimated Time of Departure", "Estimated Delivery")
        ref = find_value_after_label(lines, "Ref:", "Reference:", "Ref No", "Our Ref")
        inv_no = find_value_after_label(lines, "Invoice No", "Invoice Number", "Inv No", "Invoice #")
        customer_po_no = find_value_after_label(lines, "Customer PO", "PO No", "Purchase Order", "PO Number", "Customer PO No")

        # ── Customer block ───────────────────────────────────────────────────
        customer_block = extract_block(
            lines,
            start_label="Customer",
            end_labels=["Ship To", "Deliver To", "Delivery Address", "Product Selection", "Items"],
        )

        company_name = clean(customer_block[0]) if customer_block else None
        customer_name = clean(customer_block[1]) if len(customer_block) > 1 else ""
        address_lines = customer_block[2:] if len(customer_block) > 2 else customer_block[1:]

        # Separate phone/mobile from address lines
        address_only = []
        phone = ""
        mobile = ""
        for line in address_lines:
            if re.search(r'phone|tel|mobile|mob', line, re.IGNORECASE):
                p, m = extract_contact([line])
                if p:
                    phone = p
                if m:
                    mobile = m
            else:
                address_only.append(line)

        customer_address = parse_address_block(address_only) if address_only else {
            "street": "", "city": "", "region": "", "postcode": "", "country": ""
        }

        # ── Ship To block ────────────────────────────────────────────────────
        ship_block = extract_block(
            lines,
            start_label="Ship To",
            end_labels=["Product Selection", "Items", "Order Details", "Handwritten", "Notes"],
        )
        # Try alternate labels if empty
        if not ship_block:
            ship_block = extract_block(
                lines,
                start_label="Deliver To",
                end_labels=["Product Selection", "Items", "Order Details"],
            )

        ship_name = clean(ship_block[0]) if ship_block else ""
        ship_address_lines = ship_block[1:] if len(ship_block) > 1 else []
        ship_address = parse_address_block(ship_address_lines) if ship_address_lines else {
            "street": "", "city": "", "region": "", "postcode": "", "country": ""
        }

        # ── Product Selection table ──────────────────────────────────────────
        products = extract_product_table(pdf)

        # ── Assemble response ────────────────────────────────────────────────
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
        # handwritten always empty — OCR bypassed
        result["document"]["handwritten"] = {}

        return result


# ── HTTP Handler ──────────────────────────────────────────────────────────────

class handler(BaseHTTPRequestHandler):

    def do_POST(self):
        try:
            content_type = self.headers.get("Content-Type", "")

            # ── Parse multipart/form-data (file upload from Make.com) ────────
            if "multipart/form-data" in content_type:
                form = cgi.FieldStorage(
                    fp=self.rfile,
                    headers=self.headers,
                    environ={
                        "REQUEST_METHOD": "POST",
                        "CONTENT_TYPE": content_type,
                    },
                )

                # Accept field named "file" or "pdf" or any uploaded file
                pdf_bytes = None
                for field_name in ["file", "pdf", "document"]:
                    if field_name in form:
                        pdf_bytes = form[field_name].file.read()
                        break

                # Fallback: grab the first file field found
                if pdf_bytes is None:
                    for key in form.keys():
                        item = form[key]
                        if hasattr(item, "file") and item.file:
                            pdf_bytes = item.file.read()
                            break

                if not pdf_bytes:
                    self._send_json(400, {"error": "No PDF file found in request. Send as multipart/form-data with field name 'file'."})
                    return

            # ── Accept raw binary body (application/pdf) ─────────────────────
            elif "application/pdf" in content_type or "application/octet-stream" in content_type:
                content_length = int(self.headers.get("Content-Length", 0))
                pdf_bytes = self.rfile.read(content_length)

            else:
                self._send_json(400, {
                    "error": "Unsupported Content-Type. Send PDF as multipart/form-data (field: 'file') or as application/pdf body."
                })
                return

            # ── Extract ──────────────────────────────────────────────────────
            result = extract_delivery_order(pdf_bytes)
            self._send_json(200, result)

        except Exception as e:
            self._send_json(500, {"error": str(e)})

    def do_GET(self):
        """Health check endpoint."""
        self._send_json(200, {
            "status": "ok",
            "service": "Grove PDF Extractor",
            "usage": "POST a PDF file as multipart/form-data (field: 'file') or as application/pdf body.",
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
        pass  # Suppress default access logging on Vercel
