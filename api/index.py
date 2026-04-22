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
                    rm = re.search(r'([A-Z0-9][A-Z0-9\-]{2,})', after_date)
                    if rm:
                        ref = rm.group(1)
                # Try date as dd/mm/yyyy
                if not etd:
                    dm2 = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', vline)
                    if dm2:
                        etd = dm2.group(1).strip()
                        after = vline[dm2.end():].strip()
                        rm2 = re.search(r'([A-Z0-9][A-Z0-9\-]{2,})', after)
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

    # Brand name: text before "ETD" or "Delivery Order" label
    title = ""
    title_m = re.match(r'^(.+?)(?=\bETD\b|\bDelivery\s+Order\b)', text.strip(), re.IGNORECASE)
    if title_m:
        title = clean(title_m.group(1))
    else:
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
            r'Ref[\s:]+([A-Z0-9][A-Z0-9\-]{2,})',  # any alphanumeric ref code
        )

    # Invoice Number — must contain at least one digit to avoid matching label words
    inv_no = find_by_regex(
        text,
        r'Invoice\s+(?:Number|No)[\s:]+((?=[A-Z0-9\-/]*\d)[A-Z0-9\-/]+)',
        r'Inv(?:oice)?[\s.#:]+((?=[A-Z0-9\-/]*\d)[A-Z0-9\-/]+)',
    )

    # Customer PO Number — 3+ chars, not a common document word
    customer_po_no = find_by_regex(
        text,
        r'Customer\s+PO\s+No[\s:]+(?!Delivery|Reference|Invoice|Customer|Ship|Number)([A-Z0-9][A-Z0-9\-/]{2,})',
        r'Customer\s+PO[\s:]+(?!Delivery|Reference|Invoice|Customer|Ship|Number)([A-Z0-9][A-Z0-9\-/]{2,})',
        r'PO\s+No[\s:]+(?!Delivery|Reference|Invoice|Customer|Ship|Number)([A-Z0-9][A-Z0-9\-/]{2,})',
        r'Purchase\s+Order[\s:]+(?!Delivery|Reference|Invoice|Customer|Ship|Number)([A-Z0-9][A-Z0-9\-/]{2,})',
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

    # Skip person-name-only lines that appear before the street address
    # (e.g. 'Ted Baigan' appearing in a ship_to block after the company name)
    PERSON_LINE_RE = re.compile(r'^[A-Z][a-z]+\s+[A-Z][a-z]+$')
    SKIP_NAMES = {'United Kingdom', 'United States', 'Scotland', 'England', 'Wales', 'Ireland'}
    seen_street = False
    filtered_addr_lines = []
    for line in addr_lines:
        if STREET_TYPES_RE.search(line) or re.search(r'\d', line):
            seen_street = True
        if (not seen_street and PERSON_LINE_RE.match(line)
                and line not in SKIP_NAMES
                and not UK_POSTCODE_RE.search(line)):
            continue  # skip person name before street
        filtered_addr_lines.append(line)
    addr_lines = filtered_addr_lines

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


# ── Customer and Ship To extraction ─────────────────────────────────────────

STREET_TYPES_RE = re.compile(
    r'\b(?:Road|Street|Avenue|Lane|Way|Close|Drive|Court|Place|Terrace|'
    r'Gardens|Grove|Hill|Rise|Walk|Mews|Crescent|Square|Parade|Boulevard|'
    r'Row|Circus|Wharf|Quay|Yard|Gate|View|Park|Estate|Bank|Wynd|Loan|'
    r'Brae|Causeway|Croft|Dell|Dene|Drove|Garth|Glade|Glen|Meadow|Mount|'
    r'Orchard|Path|Ridge|Strand|Vale|Villa|Villas|Warren|Wood|Roundabout|'
    r'Terrace|Approach|Arcade|Passage|Precinct|Promenade|Quayside|Ring)\b',
    re.IGNORECASE
)
UK_POSTCODE_RE = re.compile(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b', re.IGNORECASE)


def _parse_flat_address(text):
    """
    Parse a flat single-line address string into structured fields.
    Handles all Grove customer address formats:
    - Individual: "Graeme Markham 50 Broomhouse Bank Edinburgh EH11 3TL"
    - Company + person: "Brigend Trading Ted Baigan 17 Old Dalkeith Road Edinburgh EH16 4TE"
    - Company = person: "Cullen Property Cullen Property 30 Rutland Square Edinburgh EH1 2BW"
    - Loren Williams format: "Abingdon Beds Ltd Ashley Alsworth The Retail Warehouse Marcham Road Abingdon, Oxfordshire OX14 1TZ"
    """
    COMPANY_KW = re.compile(
        r'\b(Ltd|Limited|PLC|LLP|Inc|Group|Property|Trading|Services|Hotel|' 
        r'House|Lodge|Letting|Rentals|Investments|Management|Consultancy|' 
        r'Associates|Partners|Trust|Foundation|Concierge|Bedding)\b',
        re.IGNORECASE
    )

    pc_m = UK_POSTCODE_RE.search(text)
    if not pc_m:
        return None, "", {"street": "", "city": "", "region": "", "postcode": "", "country": "United Kingdom"}

    postcode = pc_m.group(1).strip()
    before_pc = text[:pc_m.start()].strip().rstrip(",")
    after_pc = text[pc_m.end():].strip()

    # Region: text after last comma before postcode
    if "," in before_pc:
        comma_idx = before_pc.rfind(",")
        region = before_pc[comma_idx + 1:].strip()
        before_region = before_pc[:comma_idx].strip()
    else:
        region = ""
        before_region = before_pc

    # City: last word after last street type (handles "Morningside Edinburgh" -> "Edinburgh")
    st_ms = list(STREET_TYPES_RE.finditer(before_region))
    if st_ms:
        after_st = before_region[st_ms[-1].end():].strip()
        before_st = before_region[:st_ms[-1].end()].strip()
        if "," in after_st:
            city = after_st.rsplit(",", 1)[-1].strip()
        else:
            city_words = after_st.split()
            city = city_words[-1] if city_words else ""
    else:
        words = before_region.rsplit(None, 1)
        city = words[-1] if len(words) > 1 else ""
        before_st = words[0] if len(words) > 1 else before_region

    # Company detection
    company = None
    person = ""
    street = ""

    company_m = COMPANY_KW.search(before_st)
    if company_m:
        company = before_st[:company_m.end()].strip()
        after_company = before_st[company_m.end():].strip()

        # Check if company name repeats (e.g. "Cullen Property Cullen Property 30...")
        cwords = company.split()
        awords = after_company.split()
        if len(awords) >= len(cwords) and awords[:len(cwords)] == cwords:
            # Repeated - person = company, rest is street
            person = company
            street = " ".join(awords[len(cwords):])
        else:
            # Different name follows - extract person (2 title-case words)
            name_m = re.match(r'([A-Z][a-z]+\s+[A-Z][a-z]+)(?=\s|$)', after_company)
            if name_m:
                person = name_m.group(1)
                street = after_company[name_m.end():].strip()
            else:
                person = ""
                street = after_company
    else:
        # No company keyword - look for person name (2 title-case words)
        name_m = re.match(r'([A-Z][a-z]+\s+[A-Z][a-z]+)(?=\s|$)', before_st)
        if name_m:
            person = name_m.group(1)
            street = before_st[name_m.end():].strip()
        else:
            person = ""
            street = before_st

    addr = {
        "street": street,
        "city": city,
        "region": region,
        "postcode": postcode,
        "country": after_pc.strip() or "United Kingdom",
    }
    return company, person, addr


def extract_customer(text):
    """
    Extract customer block. Works with both:
    - Multi-line OCR: uses section extraction + line parsing
    - Flat single-line OCR: uses regex boundaries + flat address parser
    """
    # Locate customer block
    cust_m = re.search(r'Customer\s*:', text, re.IGNORECASE)
    if not cust_m:
        empty_addr = {"street": "", "city": "", "region": "", "postcode": "", "country": "United Kingdom"}
        return None, "", empty_addr, "", ""

    # Check if 'Customer:' and 'Ship To:' are on the same line (two-column layout)
    rest_of_line_m = re.search(r'[^\n]*', text[cust_m.end():], re.IGNORECASE)
    rest_of_line = rest_of_line_m.group() if rest_of_line_m else ''
    same_line_ship = bool(re.search(r'Ship\s+To:', rest_of_line, re.IGNORECASE))

    # Find end of customer block (stop before Phone:, Product Selection, or Ship To:)
    # If Ship To: is on the same line as Customer:, skip to the line AFTER
    search_start = cust_m.end()
    if same_line_ship:
        # Move past the current line
        newline_m = re.search(r'\n', text[search_start:])
        if newline_m:
            search_start += newline_m.end()
    cust_end_m = re.search(r'Phone:|Mobile:|Product\s+Selection|Ship\s+To:', text[search_start:], re.IGNORECASE)
    cust_block = text[search_start: search_start + cust_end_m.start()].strip() if cust_end_m else text[search_start:].strip()

    # Phone / mobile from full text
    phone = ""
    mobile = ""
    ph_m = re.search(r'Phone\s*:\s*([\+\d][\d\s\-\+\(\)]{5,})', text, re.IGNORECASE)
    mob_m = re.search(r'Mobile\s*:\s*([\+\d][\d\s\-\+\(\)]{5,})', text, re.IGNORECASE)
    if ph_m:
        phone = re.sub(r'[^\d]+$', '', ph_m.group(1)).strip()
    if mob_m:
        mobile = re.sub(r'[^\d]+$', '', mob_m.group(1)).strip()

    # Check if block has newlines (multi-line OCR) or is flat
    lines = [l.strip() for l in cust_block.split('\n') if l.strip()]

    if len(lines) >= 3:
        # Multi-line: parse line by line
        # Detect if lines[0] is a person name (individual customer, no company)
        PERSON_RE = re.compile(r'^[A-Z][a-z]+\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)?$')
        COMPANY_KW2 = re.compile(
            r'\b(Ltd|Limited|PLC|LLP|Inc|Group|Property|Trading|Services|Hotel|'
            r'House|Lodge|Letting|Rentals|Investments|Management|Concierge|'
            r'Bedding|Edinburgh|Home|In)\b', re.IGNORECASE
        )
        first_line = lines[0]
        if PERSON_RE.match(first_line) and not COMPANY_KW2.search(first_line):
            # Individual customer: first line is the person name, no company
            company_name = None
            cust_name = first_line
            addr_lines = [l for l in lines[1:] if not re.search(r'Phone|Mobile', l, re.IGNORECASE)]
        else:
            # Company customer: first line is company, second is person name
            company_name = first_line
            cust_name = lines[1] if len(lines) > 1 else ""
            addr_lines = [l for l in lines[2:] if not re.search(r'Phone|Mobile', l, re.IGNORECASE)]
        addr, p, m = parse_address_lines(addr_lines)
        if p: phone = p
        if m: mobile = m
        return company_name, cust_name, addr, phone, mobile
    else:
        # Flat single-line: use address parser
        company_name, cust_name, addr = _parse_flat_address(cust_block)
        return company_name or None, cust_name, addr, phone, mobile


def extract_ship_to(text):
    """
    Extract Ship To block. Works with both multi-line and flat single-line OCR.
    """
    ship_m = re.search(r'Ship\s+To\s*:', text, re.IGNORECASE)
    if not ship_m:
        empty_addr = {"street": "", "city": "", "region": "", "postcode": "", "country": "United Kingdom"}
        return "", empty_addr

    # Find end of ship_to block
    ship_end_m = re.search(
        r'Options\b|Product\s+Selection|Balance\s+owing|Handwritten|Authoris',
        text[ship_m.end():], re.IGNORECASE
    )
    ship_block = text[ship_m.end(): ship_m.end() + ship_end_m.start()].strip() if ship_end_m else text[ship_m.end():].strip()

    lines = [l.strip() for l in ship_block.split('\n') if l.strip()]

    if len(lines) >= 3:
        name = lines[0]
        addr, _, _ = parse_address_lines(lines[1:])
        return name, addr
    else:
        company, person, addr = _parse_flat_address(ship_block)
        # For ship_to, the "name" field should be the company name (delivery destination)
        # Fall back to person name if no company found
        ship_name = company or person or (ship_block.split()[0] if ship_block else "")
        return ship_name, addr


def parse_address_lines(lines):
    """Parse a list of address lines into structured fields."""
    phone = ""
    mobile = ""
    addr_lines = []

    for line in lines:
        l = line.strip()
        if not l:
            continue
        if re.search(r'\bphone\b|\btel\b', l, re.IGNORECASE):
            m = re.search(r'[\+\d][\d\s\-\+\(\)]{6,}', l)
            if m:
                phone = re.sub(r'[^\d]+$', '', m.group()).strip()
            continue
        if re.search(r'\bmobile\b|\bmob\b', l, re.IGNORECASE):
            m = re.search(r'[\+\d][\d\s\-\+\(\)]{6,}', l)
            if m:
                mobile = re.sub(r'[^\d]+$', '', m.group()).strip()
            continue
        addr_lines.append(l)

    # Skip person-name-only lines that appear before the street address
    # (e.g. 'Ted Baigan' appearing in a ship_to block after the company name)
    PERSON_LINE_RE = re.compile(r'^[A-Z][a-z]+\s+[A-Z][a-z]+$')
    SKIP_NAMES = {'United Kingdom', 'United States', 'Scotland', 'England', 'Wales', 'Ireland'}
    seen_street = False
    filtered_addr_lines = []
    for line in addr_lines:
        if STREET_TYPES_RE.search(line) or re.search(r'\d', line):
            seen_street = True
        if (not seen_street and PERSON_LINE_RE.match(line)
                and line not in SKIP_NAMES
                and not UK_POSTCODE_RE.search(line)):
            continue  # skip person name before street
        filtered_addr_lines.append(line)
    addr_lines = filtered_addr_lines

    postcode = ""
    postcode_idx = None
    for i, line in enumerate(addr_lines):
        m = UK_POSTCODE_RE.search(line)
        if m:
            postcode = m.group(1).strip()
            postcode_idx = i
            break

    street = ""
    city = ""
    region = ""
    country = "United Kingdom"

    if postcode_idx is not None:
        pre = addr_lines[:postcode_idx]
        post = addr_lines[postcode_idx + 1:]
        pc_line = addr_lines[postcode_idx]
        before_pc = UK_POSTCODE_RE.split(pc_line)[0].strip().rstrip(",").strip()
        if before_pc:
            if "," in before_pc:
                parts = before_pc.split(",", 1)
                city = parts[0].strip()
                region = parts[1].strip()
            else:
                city = before_pc
            street = ", ".join(pre)
        elif pre:
            city_line = pre[-1]
            if "," in city_line:
                parts = city_line.split(",", 1)
                city = parts[0].strip()
                region = parts[1].strip()
            else:
                city = city_line
            street = ", ".join(pre[:-1])
        country = post[0] if post else "United Kingdom"

    return {"street": street, "city": city, "region": region, "postcode": postcode, "country": country}, phone, mobile


# ── Product extraction ────────────────────────────────────────────────────────

SKU_RE = re.compile(r'\b\d{5}/\d{5}', re.IGNORECASE)
PRODUCT_HEADER_RE = re.compile(r'\bItem\b|\bOptions\b|\bQty\b', re.IGNORECASE)
# OPT_KW_RE matches option LABELS (always followed by a colon)
# This prevents splitting on option words that appear inside product names
# e.g. 'Purecare Cotton Mattress Protector' must NOT split at 'Mattress'
# but 'Ocean Dream Mattress Size: 5\'0 King' MUST split at 'Mattress Size:'
OPT_KW_RE = re.compile(
    r'(?:Mattress\s+Size|Bed\s+Frame\s+Size|Headboard\s+(?:Size|Height)|'
    r'Pillow\s+Option|Colour|Color|Storage|Height|Width|Depth|Size)'
    r'(?:\s*:|\s+\w+\s*:)',
    re.IGNORECASE
)


def extract_products(text):
    """
    Extract product rows from a column-strip OCR layout.

    OCR.space reads multi-column tables as vertical strips:
      Strip 1: Item names + SKU codes  (between 'Item' and 'Ship To:' or 'Options')
      Strip 2: Options                 (between 'Options' and 'Qty')
      Strip 3: Qty integers + sometimes last option  (after 'Qty')

    Strategy:
      1. Locate each strip by its label keyword
      2. Parse item names by splitting on SKU codes as delimiters
      3. Parse options by splitting on option keyword re-occurrences
      4. Parse qtys as leading integers, last option may be appended at end
      5. Zip all three lists into product dicts
    """
    # Locate product section
    ps_m = re.search(r'Product\s+Selection', text, re.IGNORECASE)
    end_m = re.search(r'Balance\s+owing|Authorisation|Authorization|VAT\s+NO', text, re.IGNORECASE)
    if not ps_m:
        return []

    ps_text = text[ps_m.end(): end_m.start() if end_m else len(text)]

    item_m    = re.search(r'\bItem\b',    ps_text, re.IGNORECASE)
    options_m = re.search(r'\bOptions\b', ps_text, re.IGNORECASE)
    qty_m     = re.search(r'\bQty\b',     ps_text, re.IGNORECASE)
    ship_m    = re.search(r'Ship\s+To:',  ps_text, re.IGNORECASE)

    # If any marker is missing, fall back to line-based parsing
    if not (item_m and options_m and qty_m):
        return _parse_line_based(ps_text)

    # Items strip: from 'Item' to 'Ship To:' or 'Options' (whichever is earlier)
    items_end_candidates = [m.start() for m in [ship_m, options_m] if m]
    items_end = min(items_end_candidates) if items_end_candidates else options_m.start()
    items_text = ps_text[item_m.end():items_end].strip()

    # Options strip: from 'Options' to 'Qty'
    options_text = ps_text[options_m.end():qty_m.start()].strip()

    # Qty strip: from 'Qty' to end
    qty_text = ps_text[qty_m.end():].strip()

    # ── 1. Parse item names ───────────────────────────────────────────────
    # Split on SKU codes (d{5}/d{5} with optional trailing "(X1)" etc)
    # keeping only text that starts with a capital letter (real item names)
    parts = re.split(r'\s*\d{5}/\d{5}\s*(?:\([^)]*\))?\s*', items_text)
    item_names = [p.strip() for p in parts if p.strip() and re.match(r'[A-Z]', p.strip())]

    if not item_names:
        return _parse_line_based(ps_text)

    # ── 2. Parse options ──────────────────────────────────────────────────
    # Split on each re-occurrence of the option keyword
    # Split options strip on re-occurrence of option label keywords
    _OPT_SPLIT = re.compile(
        r'(?=(?:Mattress\s+Size|Bed\s+Frame\s+Size|Headboard\s+(?:Size|Height)|'
        r'Pillow\s+Option|Colour|Color|Storage|Height|Width|Depth|Size)'
        r'(?:\s*:|\s+\w+\s*:))',
        re.IGNORECASE
    )
    options_list = [o.strip() for o in _OPT_SPLIT.split(options_text) if o.strip()]

    # ── 3. Parse qty section ──────────────────────────────────────────────
    # Leading integers are qtys; any remaining text is the last option
    leading_m = re.match(r'^([\d\s\#\/]+)', qty_text)
    qty_list = re.findall(r'\d+', leading_m.group(1)) if leading_m else []
    remainder = qty_text[leading_m.end():].strip() if leading_m else qty_text

    if remainder and re.search(r'[A-Za-z]{3,}', remainder):
        # Trailing option in qty section — belongs to the last item
        last_qty_m = re.search(r'\s+(\d+)\s*$', remainder)
        if last_qty_m:
            options_list.append(remainder[:last_qty_m.start()].strip())
            qty_list.append(last_qty_m.group(1))
        else:
            # Qty was cut off by OCR — default to "1"
            options_list.append(remainder.strip())
            qty_list.append('1')

    # ── 4. Zip into products ──────────────────────────────────────────────
    # Strip newlines/tabs so Make.com's toString() produces JSON-safe strings
    def _clean(s): return ' '.join(str(s).split()).strip()
    products = []
    for i, name in enumerate(item_names):
        products.append({
            'item': _clean(name),
            'options': _clean(options_list[i] if i < len(options_list) else ''),
            'qty': _clean(qty_list[i] if i < len(qty_list) else ''),
        })

    return products


def _parse_line_based(text):
    """
    Fallback line-based parser for PDFs where OCR preserves row structure
    (digital PDFs or high-resolution scans read row-by-row).
    """
    lines = [l.strip() for l in text.replace('\r', '').split('\n') if l.strip()]
    clean_lines = [
        l for l in lines
        if not (PRODUCT_HEADER_RE.search(l) and len(l.split()) <= 6)
        and not SKU_RE.match(l)
        and not re.match(r'^[\-\s\.]+$', l)
    ]

    # Single-line: item + options + qty all on one line
    products = []
    for line in clean_lines:
        qty_m = re.search(r'\s+(\d+)\s*$', line)
        if not qty_m:
            continue
        qty = qty_m.group(1)
        remainder = line[:qty_m.start()].strip()
        opts_m = OPT_KW_RE.search(remainder)
        if opts_m:
            item = remainder[:opts_m.start()].strip()
            options = remainder[opts_m.start():].strip()
        else:
            parts = re.split(r'\s{2,}', remainder, maxsplit=1)
            item = parts[0].strip()
            options = parts[1].strip() if len(parts) > 1 else ''
        def _cl(s): return ' '.join(str(s).split()).strip()
        if item and not SKU_RE.match(item):
            products.append({'item': _cl(item), 'options': _cl(options), 'qty': _cl(qty)})

    return products


# ── Response templates ────────────────────────────────────────────────────────

def other_response():
    return {"document_type": "other", "document": None}


def _sanitise(obj):
    """Recursively strip control characters from all string values.
    This ensures the JSON response is safe for Make.com to embed in HTTP bodies."""
    if isinstance(obj, str):
        # Strip control chars 0x00-0x1F and 0x7F, EXCEPT keep printable space (0x20)
        return ' '.join(obj.split()).strip()
    if isinstance(obj, dict):
        return {k: _sanitise(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitise(i) for i in obj]
    return obj


def build_response(header, company_name, cust_name, cust_addr, phone, mobile,
                   ship_name, ship_addr, products):
    response = {
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
    # Sanitise every string field before returning — removes control characters
    # that would make Make.com's JSON body builder produce invalid JSON
    return _sanitise(response)


# ── Main extraction ───────────────────────────────────────────────────────────

def extract_delivery_order(pdf_bytes):
    full_text = get_pdf_text(pdf_bytes)

    # Gatekeeper — only process Delivery Orders
    text_lower = full_text.lower()
    if "delivery order" not in text_lower:
        return other_response()
    # Explicitly reject Branch Transfers even if they somehow contain the phrase
    if "branch transfer" in text_lower:
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

            # Debug mode: ?debug=1 returns raw OCR text so you can see what OCR.space produced
            from urllib.parse import urlparse, parse_qs
            query = parse_qs(urlparse(self.path).query)
            if query.get("debug", ["0"])[0] == "1":
                raw_text = get_pdf_text(pdf_bytes)
                self._send_json(200, {"debug": True, "raw_ocr_text": raw_text})
                return

            result = extract_delivery_order(pdf_bytes)
            self._send_json(200, result)

        except Exception as e:
            self._send_json(500, {"error": str(e)})

    def do_GET(self):
        self._send_json(200, {
            "status": "ok",
            "service": "Grove PDF Extractor",
            "note": "POST a PDF as multipart/form-data (field: 'file'). Set OCR_SPACE_API_KEY on Vercel. Add ?debug=1 to POST to see raw OCR text.",
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
