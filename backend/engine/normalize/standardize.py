"""
CUIN v2 - Normalization Module

Functions to normalize and standardize customer data fields
for consistent blocking and matching.
"""

import re
import unicodedata
from datetime import datetime
from typing import Optional
import hashlib
import logging

logger = logging.getLogger(__name__)

def normalize_name(name: Optional[str]) -> Optional[str]:
    """
    Normalize a name field:
    - Convert to uppercase
    - Remove accents/diacritics
    - Remove punctuation except spaces
    - Collapse multiple spaces
    - Trim whitespace
    """
    if not name or not isinstance(name, str):
        return None
    
    # Convert to uppercase
    name = name.upper().strip()
    
    # Remove accents/diacritics
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    
    # Remove punctuation except spaces and hyphens
    # Keep alphanumeric, spaces, hyphens
    # Also consider keeping apostrophes for names like O'Connor (optional, but standard usually removes them)
    # Current regex: [^\w\s-] -> removes everything except [a-zA-Z0-9_], whitespace, and -
    name = re.sub(r"[^\w\s-]", "", name)
    
    # Replace hyphens with space
    name = name.replace("-", " ")
    
    # Collapse multiple spaces
    name = re.sub(r'\s+', ' ', name).strip()
    
    if not name:
        # Log warning if we had a non-empty input that became empty
        # but avoid spamming logs for truly empty inputs
        return None
        
    return name


def normalize_phone(phone: Optional[str]) -> Optional[str]:
    """
    Normalize phone number:
    - Extract digits only
    - Keep last 10 digits (common mobile format)
    - Return None if less than 7 digits
    """
    if not phone or not isinstance(phone, str):
        return None
    
    # Extract digits only
    digits = re.sub(r'\D', '', phone)
    
    # Must have at least 7 digits for a valid phone
    if len(digits) < 7:
        return None
    
    # Keep last 10 digits (handles country codes)
    if len(digits) > 10:
        digits = digits[-10:]
    
    return digits


def normalize_email(email: Optional[str]) -> Optional[str]:
    """
    Normalize email address:
    - Convert to lowercase
    - Trim whitespace
    - Basic validation
    """
    if not email or not isinstance(email, str):
        return None
    
    email = email.lower().strip()
    
    # Basic email validation
    if '@' not in email or '.' not in email.split('@')[-1]:
        return None
    
    return email


def extract_email_domain(email: Optional[str]) -> Optional[str]:
    """Extract domain from normalized email."""
    if not email:
        return None
    
    normalized = normalize_email(email)
    if not normalized:
        return None
    
    return normalized.split('@')[-1]


def normalize_dob(dob: Optional[str]) -> Optional[str]:
    """
    Normalize date of birth to YYYY-MM-DD format.
    Handles various input formats.
    """
    if not dob or not isinstance(dob, str):
        return None
    
    dob = dob.strip()
    
    # Common date formats to try
    formats = [
        '%Y-%m-%d',      # 2000-01-15
        '%d-%m-%Y',      # 15-01-2000
        '%m-%d-%Y',      # 01-15-2000
        '%Y/%m/%d',      # 2000/01/15
        '%d/%m/%Y',      # 15/01/2000
        '%m/%d/%Y',      # 01/15/2000
        '%d.%m.%Y',      # 15.01.2000
        '%Y%m%d',        # 20000115
        '%d %b %Y',      # 15 Jan 2000
        '%d %B %Y',      # 15 January 2000
    ]
    
    for fmt in formats:
        try:
            parsed = datetime.strptime(dob, fmt)
            # Sanity check: year between 1900-2030
            if 1900 <= parsed.year <= 2030:
                return parsed.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    return None


def normalize_address(address: Optional[str]) -> Optional[str]:
    """
    Normalize address:
    - Convert to uppercase
    - Remove extra whitespace
    - Standardize common abbreviations
    """
    if not address or not isinstance(address, str):
        return None
    
    address = address.upper().strip()
    
    # Remove accents
    address = unicodedata.normalize('NFD', address)
    address = ''.join(c for c in address if unicodedata.category(c) != 'Mn')
    
    # Standardize common abbreviations
    replacements = {
        r'\bSTREET\b': 'ST',
        r'\bROAD\b': 'RD',
        r'\bAVENUE\b': 'AVE',
        r'\bBOULEVARD\b': 'BLVD',
        r'\bDRIVE\b': 'DR',
        r'\bLANE\b': 'LN',
        r'\bCOURT\b': 'CT',
        r'\bAPARTMENT\b': 'APT',
        r'\bSUITE\b': 'STE',
        r'\bBUILDING\b': 'BLDG',
        r'\bFLOOR\b': 'FL',
        r'\bNORTH\b': 'N',
        r'\bSOUTH\b': 'S',
        r'\bEAST\b': 'E',
        r'\bWEST\b': 'W',
    }
    
    for pattern, replacement in replacements.items():
        address = re.sub(pattern, replacement, address)
    
    # Remove punctuation
    address = re.sub(r'[^\w\s]', ' ', address)
    
    # Collapse whitespace
    address = re.sub(r'\s+', ' ', address).strip()
    
    return address if address else None


def normalize_natid(natid: Optional[str]) -> Optional[str]:
    """
    Normalize national ID:
    - Remove spaces, dashes, dots
    - Convert to uppercase
    - Keep alphanumeric only
    """
    if not natid or not isinstance(natid, str):
        return None
    
    # Remove common separators and convert to uppercase
    natid = re.sub(r'[\s\-\.]', '', natid.upper().strip())
    
    # Keep only alphanumeric
    natid = re.sub(r'[^A-Z0-9]', '', natid)
    
    return natid if natid else None


def compute_record_hash(record: dict) -> str:
    """
    Compute a stable hash of normalized record fields.
    Used for delta detection.
    """
    fields = [
        normalize_name(record.get('name', '')),
        normalize_phone(record.get('phone', '')),
        normalize_email(record.get('email', '')),
        normalize_dob(record.get('dob', '')),
        normalize_address(record.get('address', '')),
        normalize_natid(record.get('natid', '')),
    ]
    
    # Create canonical string (stable ordering assumed by list order)
    canonical = '|'.join(str(f or '') for f in fields)
    
    # Return SHA-256 hash
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def normalize_record(record: dict) -> dict:
    """
    Normalize all fields in a customer record.
    Returns a new dict with normalized values.
    """
    # Combine first and last name if separate
    full_name = None
    if record.get('first_name') or record.get('last_name'):
        parts = [
            record.get('first_name', ''),
            record.get('last_name', '')
        ]
        full_name = ' '.join(p for p in parts if p)
    else:
        full_name = record.get('name', '')
    
    # Get phone from multiple possible fields
    phone = record.get('phone') or record.get('mobile') or record.get('telephone')
    
    normalized = {
        'source_customer_id': record.get('customer_id') or record.get('id') or record.get('source_customer_id') or record.get('USCOD'),
        'name_norm': normalize_name(full_name) or normalize_name(f"{record.get('CUSNMF', '')} {record.get('CUSNML', '')}".strip()),
        'phone_norm': normalize_phone(phone) or normalize_phone(record.get('MOBLNO') or record.get('TELENO')),
        'email_norm': normalize_email(record.get('email')) or normalize_email(record.get('MAILID')),
        'dob_norm': normalize_dob(record.get('dob') or record.get('date_of_birth') or record.get('CUSDOB')),
        'address_norm': normalize_address(record.get('address')) or normalize_address(record.get('ADDRS1')),
        'city_norm': normalize_name(record.get('city')) or normalize_name(record.get('CITYNM')),
        'natid_norm': normalize_natid(record.get('natid') or record.get('national_id') or record.get('NATLID')),
        'source_system': record.get('source_system', 'UNKNOWN'),
        
        # Preserve rich metadata for UI
        'metadata': {
            'cust_type': record.get('cust_type') or record.get('CUSTYP'),
            'status': record.get('status') or record.get('CUSSTS'),
            'gender': record.get('gender') or record.get('GENDER'),
            'branch': record.get('branch') or record.get('OPRBRA'),
            'sponsor': record.get('sponsor') or record.get('SPONAM'),
            'timestamp': record.get('timestamp') or record.get('TIMSTAMP')
        }
    }
    
    # Compute record hash for delta detection
    normalized['record_hash'] = compute_record_hash({
        'name': normalized['name_norm'],
        'phone': normalized['phone_norm'],
        'email': normalized['email_norm'],
        'dob': normalized['dob_norm'],
        'address': normalized['address_norm'],
        'natid': normalized['natid_norm'],
    })
    
    return normalized
