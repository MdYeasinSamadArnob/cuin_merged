"""
Data Normalization Module for Entity Resolution

This module provides preprocessing utilities for cleaning and normalizing data
before entity resolution. It includes functions for text normalization, type
conversions, and data cleaning.
"""

import re
from datetime import datetime
from typing import Any, Optional, Union


def normalize_text(text: Optional[str]) -> str:
    """
    Normalize text by converting to uppercase, removing extra whitespace,
    and stripping special characters.
    
    Args:
        text: Input text string
        
    Returns:
        Normalized text string
    """
    if not text or not isinstance(text, str):
        return ""
    
    # Convert to uppercase
    text = text.upper()
    
    # Remove extra whitespace
    text = " ".join(text.split())
    
    # Remove common punctuation but keep spaces
    text = re.sub(r'[^\w\s]', ' ', text)
    
    # Remove extra spaces again
    text = " ".join(text.split())
    
    return text.strip()


def normalize_phone(phone: Optional[str]) -> str:
    """
    Normalize phone number by removing all non-numeric characters.
    
    Args:
        phone: Input phone number
        
    Returns:
        Normalized phone number with only digits
    """
    if not phone or not isinstance(phone, str):
        return ""
    
    # Keep only digits
    digits = re.sub(r'\D', '', phone)
    
    return digits


def normalize_email(email: Optional[str]) -> str:
    """
    Normalize email address to lowercase and remove whitespace.
    
    Args:
        email: Input email address
        
    Returns:
        Normalized email address
    """
    if not email or not isinstance(email, str):
        return ""
    
    return email.lower().strip()


def normalize_date(date: Optional[Union[str, datetime]]) -> Optional[str]:
    """
    Normalize date to ISO format (YYYY-MM-DD).
    
    Args:
        date: Input date as string or datetime object
        
    Returns:
        ISO formatted date string or None
    """
    if not date:
        return None
    
    if isinstance(date, datetime):
        return date.date().isoformat()
    
    if isinstance(date, str):
        # Try common date formats
        formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f"
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date.strip(), fmt)
                return dt.date().isoformat()
            except ValueError:
                continue
    
    return None


def normalize_name(name: Optional[str]) -> str:
    """
    Normalize person name by converting to uppercase, removing extra whitespace,
    and handling common abbreviations.
    
    Args:
        name: Input name
        
    Returns:
        Normalized name
    """
    if not name or not isinstance(name, str):
        return ""
    
    # Convert to uppercase
    name = name.upper()
    
    # Replace common abbreviations with full forms
    abbreviations = {
        r'\bMD\b': 'MOHAMMAD',
        r'\bM\.\b': 'MOHAMMAD',
        r'\bDR\b': 'DOCTOR',
        r'\bDR\.\b': 'DOCTOR',
        r'\bMR\b': 'MISTER',
        r'\bMR\.\b': 'MISTER',
        r'\bMRS\b': 'MISSUS',
        r'\bMRS\.\b': 'MISSUS',
        r'\bST\b': 'SAINT',
        r'\bST\.\b': 'SAINT',
    }
    
    for pattern, replacement in abbreviations.items():
        name = re.sub(pattern, replacement, name)
    
    # Remove extra whitespace
    name = " ".join(name.split())
    
    return name.strip()


def clean_value(value: Any) -> Any:
    """
    Clean a generic value by handling None, empty strings, and whitespace.
    
    Args:
        value: Input value of any type
        
    Returns:
        Cleaned value
    """
    if value is None:
        return None
    
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        # Remove double quotes that might be in CSV data
        value = value.replace('"', '')
        return value
    
    return value


def extract_tokens(text: Optional[str], min_length: int = 2) -> set:
    """
    Extract tokens (words) from text for token-based matching.
    
    Args:
        text: Input text
        min_length: Minimum token length to include
        
    Returns:
        Set of tokens
    """
    if not text or not isinstance(text, str):
        return set()
    
    # Normalize text first
    normalized = normalize_text(text)
    
    # Split into tokens
    tokens = normalized.split()
    
    # Filter by minimum length
    tokens = {t for t in tokens if len(t) >= min_length}
    
    return tokens


def concatenate_address(address_parts: list) -> str:
    """
    Concatenate multiple address parts into a single normalized string.
    
    Args:
        address_parts: List of address components
        
    Returns:
        Concatenated and normalized address
    """
    # Clean and filter non-empty parts
    parts = [clean_value(part) for part in address_parts]
    parts = [p for p in parts if p]
    
    if not parts:
        return ""
    
    # Join with comma and space
    address = ", ".join(parts)
    
    # Normalize the full address
    return normalize_text(address)


def preprocess_record(record: dict) -> dict:
    """
    Preprocess a complete record by normalizing all fields.
    
    Args:
        record: Dictionary of field names to values
        
    Returns:
        Dictionary with normalized values
    """
    processed = {}
    
    for key, value in record.items():
        # Clean the value first
        value = clean_value(value)
        
        if value is None:
            processed[key] = None
            continue
        
        # Apply field-specific normalization
        key_lower = key.lower()
        
        if 'name' in key_lower:
            processed[key] = normalize_name(value)
        elif 'phone' in key_lower or 'mobile' in key_lower or 'tel' in key_lower or 'fax' in key_lower:
            processed[key] = normalize_phone(value)
        elif 'email' in key_lower or 'mail' in key_lower:
            processed[key] = normalize_email(value)
        elif 'dob' in key_lower or 'date' in key_lower:
            processed[key] = normalize_date(value)
        elif 'address' in key_lower or 'addr' in key_lower or 'city' in key_lower:
            processed[key] = normalize_text(value)
        else:
            # Generic text normalization for other string fields
            if isinstance(value, str):
                processed[key] = normalize_text(value)
            else:
                processed[key] = value
    
    return processed
