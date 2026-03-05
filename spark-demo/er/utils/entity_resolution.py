"""
Entity Resolution Similarity Algorithms

This module provides various similarity algorithms for entity resolution including:
- Levenshtein distance
- Jaccard similarity
- Token-based matching
- Soundex phonetic encoding
"""

import re
from typing import Set, Optional


def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calculate the Levenshtein (edit) distance between two strings.
    
    The Levenshtein distance is the minimum number of single-character edits
    (insertions, deletions, or substitutions) required to change one string
    into another.
    
    Args:
        s1: First string
        s2: Second string
        
    Returns:
        Levenshtein distance (integer)
    """
    if not s1:
        return len(s2) if s2 else 0
    if not s2:
        return len(s1)
    
    # Create a matrix to store distances
    len1, len2 = len(s1), len(s2)
    
    # Initialize matrix with base cases
    matrix = [[0] * (len2 + 1) for _ in range(len1 + 1)]
    
    for i in range(len1 + 1):
        matrix[i][0] = i
    for j in range(len2 + 1):
        matrix[0][j] = j
    
    # Fill in the matrix
    for i in range(1, len1 + 1):
        for j in range(1, len2 + 1):
            if s1[i - 1] == s2[j - 1]:
                cost = 0
            else:
                cost = 1
            
            matrix[i][j] = min(
                matrix[i - 1][j] + 1,      # deletion
                matrix[i][j - 1] + 1,      # insertion
                matrix[i - 1][j - 1] + cost  # substitution
            )
    
    return matrix[len1][len2]


def levenshtein_similarity(s1: str, s2: str) -> float:
    """
    Calculate normalized Levenshtein similarity (0 to 1).
    
    Args:
        s1: First string
        s2: Second string
        
    Returns:
        Similarity score between 0.0 and 1.0
    """
    if not s1 and not s2:
        return 1.0
    if not s1 or not s2:
        return 0.0
    
    distance = levenshtein_distance(s1, s2)
    max_len = max(len(s1), len(s2))
    
    if max_len == 0:
        return 1.0
    
    return 1.0 - (distance / max_len)


def jaccard_similarity(set1: Set, set2: Set) -> float:
    """
    Calculate Jaccard similarity between two sets.
    
    Jaccard similarity is the size of the intersection divided by the size
    of the union of two sets.
    
    Args:
        set1: First set
        set2: Second set
        
    Returns:
        Jaccard similarity between 0.0 and 1.0
    """
    if not set1 and not set2:
        return 1.0
    if not set1 or not set2:
        return 0.0
    
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    
    if union == 0:
        return 0.0
    
    return intersection / union


def tokenize(text: str, min_length: int = 2) -> Set[str]:
    """
    Tokenize text into a set of words.
    
    Args:
        text: Input text
        min_length: Minimum token length
        
    Returns:
        Set of tokens
    """
    if not text or not isinstance(text, str):
        return set()
    
    # Convert to uppercase and remove punctuation
    text = text.upper()
    text = re.sub(r'[^\w\s]', ' ', text)
    
    # Split and filter by length
    tokens = text.split()
    tokens = {t for t in tokens if len(t) >= min_length}
    
    return tokens


def token_jaccard_similarity(text1: str, text2: str, min_length: int = 2) -> float:
    """
    Calculate Jaccard similarity based on tokens extracted from text.
    
    Args:
        text1: First text string
        text2: Second text string
        min_length: Minimum token length
        
    Returns:
        Token-based Jaccard similarity between 0.0 and 1.0
    """
    tokens1 = tokenize(text1, min_length)
    tokens2 = tokenize(text2, min_length)
    
    return jaccard_similarity(tokens1, tokens2)


def soundex(name: str) -> str:
    """
    Generate Soundex phonetic encoding for a name.
    
    Soundex is a phonetic algorithm for indexing names by sound, as pronounced
    in English. The algorithm encodes a name to a 4-character code (1 letter + 3 digits).
    
    Args:
        name: Input name
        
    Returns:
        Soundex code (e.g., "S530" for "SMITH")
    """
    if not name or not isinstance(name, str):
        return "0000"
    
    # Convert to uppercase and keep only letters
    name = name.upper()
    name = re.sub(r'[^A-Z]', '', name)
    
    if not name:
        return "0000"
    
    # Keep first letter
    first_letter = name[0]
    
    # Soundex encoding mapping
    soundex_mapping = {
        'B': '1', 'F': '1', 'P': '1', 'V': '1',
        'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
        'D': '3', 'T': '3',
        'L': '4',
        'M': '5', 'N': '5',
        'R': '6'
    }
    
    # Encode the rest of the name
    encoded = ""
    prev_code = soundex_mapping.get(first_letter, '0')
    
    for char in name[1:]:
        code = soundex_mapping.get(char, '0')
        
        # Skip vowels and 'H', 'W', 'Y'
        if code == '0':
            prev_code = '0'
            continue
        
        # Skip consecutive duplicates
        if code != prev_code:
            encoded += code
            prev_code = code
        
        # Stop when we have 3 digits
        if len(encoded) >= 3:
            break
    
    # Pad with zeros if needed
    encoded = encoded.ljust(3, '0')
    
    return first_letter + encoded[:3]


def soundex_match(name1: str, name2: str) -> bool:
    """
    Check if two names have matching Soundex codes.
    
    Args:
        name1: First name
        name2: Second name
        
    Returns:
        True if Soundex codes match, False otherwise
    """
    return soundex(name1) == soundex(name2)


def name_similarity(name1: str, name2: str) -> float:
    """
    Calculate overall name similarity using multiple algorithms.
    
    Combines Levenshtein similarity, token-based Jaccard similarity,
    and Soundex matching.
    
    Args:
        name1: First name
        name2: Second name
        
    Returns:
        Similarity score between 0.0 and 1.0
    """
    if not name1 or not name2:
        return 0.0
    
    # Normalize names
    name1 = name1.upper().strip()
    name2 = name2.upper().strip()
    
    if name1 == name2:
        return 1.0
    
    # Calculate different similarity measures
    lev_sim = levenshtein_similarity(name1, name2)
    token_sim = token_jaccard_similarity(name1, name2)
    soundex_bonus = 0.2 if soundex_match(name1, name2) else 0.0
    
    # Weighted combination
    # 50% Levenshtein, 30% token-based, 20% Soundex bonus
    similarity = (0.5 * lev_sim) + (0.3 * token_sim) + soundex_bonus
    
    # Cap at 1.0
    return min(similarity, 1.0)


def address_similarity(addr1: str, addr2: str) -> float:
    """
    Calculate address similarity using token-based Jaccard similarity.
    
    Args:
        addr1: First address
        addr2: Second address
        
    Returns:
        Similarity score between 0.0 and 1.0
    """
    if not addr1 or not addr2:
        return 0.0
    
    # Token-based Jaccard works well for addresses
    return token_jaccard_similarity(addr1, addr2)


def phone_similarity(phone1: str, phone2: str) -> float:
    """
    Calculate phone number similarity.
    
    Args:
        phone1: First phone number
        phone2: Second phone number
        
    Returns:
        Similarity score (1.0 for match, 0.0 for no match)
    """
    if not phone1 or not phone2:
        return 0.0
    
    # Extract digits only
    digits1 = re.sub(r'\D', '', phone1)
    digits2 = re.sub(r'\D', '', phone2)
    
    if not digits1 or not digits2:
        return 0.0
    
    # Exact match on last 7-10 digits (handles country codes)
    min_len = min(len(digits1), len(digits2))
    compare_len = min(10, min_len)
    
    if digits1[-compare_len:] == digits2[-compare_len:]:
        return 1.0
    
    return 0.0


def email_similarity(email1: str, email2: str) -> float:
    """
    Calculate email similarity.
    
    Args:
        email1: First email address
        email2: Second email address
        
    Returns:
        Similarity score (1.0 for exact match, partial for similar)
    """
    if not email1 or not email2:
        return 0.0
    
    # Normalize to lowercase
    email1 = email1.lower().strip()
    email2 = email2.lower().strip()
    
    # Exact match
    if email1 == email2:
        return 1.0
    
    # Check username part similarity (before @)
    try:
        user1, domain1 = email1.split('@')
        user2, domain2 = email2.split('@')
        
        # Same domain is a good sign
        if domain1 == domain2:
            user_sim = levenshtein_similarity(user1, user2)
            return 0.5 + (0.5 * user_sim)
        
        # Different domains but similar usernames
        user_sim = levenshtein_similarity(user1, user2)
        return 0.3 * user_sim
    except ValueError:
        # Invalid email format
        return 0.0


def date_similarity(date1: Optional[str], date2: Optional[str]) -> float:
    """
    Calculate date similarity.
    
    Args:
        date1: First date (ISO format)
        date2: Second date (ISO format)
        
    Returns:
        Similarity score (1.0 for exact match, 0.0 for no match)
    """
    if not date1 or not date2:
        return 0.0
    
    # Exact match
    if date1 == date2:
        return 1.0
    
    return 0.0


def compute_similarity(record1: dict, record2: dict, weights: Optional[dict] = None) -> float:
    """
    Compute overall similarity between two records using weighted field similarities.
    
    Args:
        record1: First record dictionary
        record2: Second record dictionary
        weights: Optional dictionary of field weights
        
    Returns:
        Overall similarity score between 0.0 and 1.0
    """
    if weights is None:
        # Default weights for common fields
        weights = {
            'name': 0.4,
            'dob': 0.2,
            'phone': 0.15,
            'mobile': 0.15,
            'address': 0.05,
            'email': 0.05
        }
    
    total_weight = 0.0
    weighted_sum = 0.0
    
    for field, weight in weights.items():
        val1 = record1.get(field)
        val2 = record2.get(field)
        
        # Skip if both values are missing
        if not val1 or not val2:
            continue
        
        # Calculate field-specific similarity
        if 'name' in field.lower():
            sim = name_similarity(val1, val2)
        elif 'phone' in field.lower() or 'mobile' in field.lower():
            sim = phone_similarity(val1, val2)
        elif 'email' in field.lower():
            sim = email_similarity(val1, val2)
        elif 'dob' in field.lower() or 'date' in field.lower():
            sim = date_similarity(val1, val2)
        elif 'address' in field.lower() or 'addr' in field.lower():
            sim = address_similarity(val1, val2)
        else:
            # Generic text similarity
            if isinstance(val1, str) and isinstance(val2, str):
                sim = levenshtein_similarity(val1.upper(), val2.upper())
            else:
                sim = 1.0 if val1 == val2 else 0.0
        
        weighted_sum += weight * sim
        total_weight += weight
    
    if total_weight == 0:
        return 0.0
    
    return weighted_sum / total_weight
