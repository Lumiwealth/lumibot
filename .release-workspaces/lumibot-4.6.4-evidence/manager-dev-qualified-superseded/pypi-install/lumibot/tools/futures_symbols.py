"""
Futures Symbol Utilities

Pure functions for parsing, matching, and ranking futures contract symbols.
Supports multiple broker formats (Tradovate, IB, ProjectX) without dependencies.
"""

import re
from datetime import datetime, date
from typing import Dict, Optional, Set, Tuple


def parse_contract_symbol(symbol: str) -> Optional[Dict[str, Optional[str]]]:
    """
    Parse a futures contract symbol into components.
    
    Supports formats:
    - MNQU5 (Tradovate: root + month_code + 1-digit year)
    - MNQU25 (Standard: root + month_code + 2-digit year)
    - MNQ.U25 (Dot notation: root.month_code + 2-digit year)
    - MNQU2025 (Full year: root + month_code + 4-digit year)
    
    Parameters
    ----------
    symbol : str
        Contract symbol to parse
        
    Returns
    -------
    dict or None
        Parsed components: {root, month_code, year_1d, year_2d, year_4d}
        Returns None if symbol doesn't match any known pattern
    """
    if not symbol or not isinstance(symbol, str):
        return None
    
    symbol = symbol.upper().strip()
    
    # Pattern 1: Root + month code + 1-digit year (e.g., MNQU5)
    match = re.match(r'^([A-Z]+)([FGHJKMNQUVXZ])(\d)$', symbol)
    if match:
        root, month_code, year_1d = match.groups()
        year_2d = f"2{year_1d}" if int(year_1d) <= 5 else f"1{year_1d}"  # 2025 vs 2019
        year_4d = f"20{year_2d}" if year_2d.startswith('2') else f"20{year_2d}"
        return {
            'root': root,
            'month_code': month_code,
            'year_1d': year_1d,
            'year_2d': year_2d,
            'year_4d': year_4d
        }
    
    # Pattern 2: Root + month code + 2-digit year (e.g., MNQU25)
    match = re.match(r'^([A-Z]+)([FGHJKMNQUVXZ])(\d{2})$', symbol)
    if match:
        root, month_code, year_2d = match.groups()
        year_1d = year_2d[-1]
        year_4d = f"20{year_2d}"
        return {
            'root': root,
            'month_code': month_code,
            'year_1d': year_1d,
            'year_2d': year_2d,
            'year_4d': year_4d
        }
    
    # Pattern 3: Root.month_code + 2-digit year (e.g., MNQ.U25)
    match = re.match(r'^([A-Z]+)\.([FGHJKMNQUVXZ])(\d{2})$', symbol)
    if match:
        root, month_code, year_2d = match.groups()
        year_1d = year_2d[-1]
        year_4d = f"20{year_2d}"
        return {
            'root': root,
            'month_code': month_code,
            'year_1d': year_1d,
            'year_2d': year_2d,
            'year_4d': year_4d
        }
    
    # Pattern 4: Root + month code + 4-digit year (e.g., MNQU2025)
    match = re.match(r'^([A-Z]+)([FGHJKMNQUVXZ])(\d{4})$', symbol)
    if match:
        root, month_code, year_4d = match.groups()
        year_2d = year_4d[-2:]
        year_1d = year_4d[-1:]
        return {
            'root': root,
            'month_code': month_code,
            'year_1d': year_1d,
            'year_2d': year_2d,
            'year_4d': year_4d
        }
    
    return None


def symbol_matches_root(symbol: str, root: str) -> bool:
    """
    Check if a contract symbol matches the given root symbol.
    
    Parameters
    ----------
    symbol : str
        Contract symbol to check
    root : str
        Root symbol to match against
        
    Returns
    -------
    bool
        True if symbol represents a contract of the root
    """
    if not symbol or not root:
        return False
    
    # Direct match (for cont_future positions)
    if symbol.upper() == root.upper():
        return True
    
    # Parse as contract and check root
    parsed = parse_contract_symbol(symbol)
    if parsed and parsed['root'] == root.upper():
        return True
    
    return False


def from_ib_expiration_to_code(expiration_date) -> Optional[Tuple[str, str]]:
    """
    Convert IB-style expiration date to month code and 2-digit year.
    
    Parameters
    ----------
    expiration_date : date, datetime, or str
        Expiration date in YYYYMM format or date object
        
    Returns
    -------
    tuple or None
        (month_code, year_2d) or None if invalid
    """
    if expiration_date is None:
        return None
    
    try:
        if isinstance(expiration_date, str):
            if len(expiration_date) == 6:  # YYYYMM
                year = int(expiration_date[:4])
                month = int(expiration_date[4:])
            else:
                return None
        elif isinstance(expiration_date, (date, datetime)):
            year = expiration_date.year
            month = expiration_date.month
        else:
            return None
        
        # Month to code mapping
        month_codes = {
            1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
            7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
        }
        
        if month not in month_codes:
            return None
        
        month_code = month_codes[month]
        year_2d = f"{year % 100:02d}"
        
        return (month_code, year_2d)
        
    except (ValueError, TypeError):
        return None


def generate_symbol_variants(root: str, month_code: str, year_1d: str, year_2d: str, year_4d: str) -> Set[str]:
    """
    Generate all possible symbol variants for a contract.
    
    Parameters
    ----------
    root : str
        Root symbol (e.g., 'MNQ')
    month_code : str
        Month code (e.g., 'U')
    year_1d : str
        1-digit year (e.g., '5')
    year_2d : str
        2-digit year (e.g., '25')
    year_4d : str
        4-digit year (e.g., '2025')
        
    Returns
    -------
    set
        All possible symbol variants
    """
    variants = set()
    
    # Add all format variants
    variants.add(f"{root}{month_code}{year_1d}")      # MNQU5
    variants.add(f"{root}{month_code}{year_2d}")      # MNQU25
    variants.add(f"{root}.{month_code}{year_2d}")     # MNQ.U25
    variants.add(f"{root}{month_code}{year_4d}")      # MNQU2025
    
    return variants


def get_contract_priority_key(symbol: str, priority_list: list) -> int:
    """
    Get priority ranking for a contract symbol based on priority list.
    
    Parameters
    ----------
    symbol : str
        Contract symbol
    priority_list : list
        List of contract symbols in priority order
        
    Returns
    -------
    int
        Priority index (lower = higher priority), or 999999 if not found
    """
    if not symbol or not priority_list:
        return 999999
    
    # Direct match
    if symbol in priority_list:
        return priority_list.index(symbol)
    
    # Parse symbol and generate variants to check against priority list
    parsed = parse_contract_symbol(symbol)
    if not parsed:
        return 999999
    
    variants = generate_symbol_variants(
        parsed['root'],
        parsed['month_code'],
        parsed['year_1d'],
        parsed['year_2d'],
        parsed['year_4d']
    )
    
    # Find best matching priority
    best_priority = 999999
    for variant in variants:
        if variant in priority_list:
            priority = priority_list.index(variant)
            best_priority = min(best_priority, priority)
    
    return best_priority


def build_ib_contract_variants(root: str, expiration_date) -> Set[str]:
    """
    Build contract symbol variants from IB-style root + expiration.
    
    Parameters
    ----------
    root : str
        Root symbol
    expiration_date : date, datetime, or str
        Expiration date
        
    Returns
    -------
    set
        Set of possible contract symbols
    """
    code_and_year = from_ib_expiration_to_code(expiration_date)
    if not code_and_year:
        return set()
    
    month_code, year_2d = code_and_year
    year_1d = year_2d[-1]
    year_4d = f"20{year_2d}"
    
    return generate_symbol_variants(root, month_code, year_1d, year_2d, year_4d)
