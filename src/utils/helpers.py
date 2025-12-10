"""Helper utilities."""
import time
import re
from functools import wraps
from config import REQUEST_DELAY_SECONDS
from .logger import logger


def rate_limit(delay=REQUEST_DELAY_SECONDS):
    """Decorator to add delay between function calls."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            time.sleep(delay)
            return func(*args, **kwargs)
        return wrapper
    return decorator


def normalize_revenue(revenue_str: str) -> int:
    """
    Normalize revenue string to integer.
    
    Examples:
        "200 млн" -> 200000000
        "200-500 млн" -> 200000000
        "от 500 млн" -> 500000000
    """
    if not revenue_str or pd.isna(revenue_str):
        return 0
    
    # Convert to string and lowercase
    s = str(revenue_str).lower().strip()
    
    # Remove spaces and commas
    s = s.replace(" ", "").replace(",", "")
    
    # Handle ranges (take minimum)
    if "-" in s or "–" in s:
        s = re.split(r"[-–]", s)[0]
    
    # Handle "от" (from)
    if "от" in s:
        s = s.replace("от", "")
    
    # Extract number
    match = re.search(r"(\d+\.?\d*)", s)
    if not match:
        return 0
    
    num = float(match.group(1))
    
    # Handle млн/млрд
    if "млрд" in s or "billion" in s:
        num *= 1_000_000_000
    elif "млн" in s or "million" in s:
        num *= 1_000_000
    elif "тыс" in s or "thousand" in s:
        num *= 1_000
    
    return int(num)


def clean_inn(inn: str) -> str:
    """Clean and validate INN."""
    if not inn:
        return ""
    
    # Remove all non-digits
    inn_clean = re.sub(r"\D", "", str(inn))
    
    # INN should be 10 or 12 digits
    if len(inn_clean) not in [10, 12]:
        logger.warning(f"Invalid INN length: {inn}")
        return ""
    
    return inn_clean


def clean_text(text: str) -> str:
    """Clean text from extra whitespace."""
    if not text:
        return ""
    
    # Remove extra whitespace
    text = " ".join(text.split())
    
    return text.strip()


import pandas as pd

def safe_get(data: dict, *keys, default=None):
    """Safely get nested dictionary value."""
    for key in keys:
        try:
            data = data[key]
        except (KeyError, TypeError, IndexError):
            return default
    return data
