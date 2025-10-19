import pandas as pd
import hashlib

def generate_key(*args) -> str:
    """Generate a deterministic surrogate key from input values."""
    combined = '|'.join(str(arg) for arg in args if arg is not None)
    return hashlib.md5(combined.encode()).hexdigest()[:16]
