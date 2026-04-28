"""
config.utils
~~~~~~~~~~~~~
Shared utility functions used across multiple pipeline layers.

Centralizes common data cleaning operations to avoid code
duplication (DRY principle).
"""

from typing import Optional

import pandas as pd


def normalizar_sku(series: pd.Series) -> pd.Series:
    """Normalizes a Pandas Series of SKU values.

    Applies the following transformations:
        1. Casts to string.
        2. Strips leading/trailing whitespace.
        3. Removes trailing ``.0`` suffixes introduced by
           Excel's automatic numeric inference.

    Args:
        series: Raw SKU column from a DataFrame.

    Returns:
        Cleaned SKU Series ready for joins and lookups.
    """
    # Note: pattern is r"\.0$" (NOT r"\\.0$"). The double-backslash
    # variant matches the literal sequence ``\.0`` and never trimmed
    # the Excel-induced ``.0`` suffix — a silent join-killer bug.
    return (
        series
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )


def truncar(valor: Optional[str], limite: int) -> str:
    """Truncates a string to fit a VARCHAR column safely.

    Centralized helper to avoid duplicating ``_truncar`` across every
    marketplace adapter. Returns an empty string for ``None``/falsy
    inputs so adapters never propagate ``NaN`` into the database.

    Args:
        valor: Input string (may be ``None``).
        limite: Maximum length allowed by the destination column.

    Returns:
        ``str(valor)`` truncated to ``limite`` characters, or ``""``.
    """
    if not valor:
        return ""
    return str(valor)[:limite]
