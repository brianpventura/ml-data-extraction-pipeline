"""
config.utils
~~~~~~~~~~~~~
Shared utility functions used across multiple pipeline layers.

Centralizes common data cleaning operations to avoid code
duplication (DRY principle).
"""

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
    return (
        series
        .astype(str)
        .str.strip()
        .str.replace(r"\\.0$", "", regex=True)
    )
