"""data_tools/0_shared/services/creation_id.py"""

import pandas as pd
from slug import slugify


def make_id_tertiaire(title: object, year: object) -> str | None:
    """
    Generates a unique identifier string by combining a slugified title and a year.

    The format produced is 'slugified-title_year'. Returns None if either input
    is missing, if the title is empty after stripping, or if the year is invalid.

    Args:
        title (object): The title of the movie or entity (usually str or NaN).
        year (object): The release year (usually int, float or NaN).

    Returns:
        str | None: A formatted string "slug_year" or None if validation fails.
    """
    # Check if either input is a null value (pandas NaN or None)
    if pd.isna(title) or pd.isna(year):
        return None

    # Standardize inputs
    title_str = str(title).strip()

    try:
        # Cast to integer to remove decimals if year was a float
        year_int = int(year)
    except (ValueError, TypeError):
        # Return None if year cannot be converted to an integer
        return None

    # Final validation for empty strings or non-positive years
    if not title_str or year_int <= 0:
        return None

    # Generate slug and concatenate with year
    return f"{slugify(title_str)}_{year_int}"
