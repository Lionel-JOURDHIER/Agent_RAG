import re

from unidecode import unidecode


def slugify(title: str) -> str:
    """
    Converts a string into a URL-friendly slug.

    This function removes accents, converts to lowercase, replaces special
    characters/spaces with underscores, and ensures no leading or trailing underscores.

    Args:
        title (str): The raw string to be converted into a slug.

    Returns:
        str: The sanitized, slugified version of the input string.
    """
    # Remove accents and normalize to ASCII characters
    title = unidecode(title)

    # Standardize casing to lowercase
    title = title.lower()

    # Remove apostrophes and backticks to prevent gaps (e.g., L'arbre -> Larbre)
    title = re.sub(r"[''`]", "", title)

    # Replace any sequence of non-alphanumeric characters with a single underscore
    title = re.sub(r"[^a-z0-9]+", "_", title)

    # Replace double "__" in "_" :
    title = re.sub(r"_{2,}", "_", title)

    # Strip underscores from both ends
    return title.strip("_")
