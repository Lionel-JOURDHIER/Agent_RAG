"""data_tools/0_shared/services/creation_id.py"""

import pandas as pd
from slug import slugify


def make_id_tertiaire(title: object, year: object) -> str | None:
    """Retourne slug(title)_year ou None si l'un des deux est manquant."""
    if pd.isna(title) or pd.isna(year):
        return None
    title_str = str(title).strip()
    year_int = int(year)
    if not title_str or year_int <= 0:
        return None
    return f"{slugify(title_str)}_{year_int}"
