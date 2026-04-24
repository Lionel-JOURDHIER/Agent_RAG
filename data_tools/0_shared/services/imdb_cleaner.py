"""
data_tools/0_shared/services/imdb_cleaner.py
─────────────────────────────────
Nettoyage de horror_movies_imdb_scores.csv selon les règles suivantes :

  1. title null (1 ligne)          : suppression
  2. id_tertiaire                  : non généré (pas de colonne année)
  3. numVotes                      : inchangé (float64 conservé)
  4. genres format virgule         : normalisé → "Horror, Drama" (virgule + espace)
  5. primaryTitle null             : primaryTitle = title

"""

from pathlib import Path

import pandas as pd
from config import Config


def normalize_genres(raw: object) -> str | None:
    """
    Normalizes IMDb genre strings to a standardized "Genre1, Genre2" format.

    Args:
        raw (object): The raw genre string from the dataset (e.g., "Horror,Drama" or NaN).

    Returns:
        str | None: A cleaned string with consistent spacing or None if input is invalid.
    """
    if pd.isna(raw):
        return None
    parts = [g.strip() for g in str(raw).split(",") if g.strip()]
    return ", ".join(parts)


def fix(input_path: Path, output_path: Path) -> pd.DataFrame:
    """
    Executes a cleaning pipeline on movie metadata.

    Handles newline stripping, null title removal, title fallback logic,
    and genre normalization.

    Args:
        input_path (Path): Source file path (IMDb format).
        output_path (Path): Destination file path for cleaned data.

    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    print("Lecture : %s", input_path)
    df = pd.read_csv(input_path, low_memory=False)
    print("Shape initiale                        : %d × %d", *df.shape)

    # -- Clean line breaks in all string columns to prevent CSV corruption --
    # We identify columns with 'object' or 'string' type
    str_cols = df.select_dtypes(include="str").columns
    df[str_cols] = df[str_cols].apply(
        lambda col: col.str.replace(r"[\r\n]+", " ", regex=True).str.strip()
    )
    print("Retours à la ligne nettoyés           : %d colonnes", len(str_cols))

    # -- 1. Drop rows where the mandatory 'title' field is missing --
    n = df["title"].isna().sum()
    df = df.dropna(subset=["title"]).reset_index(drop=True)
    print("Titres nuls supprimés                 : %d ligne(s)", n)

    # -- 2. Fallback logic: Fill primaryTitle with title if missing --
    mask = df["primaryTitle"].isna()
    n = mask.sum()
    df.loc[mask, "primaryTitle"] = df.loc[mask, "title"]
    print("primaryTitle rempli depuis title      : %d valeur(s)", n)

    # -- 3. Normalize genres string format --
    df["genres"] = df["genres"].apply(normalize_genres)
    print("genres normalisés (virgule + espace)")

    # -- 4. Column Reordering --
    priority = [
        "tconst",
        "tmdb_id",
        "title",
        "primaryTitle",
        "genres",
        "averageRating",
        "numVotes",
    ]
    # Ensure we only try to reorder columns that exist
    other = [c for c in df.columns if c not in priority]
    df = df[priority + other]

    # -- 5. Final Export --
    df.to_csv(output_path, index=False, encoding="utf-8")
    print("Shape finale                          : %d × %d", *df.shape)
    print("Export → %s", output_path)

    return df


if __name__ == "__main__":
    df = fix(Config.RAW_CSV_IMDB, Config.OUTPUT_CSV_IMDB)
