"""
data_tools/0_shared/services/tmdb_cleaner.py
───────────────────────────
Nettoyage de horror_movies_tmdb_raw.csv selon les règles suivantes :

  1. vote_average  : 0.0 → NaN
  2. Doublons exacts (title + release_date) : garder la ligne avec le moins de NaN
  3. genres        : inchangé
  4. Films futurs  : inchangés
  5. url_rotten    : inchangé
  6. popularity    : 0.0 → NaN
  7. overview      : inchangé
  8. title null    : suppression de la ligne
  9. suppr \n\r    : suppression des retour à la ligne
  +  id_tertiaire  : slug(title)_year ajouté
"""

from pathlib import Path

import pandas as pd
from config import Config
from creation_id import make_id_tertiaire


def fix(input_path: Path, output_path: Path) -> pd.DataFrame:
    """
    Cleans movie data with a focus on record completeness and deduplication.

    This function removes structural artifacts, normalizes scores, and
    uses a 'least-nulls' strategy to resolve duplicate movie entries.

    Args:
        input_path (Path): Path to the raw CSV file.
        output_path (Path): Path where the optimized CSV will be saved.

    Returns:
        pd.DataFrame: Cleaned DataFrame with optimized record selection.
    """
    print("Lecture : %s", input_path)
    df = pd.read_csv(input_path, low_memory=False)
    print("Shape initiale       : %d × %d", *df.shape)

    # -- 1. Global String Formatting --
    # Strip whitespace and remove newline characters from all string-based columns
    str_cols = df.select_dtypes(include="str").columns
    df[str_cols] = df[str_cols].apply(
        lambda col: col.str.replace(r"[\r\n]+", " ", regex=True).str.strip()
    )
    print("Retours à la ligne supprimés sur %d colonnes string", len(str_cols))

    ## -- 2. Vital Data Integrity --
    # Every record must have a title to be valid
    n = df["title"].isna().sum()
    df = df.dropna(subset=["title"]).reset_index(drop=True)
    print("Titres nuls supprimés : %d ligne(s)", n)

    # -- 3. Numeric Normalization --
    # Replace 0.0 with NaN for metrics where 0 is used as a placeholder for unknown
    # Vote average
    n = (df["vote_average"] == 0).sum()
    df["vote_average"] = df["vote_average"].replace(0.0, float("nan"))
    print("vote_average zeros→NaN : %d valeurs", n)

    # Popularity
    n = (df["popularity"] == 0).sum()
    df["popularity"] = df["popularity"].replace(0.0, float("nan"))
    print("popularity zeros→NaN   : %d valeurs", n)

    # -- 4. Qualitative Deduplication --
    # Strategy: When duplicates exist for (title, release_date),
    # keep the record with the most populated fields (fewest NaNs).
    before = len(df)
    df["_nan_count"] = df.isna().sum(axis=1)
    df = (
        df.sort_values("_nan_count")  # moins de NaN en premier
        .drop_duplicates(subset=["title", "release_date"], keep="first")
        .drop(columns=["_nan_count"])
        .reset_index(drop=True)
    )
    print("Doublons exacts supprimés : %d ligne(s)", before - len(df))

    # -- 5. Third ID Generation --
    # Extract year from release_date to generate the tertiary business key
    release_dt = pd.to_datetime(df["release_date"], errors="coerce")
    year_series = release_dt.dt.year

    df["id_tertiaire"] = [
        make_id_tertiaire(t, y) for t, y in zip(df["title"], year_series)
    ]
    n_no_id = df["id_tertiaire"].isna().sum()
    if n_no_id:
        print("Lignes sans id_tertiaire : %d", n_no_id)

    # -- 6. Column Schema Organization --
    priority = [
        "tmdb_id",
        "imdb_id_fetched",
        "id_tertiaire",
        "title",
        "release_date",
        "genres",
        "vote_average",
        "popularity",
        "overview",
        "poster_path",
    ]
    # Reorder present columns and append remaining ones
    other = [c for c in df.columns if c not in priority]
    df = df[priority + other]

    # -- 7. Export to csv --
    df.to_csv(output_path, index=False, encoding="utf-8")
    print("Shape finale         : %d × %d", *df.shape)
    print("Export → %s", output_path)

    return df


if __name__ == "__main__":
    df = fix(Config.RAW_CSV_TMDB, Config.OUTPUT_CSV_TMDB)
