"""
data_tools/0_shared/services/db_cleaner.py
──────────────────────────────
Nettoyage de horror_movies_database.csv selon les règles suivantes :

  1. budget < 1000 et > 0 (unités mixtes)  : → NaN
  2. budget et revenue zéros               : → NaN
  3. vote_average et vote_count zéros      : → NaN
  4. popularity zéros                      : conservés
  5. colonne `id` séquentielle             : supprimée  (`uid` = vrai ID TMDB)
  6. id_tertiaire                          : slug(title)_year ajouté


"""

from pathlib import Path

import pandas as pd
from config import Config
from creation_id import make_id_tertiaire

# ── Pipeline ──────────────────────────────────────────────────────────────────


def fix(input_path: Path, output_path: Path) -> pd.DataFrame:
    """
    Cleans and normalizes horror movie dataset by fixing suspicious values and reordering columns.

    This function handles common data quality issues like 0 values that represent missing data,
    converts date formats, and generates a business ID.

    Args:
        input_path (Path): Path to the source CSV file.
        output_path (Path): Path where the cleaned CSV will be saved.

    Returns:
        pd.DataFrame: The cleaned and reordered DataFrame.
    """
    # 0. Initial Load
    print("Lecture : %s", input_path)
    df = pd.read_csv(input_path, low_memory=False)
    print("Shape initiale                      : %d × %d", *df.shape)

    # 1. Remove sequential ID column (irrelevant for the final dataset)
    df = df.drop(columns=["id"])
    print("Colonne `id` séquentielle supprimée")

    # 2. Fix suspicious budget values
    # Values between 0 and 1000 are likely expressed in Millions instead of Dollars
    mask_sus = (df["budget"] > 0) & (df["budget"] < 1000)
    n = mask_sus.sum()
    df.loc[mask_sus, "budget"] = float("nan")
    print("budget unités suspectes → NaN       : %d valeur(s)", n)

    # 3. Handle zero values as missing data (NaN)
    # Budget
    n = (df["budget"] == 0).sum()
    df["budget"] = df["budget"].replace(0.0, float("nan"))
    print("budget zéros → NaN                  : %d valeur(s)", n)

    # Revenue
    n = (df["revenue"] == 0).sum()
    df["revenue"] = df["revenue"].replace(0.0, float("nan"))
    print("revenue zéros → NaN                 : %d valeur(s)", n)

    # Vote average
    n = (df["vote_average"] == 0).sum()
    df["vote_average"] = df["vote_average"].replace(0.0, float("nan"))
    print("vote_average zéros → NaN            : %d valeur(s)", n)

    # Vote count
    n = (df["vote_count"] == 0).sum()
    df["vote_count"] = df["vote_count"].replace(0, pd.NA)
    print("vote_count zéros → NaN              : %d valeur(s)", n)

    # 4. Generate third Key: id_tertiaire
    # Extract year from release_date for the slug generation
    release_dt = pd.to_datetime(df["release_date"], errors="coerce")
    year_series = release_dt.dt.year

    # List comprehension for fast business key generation
    df["id_tertiaire"] = [
        make_id_tertiaire(t, y) for t, y in zip(df["title"], year_series)
    ]
    n_no_id = df["id_tertiaire"].isna().sum()
    if n_no_id:
        print("Lignes sans id_tertiaire            : %d", n_no_id)

    # 5. Reorder columns for better data exploration
    priority = [
        "uid",
        "id_tertiaire",
        "title",
        "original_title",
        "release_date",
        "budget",
        "revenue",
        "popularity",
        "vote_average",
        "vote_count",
        "overview",
        "tagline",
        "director_id",
        "name",
    ]
    # Keep columns present in priority list then append others
    other = [c for c in df.columns if c not in priority]
    df = df[priority + other]

    # 6. Final Export
    df.to_csv(output_path, index=False, encoding="utf-8")
    print("Shape finale                        : %d × %d", *df.shape)
    print("Export → %s", output_path)

    return df


if __name__ == "__main__":
    df = fix(Config.RAW_CSV_DB, Config.OUTPUT_CSV_DB)
