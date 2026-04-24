"""
data_tools/1_web_scrapping/src/rt_cleaner.py
───────────────────────────────
Nettoyage de horror_movies_rt_scores.csv selon les règles suivantes :

  1. Doublons exacts (url_rotten ou title+year)  : suppression
  2. Lignes sans title ET sans year              : suppression
  3. rt_tomatometer zéros                        : conservés (0% valide sur RT)
  4. rt_audience_score zéros                     : conservés (0% valide sur RT)
  5. year float64                                : → Int64 (entier nullable)
  6. id_tertiaire                                : slug(title)_year ajouté
"""

from pathlib import Path

import pandas as pd
from config import Config
from creation_id import make_id_tertiaire


def fix(input_path: Path, output_path: Path) -> pd.DataFrame:
    """
    Refines Rotten Tomatoes horror movie data by deduplicating entries
    and recovering missing release years from URLs.

    Args:
        input_path (Path): Path to the raw scraped CSV file.
        output_path (Path): Path to the cleaned CSV file.

    Returns:
        pd.DataFrame: The cleaned dataset with recovered years and business IDs.
    """
    print("Lecture : %s", input_path)
    df = pd.read_csv(input_path, low_memory=False)
    print("Shape initiale                  : %d × %d", *df.shape)

    # -- 1. Data Pruning --
    # Remove rows where both critical identifying fields are missing
    mask_both_null = df["title"].isna() & df["year"].isna()
    n = mask_both_null.sum()
    df = df[~mask_both_null].reset_index(drop=True)
    print("Sans title + year supprimées    : %d ligne(s)", n)

    # -- 2. Deduplication --
    # Step A: Remove exact URL duplicates (unique web resource)
    before = len(df)
    df = df.drop_duplicates(subset=["url_rotten"], keep="first").reset_index(drop=True)
    print("Doublons url_rotten supprimés   : %d ligne(s)", before - len(df))

    # Step B: Remove duplicates based on Title + Year (business uniqueness)
    before = len(df)
    df = df.drop_duplicates(subset=["title", "year"], keep="first").reset_index(
        drop=True
    )
    print("Doublons title+year supprimés   : %d ligne(s)", before - len(df))

    # -- 3. Year Recovery Logic --
    # Extract year from URL pattern (e.g., .../m/maniac_1980 -> 1980)
    year_from_url = df["url_rotten"].str.extract(r"_(\d{4})$")[0].astype("float")

    # Fill missing years only if extraction was successful
    mask_null_year = df["year"].isna()
    recovered = mask_null_year & year_from_url.notna()
    df.loc[recovered, "year"] = year_from_url[recovered]
    print("Années récupérées depuis URL    : %d ligne(s)", recovered.sum())

    # Convert to nullable integer type for consistency
    df["year"] = df["year"].astype("Int64")
    print("year converti en Int64")

    # -- 4. third ID Generation --
    df["id_tertiaire"] = [
        make_id_tertiaire(t, y) for t, y in zip(df["title"], df["year"])
    ]
    n_no_id = df["id_tertiaire"].isna().sum()
    if n_no_id:
        print("Lignes sans id_tertiaire        : %d", n_no_id)

    # -- 5. Formatting & Export --
    priority = [
        "id_tertiaire",
        "title",
        "year",
        "url_rotten",
        "rt_tomatometer",
        "rt_audience_score",
        "rt_critics_consensus",
    ]
    # Reorder columns with priority list first
    other = [c for c in df.columns if c not in priority]
    df = df[priority + other]

    # Export to CSV
    df.to_csv(output_path, index=False, encoding="utf-8")
    print("Shape finale                    : %d × %d", *df.shape)
    print("Export → %s", output_path)

    return df


if __name__ == "__main__":
    fix(Config.RAW_CSV_RT, Config.OUTPUT_CSV_RT)
