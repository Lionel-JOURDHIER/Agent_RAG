"""
data_tools/0_shared/services_database/build_scores_tmdb.py
──────────────────────
Source :
    raw_data/horror_movies_tmdb.csv (tmdb_id + vote_average + vote_count + popularity)

Sortie : data/scores_tmdb.csv
    id_score_tmdb (AUTO_INCREMENT géré par la BDD, absent du CSV)
    tmdb_id (INT FK),
    vote_average (DECIMAL(3,1)),
    vote_count (INT),
    popularity (DECIMAL(10,4))

"""

import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from services_database.config import Config
from services_database.export import export_to_csv


def build_scores_tmdb() -> pd.DataFrame:
    """
    Extracts and normalizes TMDB performance metrics (ratings, counts, and popularity).

    This function ensures that movie IDs and vote counts are handled as integers,
    while ratings and popularity scores maintain their decimal precision.

    Returns:
        pd.DataFrame: A cleaned DataFrame containing TMDB metrics.
    """
    # 0. Load source data with specific columns to save memory
    print("Lecture : %s", Config.INPUT_CSV_TMDB)
    df = pd.read_csv(
        Config.INPUT_CSV_TMDB,
        usecols=[
            "tmdb_id",
            "vote_average",
            "vote_count",
            "popularity",
        ],
        low_memory=False,
    )

    # 1. Cleaning: Drop rows where all score-related metrics are null
    df = df.dropna(subset=["vote_average", "vote_count", "popularity"], how="all")

    # 2. Type Conversion: Ensure scores are floats
    # Using errors="coerce" to handle potential non-numeric strings in the source
    df["vote_average"] = pd.to_numeric(df["vote_average"], errors="coerce")
    df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce")

    # 3. ID and Count Conversion: Cast to nullable Integer type (Int64)
    df["tmdb_id"] = pd.to_numeric(df["tmdb_id"], errors="coerce").astype("Int64")
    df["vote_count"] = pd.to_numeric(df["vote_count"], errors="coerce").astype("Int64")

    # 4. Final selection and ordering of columns
    df = df[
        [
            "tmdb_id",
            "vote_average",
            "vote_count",
            "popularity",
        ]
    ]

    return df


if __name__ == "__main__":
    df_scores_tmdb = build_scores_tmdb()
    export_to_csv(df_scores_tmdb, Config.CSV_SCORES_TMDB)
