"""
data_tools/0_shared/services_database/build_scores_imdb.py
──────────────────────
Source : raw_data/horror_movies_imdb_scores.csv (tconst + title + averageRating + numVotes)
Sortie : data/scores_imdb.csv
    id_score_imdb (AUTO_INCREMENT géré par la BDD, absent du CSV)
    tconst (VARCHAR(10) FK),
    title (VARCHAR(150)),
    average_rating (DECIMAL(3,1)),
    num_votes (INT)
"""

import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from services_database.config import Config
from services_database.export import export_to_parquet


def build_scores_imdb() -> pd.DataFrame:
    """
    Extracts and normalizes IMDb ratings and vote counts into a dedicated scores table.

    The function enforces numeric data types and string length constraints to ensure
    compatibility with a database schema (e.g., DECIMAL for ratings, INT for votes).

    Returns:
        pd.DataFrame: A DataFrame with columns [tconst, title, average_rating, num_votes].
    """
    # 0. Ingestion des données IMDb
    print("Lecture : %s", Config.INPUT_CSV_IMDB)
    df = pd.read_csv(
        Config.INPUT_CSV_IMDB,
        usecols=[
            "tconst",
            "title",
            "averageRating",
            "numVotes",
        ],
        low_memory=False,
    )

    # 1. Nettoyage : On ne garde que les films ayant au moins une info de score
    df = df.dropna(subset=["averageRating", "numVotes"], how="all")

    # 2. Normalisation des chaînes (Protection VARCHAR))
    df["tconst"] = df["tconst"].str.strip().str[:10]
    df["title"] = df["title"].str.strip().str[:150]

    # 3. Conversion Numérique
    # averageRating -> float (ex: 7.5)
    df["averageRating"] = pd.to_numeric(df["averageRating"], errors="coerce")
    # numVotes -> Int64 nullable (évite les .0 des floats classiques)
    df["numVotes"] = pd.to_numeric(df["numVotes"], errors="coerce").astype("Int64")

    # 4. Renommage et sélection finale selon le schéma cible
    df = df.rename(columns={"averageRating": "average_rating", "numVotes": "num_votes"})

    df = df[
        [
            "tconst",
            "title",
            "average_rating",
            "num_votes",
        ]
    ]

    return df


if __name__ == "__main__":
    df_scores_imdb = build_scores_imdb()
    export_to_parquet(df_scores_imdb, Config.PARQUET_SCORES_IMDB)
