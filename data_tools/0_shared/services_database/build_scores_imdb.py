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

import pandas as pd
from config import Config


def build_scores_imdb() -> pd.DataFrame:
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

    df = df.dropna(subset=["averageRating", "numVotes"], how="all")

    # Nettoyage et limites VARCHAR (selon ton docstring)
    df["tconst"] = df["tconst"].str.strip().str[:10]
    df["title"] = df["title"].str.strip().str[:150]

    # averageRating doit rester un float pour le DECIMAL(3,1)
    df["averageRating"] = pd.to_numeric(df["averageRating"], errors="coerce")

    # numVotes peut être un Int64 (nullable)
    df["numVotes"] = pd.to_numeric(df["numVotes"], errors="coerce").astype("Int64")

    # Renommage pour coller exactement au docstring / BDD
    df = df.rename(columns={"averageRating": "average_rating", "numVotes": "num_votes"})

    df = df[
        [
            "tconst",
            "title",
            "average_rating",
            "num_votes",
        ]
    ]

    df.to_csv(Config.CSV_SCORES_IMDB, index=False, encoding="utf-8")
    print("Export → %s (%d lignes)", Config.CSV_SCORES_IMDB, len(df))
    return df


if __name__ == "__main__":
    build_scores_imdb()
