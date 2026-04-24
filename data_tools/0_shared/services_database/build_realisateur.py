"""
data_tools/0_shared/services_database/build_realisateur.py
──────────────────────
Source : raw_data/horror_movies_db (colonnes director_id + name)
Sortie : data/realisateurs.csv
  director_id (INT PK)
  name        (VARCHAR 50)
"""

import pandas as pd
from config import Config


def build() -> pd.DataFrame:
    print("Lecture : %s", Config.INPUT_CSV_DB)
    df = pd.read_csv(
        Config.INPUT_CSV_DB, usecols=["director_id", "name"], low_memory=False
    )

    # Un réalisateur peut apparaître sur plusieurs films → déduplication
    df = df.drop_duplicates(subset=["director_id"]).reset_index(drop=True)

    df["director_id"] = df["director_id"].astype(int)
    df["name"] = df["name"].str.strip().str[:50]

    df = df[["director_id", "name"]]

    df.to_csv(Config.CSV_REALISATEURS, index=False, encoding="utf-8")
    print("Export → %s (%d lignes)", Config.CSV_REALISATEURS, len(df))
    return df


if __name__ == "__main__":
    build()
