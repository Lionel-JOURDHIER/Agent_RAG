"""
data_tools/0_shared/services_database/build_realisateur.py
──────────────────────
Source : raw_data/horror_movies_db (colonnes director_id + name)
Sortie : data/realisateurs.csv
  director_id (INT PK)
  name        (VARCHAR 50)
"""

import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from services_database.config import Config
from services_database.export import export_to_csv


def build_realisateurs() -> pd.DataFrame:
    """
    Extracts unique directors from the database source and creates a reference table.

    This function ensures that each director is represented only once,
    normalizes their names, and casts IDs to integers for database consistency.

    Returns:
        pd.DataFrame: A DataFrame containing unique 'director_id' and 'name'.
    """
    # 0. Load raw data with specific columns
    print("Lecture : %s", Config.INPUT_CSV_DB)
    df = pd.read_csv(
        Config.INPUT_CSV_DB, usecols=["director_id", "name"], low_memory=False
    )

    # 1. Deduplication: A director may appear multiple times in the film list
    df = df.drop_duplicates(subset=["director_id"]).reset_index(drop=True)

    # 2. Type Casting and String Normalization
    # Cast to int to remove decimal points from ID
    df["director_id"] = df["director_id"].astype(int)
    df["name"] = df["name"].str.strip().str[:50]

    # 3. Final Selection
    df = df[["director_id", "name"]]

    return df


if __name__ == "__main__":
    df_realisateurs = build_realisateurs()
    export_to_csv(df_realisateurs, Config.CSV_REALISATEURS)
