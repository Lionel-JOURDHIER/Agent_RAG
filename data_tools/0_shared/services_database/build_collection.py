"""
data_tools/0_shared/services_database/build_collection.py
─────────────────────
Source : raw_data/horror_movies_kaggle.csv (colonnes collection + collection_name)
Sortie : data/collections.csv
  id_collection (AUTO_INCREMENT géré par la BDD, absent du CSV)
  tmdb_collection_id (INT)
  collection_name    (VARCHAR 60)
"""

import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from services_database.config import Config
from services_database.export import export_to_parquet


def build_collections() -> pd.DataFrame:
    """
    Extracts and normalizes movie collections into a separate reference table.

    Reads from the Kaggle source, filters for unique collections, and formats
    columns to create a clean mapping between TMDB collection IDs and names.

    Returns:
        pd.DataFrame: A DataFrame containing unique 'tmdb_collection_id' and 'collection_name'.
    """
    # 0. Load specific columns to save memory
    print("Lecture : %s", Config.INPUT_CSV_KAGGLE)
    df = pd.read_csv(
        Config.INPUT_CSV_KAGGLE,
        usecols=["collection", "collection_name"],
        low_memory=False,
    )

    # 1. Integrity check: Remove movies that do not belong to any collection
    df = df.dropna(subset=["collection"])

    # 2. Deduplication: Ensure each collection ID appears only once in the reference table
    df = df.drop_duplicates(subset=["collection"]).reset_index(drop=True)

    # 3. Rename and Type Casting
    df = df.rename(columns={"collection": "tmdb_collection_id"})

    # Ensure ID is integer (avoids .0 floats) and truncate name for DB compatibility
    df["tmdb_collection_id"] = df["tmdb_collection_id"].astype(int)
    df["collection_name"] = df["collection_name"].str.strip().str[:60]

    # 4. Final selection
    df = df[["tmdb_collection_id", "collection_name"]]

    return df


if __name__ == "__main__":
    df_collection = build_collections()
    export_to_parquet(df_collection, Config.PARQUET_COLLECTIONS)
