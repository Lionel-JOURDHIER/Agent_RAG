"""
data_tools/0_shared/services_database/build_collection.py
─────────────────────
Source : raw_data/horror_movies_kaggle.csv (colonnes collection + collection_name)
Sortie : data/collections.csv
  id_collection (AUTO_INCREMENT géré par la BDD, absent du CSV)
  tmdb_collection_id (INT)
  collection_name    (VARCHAR 60)
"""

import pandas as pd
from config import Config


def build_collections() -> pd.DataFrame:
    print("Lecture : %s", Config.INPUT_CSV_KAGGLE)
    df = pd.read_csv(
        Config.INPUT_CSV_KAGGLE,
        usecols=["collection", "collection_name"],
        low_memory=False,
    )

    # Suppression des films sans collection
    df = df.dropna(subset=["collection"])

    # Déduplication — une collection = une ligne
    df = df.drop_duplicates(subset=["collection"]).reset_index(drop=True)

    # Renommage + typage
    df = df.rename(columns={"collection": "tmdb_collection_id"})
    df["tmdb_collection_id"] = df["tmdb_collection_id"].astype(int)
    df["collection_name"] = df["collection_name"].str.strip().str[:60]

    df = df[["tmdb_collection_id", "collection_name"]]

    df.to_csv(Config.CSV_COLLECTIONS, index=False, encoding="utf-8")
    print("Export → %s (%d lignes)", Config.CSV_COLLECTIONS, len(df))
    return df


if __name__ == "__main__":
    build_collections()
