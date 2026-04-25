"""
data_tools/0_shared/services_database/build_scores_tmdb.py
──────────────────────
Source :
    raw_data/horror_movies_tmdb.csv (tmdb_id + imdb_id_fetched + id_tertiaire + title + release_date + overview + poster_path)
    raw_data/horror_movies_kaggle.csv (id_collection + original_title + original_language + status + runtime + tagline + budget + revenue)
    raw_data/horror_movies_db.csv (director_id)

Sortie : data/scores_tmdb.csv
    tmdb_id (INT PK),
    director_id (INT FK),
    id_collection (INT FK),
    imdb_id(VARCHAR(10) UK),
    id_tertiaire(VARCHAR(255), UK),
    title (VARCHAR(200), NON NULL),
    original_title (VARCHAR(200)),
    original_language (CHAR(2)),
    release_date (DATE),
    status (VARCHAR(15)),
    runtime (SMALLINT),
    overview (TEXT),
    tagline (VARCHAR(260)),
    poster_path (VARCHAR(65)),
    budget (BIGINT),
    revenue (BIGINT),
"""

import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from services_database.config import Config
from services_database.export import export_to_parquet


def build_films() -> pd.DataFrame:
    """
    Merges TMDB, Kaggle, and internal Database sources into a consolidated films table.

    The pipeline performs:
    1. Loading and cleaning core TMDB data.
    2. Joining supplemental financial and status data from Kaggle.
    3. Mapping director IDs from the local database.
    4. Enforcing strict data types and string length constraints for SQL safety.

    Returns:
        pd.DataFrame: A unified DataFrame ready for final storage.
    """
    # -- 1. TMDB: Core dataset --
    print("Lecture TMDB : %s", Config.INPUT_CSV_TMDB)
    tmdb = pd.read_csv(
        Config.INPUT_CSV_TMDB,
        usecols=[
            "tmdb_id",
            "imdb_id_fetched",
            "id_tertiaire",
            "title",
            "release_date",
            "overview",
            "poster_path",
        ],
        low_memory=False,
    )
    tmdb = tmdb.rename(columns={"imdb_id_fetched": "imdb_id"})
    # Coerce to numeric then nullable Int64 to avoid float conversion
    tmdb["tmdb_id"] = pd.to_numeric(tmdb["tmdb_id"], errors="coerce").astype("Int64")
    tmdb = tmdb.dropna(subset=["tmdb_id"])

    # -- 2. KAGGLE: Supplementary columns --
    print("Lecture Kaggle : %s", Config.INPUT_CSV_KAGGLE)
    kaggle = pd.read_csv(
        Config.INPUT_CSV_KAGGLE,
        usecols=[
            "id",
            "collection",
            "original_title",
            "original_language",
            "status",
            "runtime",
            "tagline",
            "budget",
            "revenue",
        ],
        low_memory=False,
    )
    kaggle = kaggle.rename(columns={"id": "tmdb_id", "collection": "id_collection"})
    kaggle["tmdb_id"] = pd.to_numeric(kaggle["tmdb_id"], errors="coerce").astype(
        "Int64"
    )
    kaggle["id_collection"] = pd.to_numeric(
        kaggle["id_collection"], errors="coerce"
    ).astype("Int64")

    # -- 3. DB: Director mapping --
    print("Lecture DB : %s", Config.INPUT_CSV_DB)
    db = pd.read_csv(
        Config.INPUT_CSV_DB,
        usecols=["uid", "director_id"],
        low_memory=False,
    )
    db = db.rename(columns={"uid": "tmdb_id"})
    db = db.drop_duplicates(subset=["tmdb_id"])
    db["tmdb_id"] = pd.to_numeric(db["tmdb_id"], errors="coerce").astype("Int64")
    db["director_id"] = pd.to_numeric(db["director_id"], errors="coerce").astype(
        "Int64"
    )

    # -- 4. Multi-source Merge --
    # Left joins to preserve TMDB as the primary source of truth
    df = tmdb.merge(kaggle, on="tmdb_id", how="left")
    df = df.merge(db, on="tmdb_id", how="left")

    # -- 5. Data Normalization & Formatting --
    df = df.dropna(subset=["title"])

    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce").dt.date
    df["runtime"] = pd.to_numeric(df["runtime"], errors="coerce").astype("Int64")
    df["budget"] = pd.to_numeric(df["budget"], errors="coerce").astype("Int64")
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").astype("Int64")
    df["original_language"] = df["original_language"].str.strip().str[:2]
    df["status"] = df["status"].str.strip().str[:15]
    df["imdb_id"] = df["imdb_id"].str.strip().str[:10]
    df["id_tertiaire"] = df["id_tertiaire"].str.strip().str[:255]
    df["title"] = df["title"].str.strip().str[:200]
    df["original_title"] = df["original_title"].str.strip().str[:200]
    df["tagline"] = df["tagline"].str.strip().str[:260]
    df["poster_path"] = df["poster_path"].str.strip().str[:65]

    # TODO
    df = df.replace({"NOT_FOUND": None, "": None, "N/A": None})
    mask_tert = df["id_tertiaire"].notna()
    df_with_tert = df[mask_tert].drop_duplicates(subset=["id_tertiaire"], keep="first")
    df_without_tert = df[~mask_tert]
    df = pd.concat([df_with_tert, df_without_tert], ignore_index=True)

    # -- 6. Column Ordering --
    df = df[
        [
            "tmdb_id",
            "director_id",
            "id_collection",
            "imdb_id",
            "id_tertiaire",
            "title",
            "original_title",
            "original_language",
            "release_date",
            "status",
            "runtime",
            "overview",
            "tagline",
            "poster_path",
            "budget",
            "revenue",
        ]
    ]
    return df


if __name__ == "__main__":
    df = build_films()
    export_to_parquet(df, Config.PARQUET_FILMS)
