import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config_bdd
from init_db import get_engine, get_session_factory
from services_database.build_collection import build_collections
from tables.collections import Collection


def ingest_collection_pipeline():
    """
    Orchestrates the data ingestion pipeline for movie collections.

    Loads cleaned data from build_collections(), converts types for SQLAlchemy compatibility,
    and performs a bulk-check insertion to add only new collections to the database.

    Returns:
        pd.DataFrame: The processed DataFrame used for ingestion.
    """
    # 1. Generate the cleaned DataFrame from source
    df = build_collections()

    # --- CRITICAL: SQLAlchemy Compatibility ---
    # Convert Pandas-specific types (pd.NA, NaN) to standard Python types (None)
    # This prevents SQLAlchemy from raising errors on nullable integer types.
    df = df.astype(object).where(pd.notnull(df), None)

    engine = get_engine(Config_bdd.DATABASE_URL)
    SessionFactory = get_session_factory(engine)

    # 3. Database Insertion Process
    with SessionFactory() as session:
        try:
            print(f"Début de l'insertion dans {Config_bdd.DATABASE_URL}...")

            # 1. Pre-fetch existing IDs to avoid redundant SQL queries inside the loop
            # Using a set for O(1) lookup performance
            existing_collection_id = {
                g.tmdb_collection_id
                for g in session.query(Collection.tmdb_collection_id).all()
            }

            count_added = 0

            # Iterate through DataFrame rows
            for _, row in df.iterrows():
                tmdb_collection_id = row["tmdb_collection_id"]

                # 2. On n'ajoute que si le nom n'est pas déjà dans la base
                if tmdb_collection_id not in existing_collection_id:
                    # 2. Add only if the ID is not already present in the database or current batch
                    new_genre = Collection(
                        tmdb_collection_id=tmdb_collection_id,
                        collection_name=row["collection_name"],
                    )
                    session.add(new_genre)
                    # Update the local cache to handle duplicates within the source CSV
                    existing_collection_id.add(tmdb_collection_id)
                    count_added += 1
            # Commit the entire transaction
            session.commit()
            print(
                f"✅ Migration terminée : {count_added} nouvelles collections importées."
            )

        except Exception as e:
            # Rollback in case of any failure to maintain data integrity
            session.rollback()
            print(f"❌ Erreur lors de l'ingestion : {e}")
    return df


if __name__ == "__main__":
    ingest_collection_pipeline()
