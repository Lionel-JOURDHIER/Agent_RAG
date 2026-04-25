import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config_bdd
from init_db import get_engine, get_session_factory
from services_database.build_realisateur import build_realisateurs
from tables.realisateurs import Realisateur


def ingest_realisateurs_pipeline():
    """
    Orchestrates the ETL process for directors, from DataFrame generation
    to database insertion with deduplication logic.

    Returns:
        pd.DataFrame: The processed and cleaned DataFrame used for ingestion.

    Raises:
        Exception: If database connection or insertion fails, triggering a rollback.
    """
    # 1. Generate the cleaned DataFrame
    df = build_realisateurs()

    # --- CRITIQUE: SQLAlchemy Conversion ---
    # Convert Pandas-specific NA values (pd.NA) to standard Python None
    # to prevent SQLAlchemy from raising errors on nullable integer types.
    df = df.astype(object).where(pd.notnull(df), None)

    # Initialize database connection components
    engine = get_engine(Config_bdd.DATABASE_URL)
    SessionFactory = get_session_factory(engine)

    # 2. Insertion Phase
    with SessionFactory() as session:
        try:
            print(f"Début de l'insertion dans {Config_bdd.DATABASE_URL}...")

            # Fetch all existing director IDs to prevent N+1 query issues during duplication checks
            existing_director_id = {
                g.director_id for g in session.query(Realisateur.director_id).all()
            }

            count_added = 0
            # Iterate through DataFrame rows
            for _, row in df.iterrows():
                director_id = row["director_id"]

                # Add only if the ID does not exist in the database or the current batch
                if director_id not in existing_director_id:
                    new_director = Realisateur(
                        director_id=director_id,
                        name=row["name"],
                    )
                    session.add(new_director)
                    # Update local set to prevent duplicates within the same CSV/DataFrame batch
                    existing_director_id.add(director_id)
                    count_added += 1

            # Persist changes to the database
            session.commit()
            print(
                f"✅ Migration terminée : {count_added} nouveaux réalisateurs importées."
            )

        except Exception as e:
            # Rollback transaction in case of error to maintain data integrity
            session.rollback()
            print(f"❌ Erreur lors de l'ingestion : {e}")
    return df


if __name__ == "__main__":
    ingest_realisateurs_pipeline()
