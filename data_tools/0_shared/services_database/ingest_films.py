import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config_bdd
from init_db import get_engine, get_session_factory
from services_database.build_films import build_films
from tables.films import Film


def ingest_films_pipeline():
    """
    Executes the full ingestion cycle for movie records.

    This function retrieves cleaned film data, ensures SQLAlchemy type compatibility,
    and performs a filtered bulk insertion to populate the database while avoiding
    unique constraint violations on 'tmdb_id'.

    Returns:
        None
    """
    # 1. Generate the cleaned DataFrame from the domain logic function
    df = build_films()

    # --- CRITICAL: SQLAlchemy Compatibility ---
    # Convert Pandas types (pd.NA/NaN) to standard Python (None)
    # Necessary for specific database drivers that do not recognize Pandas-specific nulls.
    df = df.astype(object).where(pd.notnull(df), None)

    # Database connection initialization
    engine = get_engine(Config_bdd.DATABASE_URL)
    SessionFactory = get_session_factory(engine)

    # 2. Insertion Process
    with SessionFactory() as session:
        try:
            print(f"Début de l'insertion dans {Config_bdd.DATABASE_URL}...")
            # Pre-fetch existing film IDs to avoid "Select before Insert" N+1 performance issues
            existing_films = {g.tmdb_id for g in session.query(Film.tmdb_id).all()}

            count_added = 0

            # Iterate through cleaned data
            for _, row in df.iterrows():
                tmdb_id = row["tmdb_id"]

                # Add only if the film is not already present in the database cache
                if tmdb_id not in existing_films:
                    f = Film(
                        tmdb_id=row["tmdb_id"],
                        director_id=row["director_id"],
                        id_collection=row["id_collection"],
                        imdb_id=row["imdb_id"],
                        id_tertiaire=row["id_tertiaire"],
                        title=row["title"],
                        original_title=row["original_title"],
                        original_language=row["original_language"],
                        release_date=row["release_date"],
                        status=row["status"],
                        runtime=row["runtime"],
                        overview=row["overview"],
                        tagline=row["tagline"],
                        poster_path=row["poster_path"],
                        budget=row["budget"],
                        revenue=row["revenue"],
                    )
                    session.add(f)

                    # Update cache to handle potential duplicates within the source data itself
                    existing_films.add(tmdb_id)
                    count_added += 1

            # Finalize the transaction
            session.commit()
            print(f"✅ Migration terminée : {count_added} films importés.")
        except Exception as e:
            # Revert all changes in this session if any error occurs
            session.rollback()
            print(f"❌ Erreur lors de l'ingestion : {e}")


if __name__ == "__main__":
    ingest_films_pipeline()
