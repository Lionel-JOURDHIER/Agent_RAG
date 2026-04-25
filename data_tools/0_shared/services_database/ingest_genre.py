import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config_bdd
from init_db import get_engine, get_session_factory
from services_database.build_genre import build_filmgenres, build_genres
from tables.film_genres import FilmGenre
from tables.films import Film
from tables.genres import Genre


def ingest_genre_pipeline():
    """
    Orchestrates the ingestion of movie genres into the database.

    Retrieves genre data, converts it for SQLAlchemy compatibility, and ensures
    that only unique genre names are persisted to prevent duplicates.

    Returns:
        tuple: A pair of DataFrames (processed genres, secondary metadata/mapping).
    """
    # 1. Generate cleaned DataFrames from the source
    df, df_ = build_genres()

    # --- CRITICAL: SQLAlchemy Compatibility ---
    # Replaces Pandas-specific nulls (pd.NA) with Python native None to ensure
    # compatibility with SQL drivers and the SQLAlchemy ORM.
    df = df.astype(object).where(pd.notnull(df), None)

    # Database engine and session factory setup
    engine = get_engine(Config_bdd.DATABASE_URL)
    SessionFactory = get_session_factory(engine)

    # 2. Insertion Logic
    with SessionFactory() as session:
        try:
            print(f"Début de l'insertion dans {Config_bdd.DATABASE_URL}...")

            # 1. Load existing genre names into memory for O(1) lookups
            # This avoids the N+1 query problem during the loop.
            existing_genres = {
                g.genre_name for g in session.query(Genre.genre_name).all()
            }

            count_added = 0
            # Iterate through the primary genres DataFrame
            for _, row in df.iterrows():
                name = row["genre_name"]

                # Add genre only if it doesn't already exist in DB or current batch
                if name not in existing_genres:
                    new_genre = Genre(genre_name=name)
                    session.add(new_genre)
                    # Update local cache to prevent duplicates within the source data
                    existing_genres.add(name)
                    count_added += 1

            # Commit transaction once all new objects are added to the session
            session.commit()
            print(f"✅ Migration terminée : {count_added} nouveaux genres importés.")

        except Exception as e:
            # Revert session state on error to maintain data integrity
            session.rollback()
            print(f"❌ Erreur lors de l'ingestion : {e}")
    return df, df_


def ingest_filmgenre_pipeline(df_genre, df_exploded):
    """
    Ingests relationship data between films and genres into the FilmGenre table.

    Processes an association DataFrame, validates foreign key existence against
    the Film table, and performs bulk insertion for new relationships.

    Args:
        df_genre (pd.DataFrame): DataFrame containing unique genre identifiers.
        df_exploded (pd.DataFrame): DataFrame where each row represents a film-genre pair.
    """
    # 1. Build the cleaned junction DataFrame
    df = build_filmgenres(df_genre, df_exploded)

    # --- CRITICAL: SQLAlchemy Compatibility ---
    # Convert Pandas types to Python standard types to prevent dialect errors
    df = df.astype(object).where(pd.notnull(df), None)

    engine = get_engine(Config_bdd.DATABASE_URL)
    SessionFactory = get_session_factory(engine)

    # 2. Database Insertion
    with SessionFactory() as session:
        try:
            print(f"Début de l'insertion dans {Config_bdd.DATABASE_URL}...")
            # Referential Integrity Check: Ensure only existing films are processed
            # Queries all valid film IDs from the database
            valid_tmdb = {r[0] for r in session.query(Film.tmdb_id).all()}
            df = df[df["tmdb_id"].isin(valid_tmdb)]

            # Fetch existing associations to avoid duplicates
            # NOTE: Logic should ideally check (film_id, genre_id) pairs, not just film_id
            existing_film = {g.tmdb_id for g in session.query(FilmGenre.tmdb_id).all()}

            count_added = 0
            for _, row in df.iterrows():
                tmdb_id = row["tmdb_id"]

                # Check if the association already exists for this film
                if tmdb_id not in existing_film:
                    new_film_genre = FilmGenre(
                        tmdb_id=tmdb_id, id_genre=row["id_genre"]
                    )
                    session.add(new_film_genre)
                    count_added += 1

            session.commit()
            print(
                f"✅ Migration terminée : {count_added} nouveaux couple film/genres importés."
            )

        except Exception as e:
            # Rollback to maintain atomicity
            session.rollback()
            print(f"❌ Erreur lors de l'ingestion : {e}")


if __name__ == "__main__":
    df_genre, df_exploded = ingest_genre_pipeline()
    ingest_filmgenre_pipeline(df_genre, df_exploded)
