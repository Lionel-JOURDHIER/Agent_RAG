import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config_bdd
from init_db import get_engine, get_session_factory
from services_database.build_scores_tmdb import build_scores_tmdb
from tables.films import Film
from tables.scores_tmdb import ScoreTmdb


def ingest_scores_tmdb_pipeline():
    """
    Executes the ETL pipeline for TMDB scores, including referential integrity
    checks against the Film table and record deduplication.

    Returns:
        pd.DataFrame: The final processed DataFrame after filtering.
    """
    # 1. Generate the cleaned source DataFrame
    df = build_scores_tmdb()

    # --- CRITIQUE: SQLAlchemy Conversion ---
    # Convert Pandas-specific NA values to standard Python 'None' objects.
    # This avoids issues with SQLAlchemy's handling of nullable integer/float types.
    df = df.astype(object).where(pd.notnull(df), None)

    # Initialize Database connection components
    engine = get_engine(Config_bdd.DATABASE_URL)
    SessionFactory = get_session_factory(engine)

    # Data Insertion Phase
    with SessionFactory() as session:
        try:
            print(f"Début de l'insertion dans {Config_bdd.DATABASE_URL}...")

            # Fetch valid TMDB IDs from the Film table to enforce referential integrity
            valid_tmdb = {r[0] for r in session.query(Film.tmdb_id).all()}

            # Keep only rows where the movie exists in the main Film table
            df = df[df["tmdb_id"].isin(valid_tmdb)]

            # Fetch existing records in ScoreTmdb to avoid primary key/unique constraint violations
            existing_id_score_tmdb = {
                g.tmdb_id for g in session.query(ScoreTmdb.tmdb_id).all()
            }

            count_added = 0
            # Iterate through rows for individual record processing
            for _, row in df.iterrows():
                tmdb_id = row["tmdb_id"]

                # Deduplication check: process only if ID is new
                if tmdb_id not in existing_id_score_tmdb:
                    new_score_tmdb = ScoreTmdb(
                        tmdb_id=row["tmdb_id"],
                        vote_average=row["vote_average"],
                        vote_count=row["vote_count"],
                        popularity=row["popularity"],
                    )
                    session.add(new_score_tmdb)

                    # Track added ID to handle duplicate rows within the source DataFrame
                    existing_id_score_tmdb.add(tmdb_id)
                    count_added += 1

            # Commit transaction to the database
            session.commit()
            print(
                f"✅ Migration terminée : {count_added} nouveaux scores_tmdb importées."
            )

        except Exception as e:
            # Rollback changes on failure to maintain consistency
            session.rollback()
            print(f"❌ Erreur lors de l'ingestion : {e}")
    return df


if __name__ == "__main__":
    ingest_scores_tmdb_pipeline()
