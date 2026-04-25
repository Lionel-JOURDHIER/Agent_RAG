import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config_bdd
from init_db import get_engine, get_session_factory
from services_database.build_scores_imdb import build_scores_imdb
from tables.films import Film
from tables.scores_imdb import ScoreImdb


def ingest_scores_imdb_pipeline():
    """
    Orchestrates the ingestion of IMDb scores into the database.

    This function performs data cleaning, validates foreign key integrity against
    the Film table, and performs deduplicated insertion.

    Returns:
        pd.DataFrame: The cleaned and filtered DataFrame used for the ingestion.
    """
    # 1. Generate the cleaned DataFrame
    df = build_scores_imdb()

    # --- CRITIQUE: SQLAlchemy Conversion ---
    # Convert Pandas-specific types to standard Python objects to ensure
    # compatibility with SQLAlchemy's type handling (especially for nulls).
    df = df.astype(object).where(pd.notnull(df), None)

    # Initialize DB connection components
    engine = get_engine(Config_bdd.DATABASE_URL)
    SessionFactory = get_session_factory(engine)

    # Insertion Process
    with SessionFactory() as session:
        try:
            print(f"Début de l'insertion dans {Config_bdd.DATABASE_URL}...")

            # --- Integrity Check: Filter out orphaned records ---
            # Fetch valid IMDb IDs from the Film table to ensure referential integrity
            valid_imdb = {r[0] for r in session.query(Film.imdb_id).all()}
            orphans = df[~df["tconst"].isin(valid_imdb)]
            if not orphans.empty:
                print(f"⚠️  {len(orphans)} lignes ignorées (tconst absent de films)")

            # Only keep rows that have a corresponding entry in the Film table
            df = df[df["tconst"].isin(valid_imdb)]

            # --- Deduplication: Fetch existing scores ---
            existing_id_score_imdb = {
                g.tconst for g in session.query(ScoreImdb.tconst).all()
            }

            count_added = 0

            # Iterate through rows to perform conditional insertion
            for _, row in df.iterrows():
                tconst = row["tconst"]

                # Add record only if not already present in DB or processed in current batch
                if tconst not in existing_id_score_imdb:
                    new_score_imdb = ScoreImdb(
                        tconst=row["tconst"],
                        title=row["title"],
                        average_rating=row["average_rating"],
                        num_votes=row["num_votes"],
                    )
                    session.add(new_score_imdb)

                    # Track newly added ID to prevent duplicates within the same source
                    existing_id_score_imdb.add(tconst)
                    count_added += 1

            # Commit the transaction to the database
            session.commit()
            print(
                f"✅ Migration terminée : {count_added} nouveaux scores_imdb importées."
            )

        except Exception as e:
            # Revert all changes in this session if any error occurs
            session.rollback()
            print(f"❌ Erreur lors de l'ingestion : {e}")
    return df


if __name__ == "__main__":
    ingest_scores_imdb_pipeline()
