import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config_bdd
from init_db import get_engine, get_session_factory
from services_database.build_scores_imdb import build_scores_imdb
from tables.scores_imdb import ScoreImdb


def ingest_scores_imdb_pipeline():
    # 1. Générer le DataFrame nettoyé
    df = build_scores_imdb()

    # --- CRITIQUE: Conversion pour SQLAlchemy ---
    # Remplace les types Pandas (pd.NA) par des types Python standard (None)
    # Sinon, SQLAlchemy risque de lever une erreur sur les entiers nullables.
    df = df.astype(object).where(pd.notnull(df), None)

    engine = get_engine(Config_bdd.DATABASE_URL)
    SessionFactory = get_session_factory(engine)

    # 3. Insertion
    with SessionFactory() as session:
        try:
            print(f"Début de l'insertion dans {Config_bdd.DATABASE_URL}...")

            # 1. Récupérer tous les genres déjà présents pour éviter des requêtes SQL en boucle
            existing_id_score_imdb = {
                g.tconst for g in session.query(ScoreImdb.tconst).all()
            }

            count_added = 0
            for _, row in df.iterrows():
                tconst = row["tconst"]

                # 2. On n'ajoute que si le nom n'est pas déjà dans la base
                if tconst not in existing_id_score_imdb:
                    new_score_imdb = ScoreImdb(
                        tconst=row["tconst"],
                        title=row["title"],
                        average_rating=row["average_rating"],
                        num_votes=row["num_votes"],
                    )
                    session.add(new_score_imdb)
                    existing_id_score_imdb.add(
                        tconst
                    )  # On l'ajoute au set pour éviter les doublons dans le CSV lui-même
                    count_added += 1

            session.commit()
            print(
                f"✅ Migration terminée : {count_added} nouveaux scores_imdb importées."
            )

        except Exception as e:
            session.rollback()
            print(f"❌ Erreur lors de l'ingestion : {e}")
    return df


if __name__ == "__main__":
    ingest_scores_imdb_pipeline()
