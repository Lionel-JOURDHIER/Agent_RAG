import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config_bdd
from init_db import get_engine, get_session_factory
from services_database.build_scores_rt import build_scores_rt
from tables.scores_rt import ScoreRt


def ingest_scores_rt_pipeline():
    # 1. Générer le DataFrame nettoyé
    df = build_scores_rt()

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
            existing_id_score_rt = {
                g.id_tertiaire for g in session.query(ScoreRt.id_tertiaire).all()
            }

            count_added = 0
            for _, row in df.iterrows():
                id_tertiaire = row["id_tertiaire"]

                # 2. On n'ajoute que si le nom n'est pas déjà dans la base
                if id_tertiaire not in existing_id_score_rt:
                    new_score_imdb = ScoreRt(
                        id_tertiaire=row["id_tertiaire"],
                        url_rotten=row["url_rotten"],
                        rt_tomatometer=row["rt_tomatometer"],
                        rt_audience_score=row["rt_audience_score"],
                        rt_critics_consensus=row["rt_critics_consensus"],
                    )
                    session.add(new_score_imdb)
                    existing_id_score_rt.add(
                        id_tertiaire
                    )  # On l'ajoute au set pour éviter les doublons dans le CSV lui-même
                    count_added += 1

            session.commit()
            print(
                f"✅ Migration terminée : {count_added} nouveaux scores_rt importées."
            )

        except Exception as e:
            session.rollback()
            print(f"❌ Erreur lors de l'ingestion : {e}")
    return df


if __name__ == "__main__":
    ingest_scores_rt_pipeline()
