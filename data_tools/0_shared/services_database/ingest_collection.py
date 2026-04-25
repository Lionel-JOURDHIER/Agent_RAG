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
    # 1. Générer le DataFrame nettoyé
    df = build_collections()

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
            existing_collection_id = {
                g.tmdb_collection_id
                for g in session.query(Collection.tmdb_collection_id).all()
            }

            count_added = 0
            for _, row in df.iterrows():
                tmdb_collection_id = row["tmdb_collection_id"]

                # 2. On n'ajoute que si le nom n'est pas déjà dans la base
                if tmdb_collection_id not in existing_collection_id:
                    new_genre = Collection(
                        tmdb_collection_id=tmdb_collection_id,
                        collection_name=row["collection_name"],
                    )
                    session.add(new_genre)
                    existing_collection_id.add(
                        tmdb_collection_id
                    )  # On l'ajoute au set pour éviter les doublons dans le CSV lui-même
                    count_added += 1

            session.commit()
            print(
                f"✅ Migration terminée : {count_added} nouvelles collections importées."
            )

        except Exception as e:
            session.rollback()
            print(f"❌ Erreur lors de l'ingestion : {e}")
    return df


if __name__ == "__main__":
    ingest_collection_pipeline()
