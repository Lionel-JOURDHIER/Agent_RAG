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
    # 1. Générer le DataFrame nettoyé
    df = build_films()

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
            for _, row in df.iterrows():
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

            session.commit()
            print(f"✅ Migration terminée : {len(df)} films importés.")
        except Exception as e:
            session.rollback()
            print(f"❌ Erreur lors de l'ingestion : {e}")
            # Optionnel: raise e pour voir la stacktrace complète
        # Pas besoin de session.close() explicite ici, le bloc 'with' s'en occupe


if __name__ == "__main__":
    ingest_films_pipeline()
