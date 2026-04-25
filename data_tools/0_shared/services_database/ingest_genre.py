import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config_bdd
from init_db import get_engine, get_session_factory
from services_database.build_genre import build_filmgenres, build_genres
from tables.film_genres import FilmGenre
from tables.genres import Genre


def ingest_genre_pipeline():
    # 1. Générer le DataFrame nettoyé
    df, df_ = build_genres()

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
            existing_genres = {
                g.genre_name for g in session.query(Genre.genre_name).all()
            }

            count_added = 0
            for _, row in df.iterrows():
                name = row["genre_name"]

                # 2. On n'ajoute que si le nom n'est pas déjà dans la base
                if name not in existing_genres:
                    new_genre = Genre(genre_name=name)
                    session.add(new_genre)
                    existing_genres.add(
                        name
                    )  # On l'ajoute au set pour éviter les doublons dans le CSV lui-même
                    count_added += 1

            session.commit()
            print(f"✅ Migration terminée : {count_added} nouveaux genres importés.")

        except Exception as e:
            session.rollback()
            print(f"❌ Erreur lors de l'ingestion : {e}")
    return df, df_


def ingest_filmgenre_pipeline(df_genre, df_exploded):
    # 1. Générer le DataFrame nettoyé
    df = build_filmgenres(df_genre, df_exploded)

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
            existing_film = {g.tmdb_id for g in session.query(FilmGenre.tmdb_id).all()}

            count_added = 0
            for _, row in df.iterrows():
                tmdb_id = row["tmdb_id"]

                # 2. On n'ajoute que si le nom n'est pas déjà dans la base
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
            session.rollback()
            print(f"❌ Erreur lors de l'ingestion : {e}")


if __name__ == "__main__":
    df_genre, df_exploded = ingest_genre_pipeline()
    ingest_filmgenre_pipeline(df_genre, df_exploded)
