from services_database.ingest_collection import ingest_collection_pipeline
from services_database.ingest_films import ingest_films_pipeline
from services_database.ingest_genre import (
    ingest_filmgenre_pipeline,
    ingest_genre_pipeline,
)
from services_database.ingest_realisateur import ingest_realisateurs_pipeline
from services_database.ingest_scores_imdb import ingest_scores_imdb_pipeline
from services_database.ingest_scores_rt import ingest_scores_rt_pipeline
from services_database.ingest_scores_tmdb import ingest_scores_tmdb_pipeline

if __name__ == "__main__":
    # 1. Tables sans dépendances (FK parents)
    ingest_collection_pipeline()
    ingest_realisateurs_pipeline()  # ← doit précéder films !

    # 2. films dépend de collections + realisateurs
    ingest_films_pipeline()

    # 3. tables dépendant de films
    df_genre, df_exploded = ingest_genre_pipeline()
    ingest_filmgenre_pipeline(df_genre, df_exploded)
    ingest_scores_imdb_pipeline()
    ingest_scores_rt_pipeline()
    ingest_scores_tmdb_pipeline()
