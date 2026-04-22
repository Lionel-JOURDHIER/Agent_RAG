import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    API_KEY: str = os.environ["TMDB_API_KEY"]  # plante tôt si absente
    BASE_URL: str = os.getenv("TMDB_BASE_URL", "https://api.themoviedb.org/3")
    IMAGE_BASE_URL: str = os.getenv("TMDB_IMAGE_BASE_URL", "https://image.tmdb.org/t/p")
    LANGUAGE: str = os.getenv("TMDB_LANGUAGE", "en-EN")
    TIMEOUT: int = 10
    # Configuration du genre pour trier les films d'horreurs.
    HORROR_GENRE_ID: int = 27

    # Création des colonnes de bases
    CSV_COLUMNS: list[str] = [
        "title",
        "overview",
        "release_date",
        "vote_average",
        "popularity",
        "poster_path",
        "genres",
    ]
    OUTPUT_PATH: str = "../0_shared/data/horror_movies_tmdb.csv"

    # Limitation de la page pour éviter le plafond TMDB de 500 pages.
    PAGE_LIMIT: int = 490
