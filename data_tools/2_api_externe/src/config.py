import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    """
    Centralized configuration management for the TMDB extraction pipeline.

    This class handles environment variables, API endpoints,
    business logic constants, and file system paths.
    """

    # API Authentication - Hard fail if missing to prevent useless execution
    API_KEY: str = os.environ["TMDB_API_KEY"]  # plante tôt si absente

    # API Endpoints and Localization
    BASE_URL: str = os.getenv("TMDB_BASE_URL", "https://api.themoviedb.org/3")
    IMAGE_BASE_URL: str = os.getenv("TMDB_IMAGE_BASE_URL", "https://image.tmdb.org/t/p")
    LANGUAGE: str = os.getenv("TMDB_LANGUAGE", "en-EN")

    # Network settings
    TIMEOUT: int = 10

    # Business Logic: TMDB ID for the 'Horror' category
    HORROR_GENRE_ID: int = 27

    # Data Schema Definition
    CSV_COLUMNS: list[str] = [
        "tmdb_id",
        "imdb_id_fetched",
        "title",
        "overview",
        "release_date",
        "vote_average",
        "vote_count",
        "popularity",
        "poster_path",
        "genres",
    ]

    # Storage Paths
    TEMP_PATH: str = "data/horror_movies_tmdb.csv"
    RAW_PATH: str = "data/horror_movies_tmdb_raw.csv"
    OUTPUT_PATH: str = "../0_shared/data/horror_movies_tmdb.csv"

    # Pagination safety threshold (TMDB strictly limits to 500 pages per query)
    PAGE_LIMIT: int = 490
