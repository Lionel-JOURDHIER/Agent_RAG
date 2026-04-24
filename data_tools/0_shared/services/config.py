class Config:
    """
    Configuration class for managing file paths across the data pipeline.

    This class centralizes the input (raw) and output paths for various
    data sources including Rotten Tomatoes, TMDB, local databases,
    IMDB, and Kaggle datasets.
    """

    # Rotten Tomatoes data paths
    # Source: Web scraping module
    RAW_CSV_RT = "../1_web_scrapping/data/horror_movies_rt_scores_raw.csv"
    OUTPUT_CSV_RT = "raw_data/horror_movies_rt_scores.csv"

    # TMDB API data paths
    # Source: External API module
    RAW_CSV_TMDB = "../2_api_externe/data/horror_movies_tmdb_raw.csv"
    OUTPUT_CSV_TMDB = "raw_data/horror_movies_tmdb.csv"

    # Database export paths
    # Source: Database module
    RAW_CSV_DB = "../4_database/data/horror_movies_database.csv"
    OUTPUT_CSV_DB = "raw_data/horror_movies_db.csv"

    # Big Data / IMDB scores paths
    # Source: Big data processing module
    RAW_CSV_IMDB = "../5_big_data/data/horror_movies_imdb_scores.csv"
    OUTPUT_CSV_IMDB = "raw_data/horror_movies_imdb_scores.csv"

    # Kaggle local files paths
    # Source: Local files module
    RAW_CSV_KAGGLE = "../3_local_files/data/horror_movies.csv"
    OUTPUT_CSV_KAGGLE = "raw_data/horror_movies_kaggle.csv"
