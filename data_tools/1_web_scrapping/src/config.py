class Config:
    INDEX_CSV = "data/index_rotten_tomatoes.csv"
    REFERENCE_CSV = "../0_shared/data/horror_movies_tmdb.csv"
    INPUT_CSV = "data/horror_movies_merged.csv"
    RAW_CSV = "data/horror_movies_rt_scores_raw.csv"
    OUTPUT_CSV = "../0_shared/data/horror_movies_rt_scores.csv"
    TEST_CSV = "data/horror_movies_rt_scores.csv"
    RT_COLUMNS = [
        "url_rotten",
        "rt_tomatometer",
        "rt_audience_score",
        "rt_critics_consensus",
        "year",
        "title",
    ]
