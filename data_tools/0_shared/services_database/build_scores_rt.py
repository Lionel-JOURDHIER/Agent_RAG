"""
data_tools/0_shared/services_database/build_rt_score.py
──────────────────────
Source : raw_data/horror_movies_rt_scores.csv (colonnes id_tertiaire + url_rotten + rt_tomatometer + rt_audience_score + rt_critics_consensus)
Sortie : data/scores_rt.csv
    id_score_rt (AUTO_INCREMENT géré par la BDD, absent du CSV)
    id_tertiaire (VARCHAR(200) FK),
    url_rotten (VARCHAR(120) UK),
    rt_tomatometer (SMALLINT),
    rt_audience_score (SMALLINT),
    rt_critics_consensus (VARCHAR(285))

"""

import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from services_database.config import Config
from services_database.export import export_to_parquet


def build_scores_rt() -> pd.DataFrame:
    """
    Extracts, cleans, and normalizes Rotten Tomatoes (RT) movie scores and consensus data.

    Processes raw RT data by stripping strings, truncating text to database-safe lengths,
    and converting score metrics into nullable integers. Rows lacking any score or
    consensus information are discarded.

    Returns:
        pd.DataFrame: A DataFrame containing cleaned RT metrics with columns:
                      ['id_tertiaire', 'url_rotten', 'rt_tomatometer',
                       'rt_audience_score', 'rt_critics_consensus'].
    """
    # 0. Load source data with selective column loading
    print("Lecture : %s", Config.INPUT_CSV_RT)
    df = pd.read_csv(
        Config.INPUT_CSV_RT,
        usecols=[
            "id_tertiaire",
            "url_rotten",
            "rt_tomatometer",
            "rt_audience_score",
            "rt_critics_consensus",
        ],
        low_memory=False,
    )

    # 1. Filtering: Drop row if all three primary RT metrics are missing
    df = df.dropna(
        subset=["rt_tomatometer", "rt_audience_score", "rt_critics_consensus"],
        how="all",
    )

    # 2. String Cleaning: Trim whitespace and truncate to fit DB constraints (e.g., VARCHAR limits)
    df["id_tertiaire"] = df["id_tertiaire"].str.strip().str[:200]
    df["url_rotten"] = df["url_rotten"].str.strip().str[:120]

    # Matching docstring requirement: VARCHAR(285) for the consensus text
    df["rt_critics_consensus"] = df["rt_critics_consensus"].str.strip().str[:285]

    # 3. Type Conversion: Convert scores to nullable Int64 to preserve NaNs without float conversion
    df["rt_tomatometer"] = pd.to_numeric(df["rt_tomatometer"], errors="coerce").clip(
        0, 100
    )
    df["rt_audience_score"] = pd.to_numeric(
        df["rt_audience_score"], errors="coerce"
    ).clip(0, 100)

    # 4. Custom Conversion: Ensure native Python types (int/str or None) instead of NumPy/Pandas types
    def to_int_or_none(x):
        """Helper to convert scalar to native int or None."""
        try:
            if pd.isna(x):
                return None
            return int(x)
        except (TypeError, ValueError):
            return None

    def to_str_or_none(x):
        """Helper to convert scalar to native string or None if empty."""
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        return str(x)

    # Apply conversions across specific columns
    df["rt_tomatometer"] = df["rt_tomatometer"].apply(to_int_or_none)
    df["rt_audience_score"] = df["rt_audience_score"].apply(to_int_or_none)
    df["id_tertiaire"] = df["id_tertiaire"].apply(to_str_or_none)
    df["url_rotten"] = df["url_rotten"].apply(to_str_or_none)
    df["rt_critics_consensus"] = df["rt_critics_consensus"].apply(to_str_or_none)

    # 5. Final Integrity Checks: Remove null identifiers and duplicates
    df = df.dropna(subset=["id_tertiaire"])
    df = df.drop_duplicates(subset=["id_tertiaire"], keep="first")

    # 6. Final selection and column ordering
    df = df[
        [
            "id_tertiaire",
            "url_rotten",
            "rt_tomatometer",
            "rt_audience_score",
            "rt_critics_consensus",
        ]
    ]

    return df


if __name__ == "__main__":
    df_scores_rt = build_scores_rt()
    export_to_parquet(df_scores_rt, Config.PARQUET_SCORES_RT)
