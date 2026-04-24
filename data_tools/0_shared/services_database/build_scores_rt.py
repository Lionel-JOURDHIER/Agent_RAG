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

import pandas as pd
from config import Config


def build_scores_rt() -> pd.DataFrame:
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

    # --- FILTRAGE ---
    # On supprime la ligne si rt_tomatometer ET rt_audience_score ET rt_critics_consensus sont NaN
    df = df.dropna(subset=["rt_tomatometer", "rt_audience_score", "rt_critics_consensus"], how='all')

    # Nettoyage des chaînes
    df["id_tertiaire"] = df["id_tertiaire"].str.strip().str[:200]
    df["url_rotten"] = df["url_rotten"].str.strip().str[:120]
    
    # Correction : ton docstring dit VARCHAR(285) pour le consensus
    df["rt_critics_consensus"] = df["rt_critics_consensus"].str.strip().str[:285]

    # Typage des scores en Int64 (supporte les NaN/NULL sans transformer en float)
    df["rt_tomatometer"] = pd.to_numeric(df["rt_tomatometer"], errors='coerce').astype("Int64")
    df["rt_audience_score"] = pd.to_numeric(df["rt_audience_score"], errors='coerce').astype("Int64")
    
    df = df[
        [
            "id_tertiaire",
            "url_rotten",
            "rt_tomatometer",
            "rt_audience_score",
            "rt_critics_consensus",
        ]
    ]

    df.to_csv(Config.CSV_SCORES_RT, index=False, encoding="utf-8")
    print("Export → %s (%d lignes)", Config.CSV_SCORES_RT, len(df))
    return df


if __name__ == "__main__":
    build_scores_rt()
