"""
data_tools/0_shared/services/tmdb_cleaner.py
───────────────────────────
Nettoyage de horror_movies_tmdb_raw.csv selon les règles suivantes :

  1. vote_average  : 0.0 → NaN
  2. Doublons exacts (title + release_date) : garder la ligne avec le moins de NaN
  3. genres        : inchangé
  4. Films futurs  : inchangés
  5. url_rotten    : inchangé
  6. popularity    : 0.0 → NaN
  7. overview      : inchangé
  8. title null    : suppression de la ligne
  9. suppr \n\r    : suppression des retour à la ligne
  +  id_tertiaire  : slug(title)_year ajouté
"""

from pathlib import Path

import pandas as pd
from config import Config
from creation_id import make_id_tertiaire

# ── Pipeline ──────────────────────────────────────────────────────────────────


def fix(input_path: Path, output_path: Path) -> pd.DataFrame:
    print("Lecture : %s", input_path)
    df = pd.read_csv(input_path, low_memory=False)
    print("Shape initiale       : %d × %d", *df.shape)

    # ── 9. Nettoyage des retours à la ligne sur toutes les colonnes string ──────
    str_cols = df.select_dtypes(include="str").columns
    df[str_cols] = df[str_cols].apply(
        lambda col: col.str.replace(r"[\r\n]+", " ", regex=True).str.strip()
    )
    print("Retours à la ligne supprimés sur %d colonnes string", len(str_cols))

    # ── 8. Suppression des lignes sans titre ─────────────────────────────────
    n = df["title"].isna().sum()
    df = df.dropna(subset=["title"]).reset_index(drop=True)
    print("Titres nuls supprimés : %d ligne(s)", n)

    # ── 1. vote_average : 0.0 → NaN ──────────────────────────────────────────
    n = (df["vote_average"] == 0).sum()
    df["vote_average"] = df["vote_average"].replace(0.0, float("nan"))
    print("vote_average zeros→NaN : %d valeurs", n)

    # ── 6. popularity : 0.0 → NaN ────────────────────────────────────────────
    n = (df["popularity"] == 0).sum()
    df["popularity"] = df["popularity"].replace(0.0, float("nan"))
    print("popularity zeros→NaN   : %d valeurs", n)

    # ── 2. Doublons exacts (title + release_date) ─────────────────────────────
    # Garder la ligne avec le moins de NaN par groupe
    before = len(df)
    df["_nan_count"] = df.isna().sum(axis=1)
    df = (
        df.sort_values("_nan_count")  # moins de NaN en premier
        .drop_duplicates(subset=["title", "release_date"], keep="first")
        .drop(columns=["_nan_count"])
        .reset_index(drop=True)
    )
    print("Doublons exacts supprimés : %d ligne(s)", before - len(df))

    # ── id_tertiaire ──────────────────────────────────────────────────────────
    release_dt = pd.to_datetime(df["release_date"], errors="coerce")
    year_series = release_dt.dt.year

    df["id_tertiaire"] = [
        make_id_tertiaire(t, y) for t, y in zip(df["title"], year_series)
    ]
    n_no_id = df["id_tertiaire"].isna().sum()
    if n_no_id:
        print("Lignes sans id_tertiaire : %d", n_no_id)

    # ── Réordonnancement des colonnes ─────────────────────────────────────────
    priority = [
        "tmdb_id",
        "imdb_id_fetched",
        "id_tertiaire",
        "title",
        "release_date",
        "genres",
        "vote_average",
        "popularity",
        "overview",
        "poster_path",
    ]
    other = [c for c in df.columns if c not in priority]
    df = df[priority + other]

    # ── Export ────────────────────────────────────────────────────────────────
    df.to_csv(output_path, index=False, encoding="utf-8")
    print("Shape finale         : %d × %d", *df.shape)
    print("Export → %s", output_path)

    return df


if __name__ == "__main__":
    df = fix(Config.RAW_CSV_TMDB, Config.OUTPUT_CSV_TMDB)
