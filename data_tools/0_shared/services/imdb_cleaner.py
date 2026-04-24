"""
data_tools/0_shared/services/imdb_cleaner.py
─────────────────────────────────
Nettoyage de horror_movies_imdb_scores.csv selon les règles suivantes :

  1. title null (1 ligne)          : suppression
  2. id_tertiaire                  : non généré (pas de colonne année)
  3. numVotes                      : inchangé (float64 conservé)
  4. genres format virgule         : normalisé → "Horror, Drama" (virgule + espace)
  5. primaryTitle null             : primaryTitle = title

"""

from pathlib import Path

import pandas as pd
from config import Config

DEFAULT_INPUT = Path("data/horror_movies_imdb_scores.csv")
DEFAULT_OUTPUT = Path("data/horror_movies_imdb_scores_clean.csv")

# ── Normalisation des genres ──────────────────────────────────────────────────


def normalize_genres(raw: object) -> str | None:
    """
    Normalise le format genres IMDb vers "Horror, Drama" (virgule + espace).

    Entrées acceptées :
        "Drama,Horror"        →  "Drama, Horror"
        "Horror"              →  "Horror"
        NaN / None            →  None
    """
    if pd.isna(raw):
        return None
    parts = [g.strip() for g in str(raw).split(",") if g.strip()]
    return ", ".join(parts)


# ── Pipeline ──────────────────────────────────────────────────────────────────


def fix(input_path: Path, output_path: Path) -> pd.DataFrame:
    print("Lecture : %s", input_path)
    df = pd.read_csv(input_path, low_memory=False)
    print("Shape initiale                        : %d × %d", *df.shape)

    # ── Nettoyage des retours à la ligne sur toutes les colonnes string ───────
    str_cols = df.select_dtypes(include="str").columns
    df[str_cols] = df[str_cols].apply(
        lambda col: col.str.replace(r"[\r\n]+", " ", regex=True).str.strip()
    )
    print("Retours à la ligne nettoyés           : %d colonnes", len(str_cols))

    # ── 1. Suppression des lignes sans titre ──────────────────────────────────
    n = df["title"].isna().sum()
    df = df.dropna(subset=["title"]).reset_index(drop=True)
    print("Titres nuls supprimés                 : %d ligne(s)", n)

    # ── 5. primaryTitle null → primaryTitle = title ───────────────────────────
    mask = df["primaryTitle"].isna()
    n = mask.sum()
    df.loc[mask, "primaryTitle"] = df.loc[mask, "title"]
    print("primaryTitle rempli depuis title      : %d valeur(s)", n)

    # ── 4. genres : normalisation "Horror,Drama" → "Horror, Drama" ───────────
    df["genres"] = df["genres"].apply(normalize_genres)
    print("genres normalisés (virgule + espace)")

    # ── Réordonnancement des colonnes ─────────────────────────────────────────
    priority = [
        "tconst",
        "tmdb_id",
        "title",
        "primaryTitle",
        "genres",
        "averageRating",
        "numVotes",
    ]
    other = [c for c in df.columns if c not in priority]
    df = df[priority + other]

    # ── Export ────────────────────────────────────────────────────────────────
    df.to_csv(output_path, index=False, encoding="utf-8")
    print("Shape finale                          : %d × %d", *df.shape)
    print("Export → %s", output_path)

    return df


if __name__ == "__main__":
    df = fix(Config.RAW_CSV_IMDB, Config.OUTPUT_CSV_IMDB)
