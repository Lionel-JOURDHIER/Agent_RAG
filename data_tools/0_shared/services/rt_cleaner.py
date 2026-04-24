"""
data_tools/1_web_scrapping/src/rt_cleaner.py
───────────────────────────────
Nettoyage de horror_movies_rt_scores.csv selon les règles suivantes :

  1. Doublons exacts (url_rotten ou title+year)  : suppression
  2. Lignes sans title ET sans year              : suppression
  3. rt_tomatometer zéros                        : conservés (0% valide sur RT)
  4. rt_audience_score zéros                     : conservés (0% valide sur RT)
  5. year float64                                : → Int64 (entier nullable)
  6. id_tertiaire                                : slug(title)_year en première colonne
"""

from pathlib import Path

import pandas as pd
from config import Config
from slug import slugify

# ── id_tertiaire ─────────────────────────────────────────────────────────────


def make_id_tertiaire(title: object, year: object) -> str | None:
    """Retourne slug(title)_year ou None si l'un des deux est manquant."""
    if pd.isna(title) or pd.isna(year):
        return None
    title_str = str(title).strip()
    year_int = int(year)
    if not title_str or year_int <= 0:
        return None
    return f"{slugify(title_str)}_{year_int}"


# ── Pipeline ──────────────────────────────────────────────────────────────────


def fix(input_path: Path, output_path: Path) -> pd.DataFrame:
    print("Lecture : %s", input_path)
    df = pd.read_csv(input_path, low_memory=False)
    print("Shape initiale                  : %d × %d", *df.shape)

    # ── 2. Suppression des lignes sans title ET sans year ────────────────────
    mask_both_null = df["title"].isna() & df["year"].isna()
    n = mask_both_null.sum()
    df = df[~mask_both_null].reset_index(drop=True)
    print("Sans title + year supprimées    : %d ligne(s)", n)

    # ── 1a. Doublons exacts sur url_rotten ───────────────────────────────────
    before = len(df)
    df = df.drop_duplicates(subset=["url_rotten"], keep="first").reset_index(drop=True)
    print("Doublons url_rotten supprimés   : %d ligne(s)", before - len(df))

    # ── 1b. Doublons exacts sur title + year (résidus éventuels) ────────────
    before = len(df)
    df = df.drop_duplicates(subset=["title", "year"], keep="first").reset_index(
        drop=True
    )
    print("Doublons title+year supprimés   : %d ligne(s)", before - len(df))

    # ── 5a. Fallback : extraire l'année depuis l'URL pour les year nulls ─────
    #  ex: .../m/maniac_1980  →  1980
    year_from_url = (
        df["url_rotten"]
        .str.extract(r"_(\d{4})$")[0]  # 4 chiffres en fin d'URL
        .astype("float")  # float pour supporter les NaN
    )
    mask_null_year = df["year"].isna()
    recovered = mask_null_year & year_from_url.notna()
    df.loc[recovered, "year"] = year_from_url[recovered]
    print("Années récupérées depuis URL    : %d ligne(s)", recovered.sum())

    # ── 5b. year : float64 → Int64 ───────────────────────────────────────────
    df["year"] = df["year"].astype("Int64")
    print("year converti en Int64")

    # ── 6. id_tertiaire ──────────────────────────────────────────────────────
    df["id_tertiaire"] = [
        make_id_tertiaire(t, y) for t, y in zip(df["title"], df["year"])
    ]
    n_no_id = df["id_tertiaire"].isna().sum()
    if n_no_id:
        print("Lignes sans id_tertiaire        : %d", n_no_id)

    # ── Réordonnancement des colonnes ─────────────────────────────────────────
    priority = [
        "id_tertiaire",
        "title",
        "year",
        "url_rotten",
        "rt_tomatometer",
        "rt_audience_score",
        "rt_critics_consensus",
    ]
    other = [c for c in df.columns if c not in priority]
    df = df[priority + other]

    # ── Export ────────────────────────────────────────────────────────────────
    df.to_csv(output_path, index=False, encoding="utf-8")
    print("Shape finale                    : %d × %d", *df.shape)
    print("Export → %s", output_path)

    return df


if __name__ == "__main__":
    fix(Config.RAW_CSV_RT, Config.OUTPUT_CSV_RT)
