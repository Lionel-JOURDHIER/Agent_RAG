"""
fix_horror_movies.py
─────────────────────
Nettoyage de horror_movies.csv selon les règles suivantes :

  1.  Unnamed: 0 (index fantôme)          : suppression
  2.  adult (100% False)                  : suppression
  3.  budget <1000 et >0 (unités mixtes)  : → NaN
  4.  budget et revenue zéros             : → NaN
  5.  vote_average et vote_count zéros    : → NaN
      + incohérences croisées             : → NaN sur le champ incohérent
  6.  runtime zéros                       : → NaN  (< 10 min conservés)
  7.  popularity zéros                    : conservés
  8.  status                              : inchangé
  9.  id_tertiaire                        : slug(title)_year en première colonne

"""

from pathlib import Path

import pandas as pd
from config import Config
from creation_id import make_id_tertiaire

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

    # ── 1. Suppression de Unnamed: 0 (index fantôme) ─────────────────────────
    df = df.drop(columns=["Unnamed: 0"])
    print("Colonne 'Unnamed: 0' supprimée")

    # ── 2. Suppression de adult (100% False, zéro information) ───────────────
    df = df.drop(columns=["adult"])
    print("Colonne 'adult' supprimée (100%% False)")

    # ── 3. budget <1000 et >0 → NaN (unités mixtes, probablement en M$) ──────
    mask_sus = (df["budget"] > 0) & (df["budget"] < 1000)
    n = mask_sus.sum()
    df.loc[mask_sus, "budget"] = float("nan")
    print("budget unités suspectes → NaN         : %d valeur(s)", n)

    # ── 4a. budget zéros → NaN ───────────────────────────────────────────────
    n = (df["budget"] == 0).sum()
    df["budget"] = df["budget"].replace(0.0, float("nan"))
    print("budget zéros → NaN                    : %d valeur(s)", n)

    # ── 4b. revenue zéros → NaN ──────────────────────────────────────────────
    n = (df["revenue"] == 0).sum()
    df["revenue"] = df["revenue"].replace(0.0, float("nan"))
    print("revenue zéros → NaN                   : %d valeur(s)", n)

    # ── 5a. vote_average zéros → NaN ─────────────────────────────────────────
    n = (df["vote_average"] == 0).sum()
    df["vote_average"] = df["vote_average"].replace(0.0, float("nan"))
    print("vote_average zéros → NaN              : %d valeur(s)", n)

    # ── 5b. vote_count zéros → NaN ───────────────────────────────────────────
    n = (df["vote_count"] == 0).sum()
    df["vote_count"] = df["vote_count"].replace(0, pd.NA)
    print("vote_count zéros → NaN                : %d valeur(s)", n)

    # ── 5c. Incohérence avg>0 mais count=0 → count = NaN ─────────────────────
    mask = df["vote_average"].notna() & df["vote_count"].isna()
    n = mask.sum()
    df.loc[mask, "vote_count"] = pd.NA
    print("vote_count incohérent → NaN           : %d ligne(s)", n)

    # ── 5d. Incohérence count>0 mais avg=0 → avg = NaN ───────────────────────
    mask = df["vote_count"].notna() & df["vote_average"].isna()
    n = mask.sum()
    df.loc[mask, "vote_average"] = float("nan")
    print("vote_average incohérent → NaN         : %d ligne(s)", n)

    # ── 6. runtime zéros → NaN (courts-métrages < 10 min conservés) ──────────
    n = (df["runtime"] == 0).sum()
    df["runtime"] = df["runtime"].replace(0, pd.NA)
    print("runtime zéros → NaN                   : %d valeur(s)", n)

    # ── 9. id_tertiaire ──────────────────────────────────────────────────────
    release_dt = pd.to_datetime(df["release_date"], errors="coerce")
    year_series = release_dt.dt.year

    df["id_tertiaire"] = [
        make_id_tertiaire(t, y) for t, y in zip(df["title"], year_series)
    ]
    n_no_id = df["id_tertiaire"].isna().sum()
    if n_no_id:
        print("Lignes sans id_tertiaire              : %d", n_no_id)

    # ── Réordonnancement des colonnes ─────────────────────────────────────────
    priority = [
        "id",
        "id_tertiaire",
        "title",
        "original_title",
        "original_language",
        "status",
        "vote_average",
        "vote_count",
        "popularity",
        "budget",
        "revenue",
        "runtime",
        "tagline",
    ]
    other = [c for c in df.columns if c not in priority]
    df = df[priority + other]

    # ── Export ────────────────────────────────────────────────────────────────
    df.to_csv(output_path, index=False, encoding="utf-8")
    print("Shape finale                          : %d × %d", *df.shape)
    print("Export → %s", output_path)

    return df


if __name__ == "__main__":
    df = fix(Config.RAW_CSV_KAGGLE, Config.OUTPUT_CSV_KAGGLE)
