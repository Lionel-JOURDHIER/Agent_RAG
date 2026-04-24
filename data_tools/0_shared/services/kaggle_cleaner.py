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


def fix(input_path: Path, output_path: Path) -> pd.DataFrame:
    """
    Performs extensive data cleaning on movie datasets, including structural
    pruning, sanity checks on financial metrics, and cross-validation of scores.

    Args:
        input_path (Path): Path to the raw CSV file.
        output_path (Path): Path where the cleaned CSV will be exported.

    Returns:
        pd.DataFrame: The fully processed and reordered DataFrame.
    """
    # Load dataset
    print("Lecture : %s", input_path)
    df = pd.read_csv(input_path, low_memory=False)
    print("Shape initiale                        : %d × %d", *df.shape)

    # -- Global String Cleanup --
    # Remove unwanted line breaks and carriage returns in text fields
    str_cols = df.select_dtypes(include="str").columns
    df[str_cols] = df[str_cols].apply(
        lambda col: col.str.replace(r"[\r\n]+", " ", regex=True).str.strip()
    )
    print("Retours à la ligne nettoyés           : %d colonnes", len(str_cols))

    # -- Structural Pruning --
    # Drop ghost index and columns with no informational value
    df = df.drop(columns=["Unnamed: 0"])
    print("Colonne 'Unnamed: 0' supprimée")

    df = df.drop(columns=["adult"])
    print("Colonne 'adult' supprimée (100%% False)")

    # -- Financial & Metric Normalization --
    # Handle suspicious budget units (likely mixed Million/Dollar units)
    mask_sus = (df["budget"] > 0) & (df["budget"] < 1000)
    n = mask_sus.sum()
    df.loc[mask_sus, "budget"] = float("nan")
    print("budget unités suspectes → NaN         : %d valeur(s)", n)

    # Replace hard-coded zeros with actual missing value indicators (NaN)
    # Budget
    n = (df["budget"] == 0).sum()
    df["budget"] = df["budget"].replace(0.0, float("nan"))
    print("budget zéros → NaN                    : %d valeur(s)", n)

    # Revenue
    n = (df["revenue"] == 0).sum()
    df["revenue"] = df["revenue"].replace(0.0, float("nan"))
    print("revenue zéros → NaN                   : %d valeur(s)", n)

    # vote average
    n = (df["vote_average"] == 0).sum()
    df["vote_average"] = df["vote_average"].replace(0.0, float("nan"))
    print("vote_average zéros → NaN              : %d valeur(s)", n)

    # vote count
    n = (df["vote_count"] == 0).sum()
    df["vote_count"] = df["vote_count"].replace(0, pd.NA)
    print("vote_count zéros → NaN                : %d valeur(s)", n)

    # -- Cross-Validation of Vote Metrics --
    # Inconsistency 1: Rating exists but count is missing
    mask = df["vote_average"].notna() & df["vote_count"].isna()
    n = mask.sum()
    df.loc[mask, "vote_count"] = pd.NA
    print("vote_count incohérent → NaN           : %d ligne(s)", n)

    # Inconsistency 2: Count exists but rating is missing
    mask = df["vote_count"].notna() & df["vote_average"].isna()
    n = mask.sum()
    df.loc[mask, "vote_average"] = float("nan")
    print("vote_average incohérent → NaN         : %d ligne(s)", n)

    # Inconsistency 3 : runtime zéros → NaN
    n = (df["runtime"] == 0).sum()
    df["runtime"] = df["runtime"].replace(0, pd.NA)
    print("runtime zéros → NaN                   : %d valeur(s)", n)

    # -- third Key Generation --
    # Extract year and create slug-based tertiary ID
    release_dt = pd.to_datetime(df["release_date"], errors="coerce")
    year_series = release_dt.dt.year

    df["id_tertiaire"] = [
        make_id_tertiaire(t, y) for t, y in zip(df["title"], year_series)
    ]
    n_no_id = df["id_tertiaire"].isna().sum()
    if n_no_id:
        print("Lignes sans id_tertiaire              : %d", n_no_id)

    # -- Logical Column Reordering --
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

    # Export
    df.to_csv(output_path, index=False, encoding="utf-8")
    print("Shape finale                          : %d × %d", *df.shape)
    print("Export → %s", output_path)

    return df


if __name__ == "__main__":
    df = fix(Config.RAW_CSV_KAGGLE, Config.OUTPUT_CSV_KAGGLE)
