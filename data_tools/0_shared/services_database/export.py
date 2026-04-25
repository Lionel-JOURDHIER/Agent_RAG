import pandas as pd


def export_to_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"Export → {path} ({len(df)} lignes)")


def export_to_parquet(df: pd.DataFrame, path: str) -> None:
    df.to_parquet(path, engine="pyarrow", compression="snappy", index=False)
    print(f"✅ Parquet exporté avec succès — {len(df):,} lignes → {path}")
