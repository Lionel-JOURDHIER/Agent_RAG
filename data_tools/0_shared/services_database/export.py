import pandas as pd


def export_to_csv(df: pd.DataFrame, path: str):
    # 3. Final Selection and Export
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"Export → {path} ({len(df)} lignes)")
