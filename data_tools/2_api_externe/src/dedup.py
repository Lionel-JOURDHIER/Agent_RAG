import polars as pl
from config import Config


def run_deduplication(path):
    df = pl.read_csv(path)
    before = len(df)

    df = df.unique(subset=["title", "release_date"])

    after = len(df)
    df.write_csv(path)

    print(
        f"✅  {before} → {after} lignes  ({before - after} doublons supprimés) → {path}"
    )


if __name__ == "__main__":  # pragma: no cover
    run_deduplication(Config.OUTPUT_PATH)
