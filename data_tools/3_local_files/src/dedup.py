import polars as pl
from config import Config


def run_deduplication(input_path, output_path):
    df = pl.read_csv(input_path)
    before = len(df)

    df = df.unique(subset=["title", "release_date"])

    after = len(df)
    df.write_csv(output_path)

    print(
        f"✅  {before} → {after} lignes  ({before - after} doublons supprimés) → {output_path}"
    )


if __name__ == "__main__":  # pragma: no cover
    run_deduplication(Config.INPUT_PATH, Config.OUTPUT_PATH)
