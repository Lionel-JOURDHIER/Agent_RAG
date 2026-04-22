import polars as pl
from config import Config


def run_simplification(input_path, output_path):
    df = pl.read_csv(input_path)

    df = df[
        "id",
        "original_title",
        "title",
        "original_language",
        "tagline",
        "popularity",
        "vote_count",
        "vote_average",
        "budget",
        "revenue",
        "runtime",
        "status",
        "adult",
    ]

    df.write_csv(output_path)

    print(f"✅  Simplification des colonnes faite → {output_path}")


if __name__ == "__main__":  # pragma: no cover
    run_simplification(Config.INPUT_PATH, Config.OUTPUT_PATH)
