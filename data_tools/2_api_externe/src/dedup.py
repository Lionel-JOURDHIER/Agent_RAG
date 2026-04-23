import polars as pl
from config import Config


def run_deduplication(path):
    """
    Removes duplicate movie entries from a CSV file using Polars.

    This function reads a CSV file, identifies duplicates based on title
    and release date, and overwrites the file with unique records only.

    Args:
        path (str): The file path to the CSV file to be deduplicated.

    Returns:
        None
    """
    # Load the dataset using Polars' fast CSV multi-threaded reader
    df = pl.read_csv(path)
    before = len(df)

    # Deduplicate based on specific business logic keys
    # Returns a new DataFrame with only unique rows
    df = df.unique(subset=["title", "release_date"])

    after = len(df)

    # Overwrite the original file with the cleaned data
    df.write_csv(path)

    # Final reporting to console
    print(
        f"✅  {before} → {after} lignes  ({before - after} doublons supprimés) → {path}"
    )


if __name__ == "__main__":  # pragma: no cover
    # Execute deduplication on the output path defined in the global config
    run_deduplication(Config.OUTPUT_PATH)
