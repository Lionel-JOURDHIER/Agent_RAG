import re

import polars as pl
from config import Config
from unidecode import unidecode


def slugify(title: str) -> str:
    """
    Converts a string into a URL-friendly slug.

    This function removes accents, converts to lowercase, replaces special
    characters/spaces with underscores, and ensures no leading or trailing underscores.

    Args:
        title (str): The raw string to be converted into a slug.

    Returns:
        str: The sanitized, slugified version of the input string.
    """
    # Remove accents and normalize to ASCII characters
    title = unidecode(title)

    # Standardize casing to lowercase
    title = title.lower()

    # Remove apostrophes and backticks to prevent gaps (e.g., L'arbre -> Larbre)
    title = re.sub(r"[''`]", "", title)

    # Replace any sequence of non-alphanumeric characters with a single underscore
    title = re.sub(r"[^a-z0-9]+", "_", title)

    # Strip underscores from both ends
    return title.strip("_")


def merge_tmdb_rotten(
    reference_path=Config.REFERENCE_CSV,
    index_path=Config.INDEX_CSV,
    output_path=Config.INPUT_CSV,
):
    """
    Merges TMDB and Rotten Tomatoes datasets using a two-pass matching strategy.

    The function performs a strict match (slug + year) followed by a fallback
    match (slug only) to maximize the join rate while maintaining data integrity.

    Args:
        reference_path (str): Path to the source TMDB CSV file.
        index_path (str): Path to the Rotten Tomatoes index CSV file.
        output_path (str): Destination path for the merged CSV file.
    """
    # --- Loading datasets ---
    tmdb = pl.read_csv(reference_path)
    rt = pl.read_csv(index_path)

    # --- RT Cleaning: Filter out sub-paths like '/videos' or trailers ---
    rt = rt.filter(~pl.col("titre_extrait").str.contains("/"))

    # --- RT Normalization: Convert extracted titles to slugs ---
    rt = rt.with_columns(
        pl.col("titre_extrait")
        .str.to_lowercase()
        .str.replace_all(r"[^a-z0-9]+", "_")
        .str.strip_chars("_")
        .alias("slug")
    )

    # --- TMDB Normalization: Create base slug and year-appended slug ---
    tmdb = tmdb.with_columns(
        [
            # Apply custom slugify logic to the title
            pl.col("title").map_elements(slugify, return_dtype=pl.String).alias("slug"),
            # Extract year from YYYY-MM-DD format
            pl.col("release_date").str.slice(0, 4).alias("year"),
        ]
    )
    # Create composite key for unambiguous matching (e.g., "scream_1996")
    tmdb = tmdb.with_columns((pl.col("slug") + "_" + pl.col("year")).alias("slug_year"))

    # --- PASS 1: Strict matching on slug_year ---
    merged = tmdb.join(
        rt.select(["slug", "url_rotten"]).rename({"slug": "slug_year"}),
        on="slug_year",
        how="left",
    )

    # --- PASS 2: Fallback matching for rows without a match in Pass 1 ---
    unmatched = merged.filter(pl.col("url_rotten").is_null()).drop("url_rotten")
    fallback = unmatched.join(rt.select(["slug", "url_rotten"]), on="slug", how="left")

    # Combine successful matches from Pass 1 with results from Pass 2
    final = pl.concat(
        [
            merged.filter(pl.col("url_rotten").is_not_null()),
            fallback,
        ]
    )

    # --- Statistics Reporting ---
    matched = final.filter(pl.col("url_rotten").is_not_null())
    unmatched_final = final.filter(pl.col("url_rotten").is_null())

    print(f"Total TMDB     : {len(tmdb)}")
    print(f"Matchés RT     : {len(matched)}  ({100 * len(matched) / len(tmdb):.1f}%)")
    print(f"Sans match RT  : {len(unmatched_final)}")

    # --- Export: Cleanup temporary columns and save ---
    final.drop(["slug", "slug_year", "year"]).write_csv(output_path)
    print("✅  horror_movies_merged.csv exporté")


if __name__ == "__main__":  # pragma: no cover
    # Use global configuration constants for direct script execution
    # This ensures that the paths remain consistent across the entire project
    merge_tmdb_rotten(
        reference_path=Config.REFERENCE_CSV,
        index_path=Config.INDEX_CSV,
        output_path=Config.INPUT_CSV,
    )
    print(f"✅ Fichier exporté vers : {Config.INPUT_CSV}")
