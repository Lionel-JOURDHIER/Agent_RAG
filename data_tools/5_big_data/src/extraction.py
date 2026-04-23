import pandas as pd
from config import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def extract_horror_movies() -> None:
    """
    Consolidates TMDB reference data with official IMDb datasets using Spark.

    Performs a high-performance join between multiple large-scale sources,
    filters for horror movies, and exports the final dataset to a flat CSV.
    """
    spark = None
    try:
        # Initialize Spark with Arrow optimization for faster serialization
        spark = SparkSession.builder.appName("IMDbTMDBMatcher").getOrCreate()
        # Suppress verbose logs to focus on custom print statements
        spark.sparkContext.setLogLevel("WARN")

        # ── 1. TMDB Reference Data ──────────────────────────────────────────
        # Load the previously enriched data with robust CSV parsing settings
        print("🚀 Chargement du fichier de référence (TMDB)...")
        df_reference = (
            spark.read.options(
                header="True",
                inferSchema="True",
                multiLine="True",  # Handle line breaks within CSV fields
                escape='"',  # Standard escaping for double quotes
                quote='"',  # Standard quote character
            )
            .csv(Config.PATH_TMDB_HORROR_SOURCE)
            # Exclude records where the IMDb ID resolution failed
            .filter(col("imdb_id_fetched") != "NOT_FOUND")
            .select(col("imdb_id_fetched").alias("tconst"), "title", "tmdb_id")
        )

        print(f"🎬 {df_reference.count()} IDs IMDb valides trouvés dans la référence.")

        # ── 2. IMDb Basics (Titles & Genres) ───────────────────────────────
        # Filter for standard movies and TV movies only
        print("🔍 Filtrage des fichiers IMDb Basics...")
        df_basics = (
            spark.read.options(header="True", sep="\t", inferSchema="True")
            .csv(Config.PATH_BASICS)
            .filter(col("titleType").isin("movie", "tvMovie"))
            .select("tconst", "primaryTitle", "genres")
            .dropDuplicates(["tconst"])  # ← fix
        )

        # ── 3. IMDb Ratings ────────────────────────────────────────────────
        # Retrieve user ratings and vote counts from IMDb Ratings
        df_ratings = (
            spark.read.options(header="True", sep="\t", inferSchema="True")
            .csv(Config.PATH_RATINGS)
            .dropDuplicates(["tconst"])  # ← par sécurité
        )

        # ── 4. Triple Join Logic ──────────────────────────────────────────
        # Merge all data on the common 'tconst' key
        df_final = df_reference.join(df_basics, on="tconst", how="left").join(
            df_ratings, on="tconst", how="left"
        )

        # ── 5. Parquet to CSV Export ──────────────────────────────────────
        # Use Parquet as an intermediate step for disk-efficient saving
        print(f"💾 Sauvegarde du CSV vers {Config.FINAL_CSV}...")
        df_final.write.mode("overwrite").parquet("data_temp.parquet")

        # Load final data into Pandas for a convenient CSV output
        pandas_df = pd.read_parquet("data_temp.parquet")
        pandas_df.to_csv(Config.FINAL_CSV, index=False, encoding="utf-8")

        print(f"✅ Terminé ! {len(pandas_df)} films d'horreur correspondants trouvés.")
        df_final.show(10)

    except Exception as e:
        print(f"❌ Erreur lors de l'extraction : {e}")

    finally:
        # Critical: release Spark resources
        if spark:
            spark.stop()


if __name__ == "__main__":  # pragma: no cover
    extract_horror_movies()
