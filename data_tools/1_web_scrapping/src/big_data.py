from config import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def extract_horror_movies_matching_tmdb():
    try:
        # Initialisation de Spark
        spark = SparkSession.builder.appName("IMDbTMDBMatcher").getOrCreate()

        print("🚀 Chargement du fichier de référence (TMDB)...")
        # 1. Charger ton fichier de référence (celui que tu as posté)
        # On ne garde que les lignes où l'ID IMDb est valide (pas de 'NOT_FOUND')
        df_reference = (
            spark.read.options(header="True", inferSchema="True")
            .csv(Config.PATH_TMDB_HORROR_SOURCE) # Remplace par ton chemin
            .filter(col("imdb_id_fetched") != "NOT_FOUND")
            .select(col("imdb_id_fetched").alias("tconst"), "title", "tmdb_id")
        )

        print(f"🎬 {df_reference.count()} IDs IMDb valides trouvés dans la référence.")

        # 2. Charger le fichier IMDb BASICS (les films d'horreur)
        print("🔍 Filtrage des fichiers IMDb Basics...")
        df_basics = (
            spark.read.options(header="True", sep="\t", inferSchema="True")
            .csv(Config.PATH_BASICS)
            .filter((col("titleType") == "movie") & (col("genres").contains("Horror")))
            .select("tconst", "primaryTitle", "genres")
        )

        # 3. Charger le fichier IMDb RATINGS (les notes)
        df_ratings = spark.read.options(
            header="True", sep="\t", inferSchema="True"
        ).csv(Config.PATH_RATINGS)

        # 4. LA JOINTURE CRUCIALE : 
        # On joint la Référence avec Basics, puis avec Ratings
        # Seuls les tconst présents dans les trois fichiers seront conservés
        df_final = (
            df_reference
            .join(df_basics, on="tconst", how="inner")
            .join(df_ratings, on="tconst", how="inner")
        )

        # 5. Exportation
        print(f"💾 Sauvegarde du CSV vers {Config.FINAL_CSV}...")
        
        # Note : toPandas() est OK ici car le volume filtré est faible
        pandas_df = df_final.toPandas()
        pandas_df.to_csv(Config.FINAL_CSV, index=False)

        print(f"✅ Terminé ! {len(pandas_df)} films d'horreur correspondants trouvés.")
        df_final.show(10)

    except Exception as e:
        print(f"❌ Erreur lors de l'extraction : {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    extract_horror_movies_matching_tmdb()