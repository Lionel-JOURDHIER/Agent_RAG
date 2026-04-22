from config import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def extract_horror_movies_only():
    try:
        # Initialisation de Spark
        spark = SparkSession.builder.appName("IMDbHorrorMovieFilter").getOrCreate()

        print("🚀 Chargement et filtrage des fichiers IMDb...")

        # 1. Charger le fichier BASICS avec les filtres
        # On filtre directement à la lecture pour optimiser la mémoire
        df_basics = (
            spark.read.options(header="True", sep="\t", inferSchema="True")
            .csv(Config.PATH_BASICS)
            .select("tconst", "titleType", "primaryTitle", "genres")
            .filter((col("titleType") == "movie") & (col("genres").contains("Horror")))
        )

        print("🎬 Films de type 'movie' avec le genre 'Horror' filtrés.")

        # 2. Charger le fichier RATINGS
        df_ratings = spark.read.options(
            header="True", sep="\t", inferSchema="True"
        ).csv(Config.PATH_RATINGS)

        # 3. Jointure (Inner Join) pour récupérer les notes
        # On ne garde que les tconst présents dans notre liste de films d'horreur
        df_final = df_basics.join(df_ratings, on="tconst", how="inner")

        # 4. Nettoyage final (optionnel : supprimer titleType si plus besoin)
        df_final = df_final.drop("titleType")

        # 5. Exportation en un seul fichier CSV
        print(f"💾 Sauvegarde du CSV vers {Config.FINAL_CSV}...")

        print("Conversion en Pandas pour export unique...")
        pandas_df = df_final.toPandas()
        pandas_df.to_csv(Config.FINAL_CSV, index=False)

        # Statistiques et aperçu
        count = df_final.count()
        print(f"✅ Terminé ! {count} films d'horreur trouvés.")
        df_final.show(10)

        spark.stop()

        print("✅ Extraction terminée avec succès !")

    except Exception as e:
        print(f"❌ Erreur lors de l'extraction : {e}")
    finally:
        spark.stop()


if __name__ == "__main__":  # pragma: no cover
    extract_horror_movies_only()
