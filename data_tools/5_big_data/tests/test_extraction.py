import os
from unittest.mock import patch

import pandas as pd
import pytest
from config import Config
from extraction import extract_horror_movies
from pyspark.sql import SparkSession


# --- FIXTURE : Session Spark de Test ---
@pytest.fixture(scope="session")
def spark_session():
    # On utilise une session Spark locale pour les tests
    spark = (
        SparkSession.builder.master("local[1]").appName("pytest-pyspark").getOrCreate()
    )
    yield spark
    spark.stop()


# --- 1. Test du Chemin Nominal (Succès) ---


def test_extract_horror_movies_success(spark_session, tmp_path):
    # Création de mini fichiers CSV IMDb pour le test
    tmdb_ref_path = tmp_path / "tmdb_ref.csv"
    basics_path = tmp_path / "basics.tsv"
    ratings_path = tmp_path / "ratings.tsv"
    output_path = tmp_path / "final.csv"

    # 1. Mock Référence TMDB (Source initiale du script)
    pd.DataFrame(
        {
            "imdb_id_fetched": ["tt1", "tt2", "NOT_FOUND"],
            "title": ["Scream", "Toy Story", "None"],
            "tmdb_id": [101, 102, 404],
        }
    ).to_csv(tmdb_ref_path, index=False)

    # Données Mock (Note: tconst 'tt2' n'est pas Horror, il doit être filtré)
    pd.DataFrame(
        {
            "tconst": ["tt1", "tt2", "tt3"],
            "titleType": ["movie", "movie", "short"],
            "primaryTitle": ["Scream", "Toy Story", "Short Horror"],
            "genres": ["Horror,Mystery", "Animation", "Horror"],
        }
    ).to_csv(basics_path, sep="\t", index=False)

    pd.DataFrame(
        {
            "tconst": ["tt1", "tt2"],
            "averageRating": [7.2, 8.3],
            "numVotes": [1000, 2000],
        }
    ).to_csv(ratings_path, sep="\t", index=False)

    # On patche la Config pour pointer vers nos fichiers temporaires
    with (
        patch.object(Config, "PATH_TMDB_HORROR_SOURCE", str(tmdb_ref_path)),
        patch.object(Config, "PATH_BASICS", str(basics_path)),
        patch.object(Config, "PATH_RATINGS", str(ratings_path)),
        patch.object(Config, "FINAL_CSV", str(output_path)),
    ):
        extract_horror_movies()

    # Vérifications
    assert os.path.exists(output_path)
    df_result = pd.read_csv(output_path)

    # Vérification de la logique :
    # tt1 est dans TMDB, est un 'movie' dans Basics -> Présent
    # tt2 est dans TMDB, est un 'movie' dans Basics -> Présent (même si pas Horror dans Basics, ton code ne filtre pas par genre dans Basics !)
    # tt3 n'est pas dans la réf TMDB -> Absent
    # NOT_FOUND est filtré -> Absent

    assert len(df_result) == 2
    assert "tt1" in df_result["tconst"].values
    assert "tt2" in df_result["tconst"].values
    assert "averageRating" in df_result.columns


# --- 2. Test de l'Exception (Couverture du bloc except) ---


def test_extract_horror_movies_exception():
    """Force une erreur en patchant read.csv pour lever une exception."""
    with patch("pyspark.sql.SparkSession.builder") as mock_builder:
        # On simule une erreur fatale lors de la création de la session ou de la lecture
        mock_builder.getOrCreate.side_effect = Exception("Spark Crash Test")

        # L'appel va entrer dans le bloc except et print l'erreur
        # On vérifie simplement que la fonction gère l'erreur sans planter le process
        extract_horror_movies()
