import os
from unittest.mock import patch

import pandas as pd
import pytest
from config import Config
from extraction import extract_horror_movies_only
from pyspark.sql import SparkSession


# --- FIXTURE : Session Spark de Test ---
@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.master("local[1]").appName("pytest-pyspark").getOrCreate()
    )
    yield spark
    spark.stop()


# --- 1. Test du Chemin Nominal (Succès) ---


def test_extract_horror_movies_success(spark_session, tmp_path):
    # Création de mini fichiers CSV IMDb pour le test
    basics_path = tmp_path / "basics.tsv"
    ratings_path = tmp_path / "ratings.tsv"
    output_path = tmp_path / "final.csv"

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
        patch.object(Config, "PATH_BASICS", str(basics_path)),
        patch.object(Config, "PATH_RATINGS", str(ratings_path)),
        patch.object(Config, "FINAL_CSV", str(output_path)),
    ):
        extract_horror_movies_only()

    # Vérifications
    assert os.path.exists(output_path)
    df_result = pd.read_csv(output_path)

    # Seul 'tt1' doit rester (est un movie ET contient Horror ET a une note)
    assert len(df_result) == 1
    assert df_result.iloc[0]["primaryTitle"] == "Scream"
    assert "titleType" not in df_result.columns  # Vérifie le .drop()


# --- 2. Test de l'Exception (Couverture du bloc except) ---


def test_extract_horror_movies_exception():
    """Force une erreur en patchant read.csv pour lever une exception."""
    with patch("pyspark.sql.SparkSession.builder") as mock_builder:
        # On simule une erreur fatale lors de la création de la session ou de la lecture
        mock_builder.getOrCreate.side_effect = Exception("Spark Crash Test")

        # L'appel va entrer dans le bloc except et print l'erreur
        # On vérifie simplement que la fonction gère l'erreur sans planter le process
        extract_horror_movies_only()
