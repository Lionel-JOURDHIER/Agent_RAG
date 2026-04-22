import os

import polars as pl
import pytest
from src.processor import run_simplification

# Chemins temporaires
INPUT_TMP = "tests/input_raw.csv"
OUTPUT_TMP = "tests/output_simple.csv"


@pytest.fixture
def mock_raw_data():
    """Crée un fichier CSV avec plus de colonnes que nécessaire."""
    data = {
        "id": [1],
        "original_title": ["Test Movie"],
        "title": ["Test Movie"],
        "original_language": ["en"],
        "tagline": ["Keep it simple"],
        "popularity": [10.5],
        "vote_count": [100],
        "vote_average": [7.5],
        "budget": [1000000],
        "revenue": [2000000],
        "runtime": [120],
        "status": ["Released"],
        "adult": [False],
        "colonne_inutile": ["A supprimer"],  # Doit disparaître
        "autre_donnee": [42],  # Doit disparaître
    }
    pl.DataFrame(data).write_csv(INPUT_TMP)

    yield INPUT_TMP, OUTPUT_TMP

    # Nettoyage
    for f in [INPUT_TMP, OUTPUT_TMP]:
        if os.path.exists(f):
            os.remove(f)


def test_run_simplification_columns(mock_raw_data):
    in_path, out_path = mock_raw_data

    # Exécution
    run_simplification(in_path, out_path)

    # Lecture du résultat
    df_res = pl.read_csv(out_path)

    # Vérifications
    expected_columns = [
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

    # 1. Vérifie que le nombre de colonnes est exactement 13
    assert len(df_res.columns) == 13

    # 2. Vérifie que toutes les colonnes attendues sont présentes
    assert all(col in df_res.columns for col in expected_columns)

    # 3. Vérifie que les colonnes inutiles ont été supprimées
    assert "colonne_inutile" not in df_res.columns
