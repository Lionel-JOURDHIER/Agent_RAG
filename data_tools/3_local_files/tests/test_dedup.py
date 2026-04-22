import os

import polars as pl
import pytest
from src.dedup import run_deduplication

# Chemins temporaires pour le test
INPUT_TEST = "tests/input_mock.csv"
OUTPUT_TEST = "tests/output_mock.csv"


@pytest.fixture
def setup_test_files():
    """Crée un CSV avec des doublons et nettoie après le test."""
    data = {
        "title": ["Scream", "Scream", "Halloween", "Scream"],
        "release_date": ["1996-12-20", "1996-12-20", "1978-10-25", "2022-01-14"],
        "other_data": [1, 2, 3, 4],
    }
    # Note : Le 1er et le 2ème sont des doublons identiques (titre + date).
    # Le 4ème a le même titre mais une date différente (remake/suite), il doit rester.

    pl.DataFrame(data).write_csv(INPUT_TEST)

    yield INPUT_TEST, OUTPUT_TEST

    # Nettoyage
    for f in [INPUT_TEST, OUTPUT_TEST]:
        if os.path.exists(f):
            os.remove(f)


def test_run_deduplication_logic(setup_test_files):
    in_path, out_path = setup_test_files

    # Exécution de la fonction
    run_deduplication(in_path, out_path)

    # Vérification
    df_result = pl.read_csv(out_path)

    # On attend 3 lignes :
    # 1. Scream (1996)
    # 2. Halloween (1978)
    # 3. Scream (2022)
    assert len(df_result) == 3

    # Vérifier que le doublon spécifique a disparu
    scream_1996 = df_result.filter(
        (pl.col("title") == "Scream") & (pl.col("release_date") == "1996-12-20")
    )
    assert len(scream_1996) == 1
