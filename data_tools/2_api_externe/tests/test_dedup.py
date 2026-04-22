import os

import polars as pl
import pytest

# L'import se fait depuis le package src
from src.dedup import run_deduplication

TEST_FILE = "tests/temp_test_movies.csv"


@pytest.fixture
def temp_csv():
    """Crée un fichier CSV de test avec des doublons."""
    data = {
        "title": ["Dracula", "Dracula", "Scream"],
        "release_date": ["1992-11-13", "1992-11-13", "1996-12-20"],
    }
    df = pl.DataFrame(data)
    df.write_csv(TEST_FILE)

    yield TEST_FILE

    if os.path.exists(TEST_FILE):
        os.remove(TEST_FILE)


def test_deduplication_logic(temp_csv):
    # Exécution
    run_deduplication(temp_csv)

    # Vérification
    df_result = pl.read_csv(temp_csv)

    # On attend 2 lignes (Dracula en double a été supprimé)
    assert len(df_result) == 2
    # Vérifie que les noms sont corrects
    assert "Scream" in df_result["title"].to_list()
    assert "Dracula" in df_result["title"].to_list()
