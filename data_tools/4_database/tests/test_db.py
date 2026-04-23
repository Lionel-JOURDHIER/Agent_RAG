import os
import sqlite3
from unittest.mock import patch

import polars as pl
import pytest
from config import Config
from db import extract_movies_table

# Chemins temporaires pour les tests
TEST_DB = "tests/test_movies.db"
TEST_OUTPUT = "tests/test_output.csv"


@pytest.fixture
def setup_sqlite():
    """Crée une base SQLite réelle pour le test d'extraction."""
    # 1. Création d'une DB de test
    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE {Config.TABLE_NAME} (id INTEGER, title TEXT)")
    cursor.execute(
        f"INSERT INTO {Config.TABLE_NAME} VALUES (1, 'Alien'), (2, 'The Thing')"
    )
    conn.commit()
    conn.close()

    # On patche la config pour pointer vers nos fichiers de test
    with (
        patch.object(Config, "DB_PATH", TEST_DB),
        patch.object(Config, "OUTPUT_PATH", TEST_OUTPUT),
    ):
        yield TEST_DB, TEST_OUTPUT

    # Nettoyage
    for f in [TEST_DB, TEST_OUTPUT]:
        if os.path.exists(f):
            os.remove(f)


# --- 1. Test du succès (Chemin nominal) ---


def test_extract_movies_table_success(setup_sqlite):
    db_path, out_path = setup_sqlite

    # Exécution
    extract_movies_table()

    # Vérifications
    assert os.path.exists(out_path)
    df = pl.read_csv(out_path)
    assert len(df) == 2
    assert df["title"].to_list() == ["Alien", "The Thing"]


# --- 2. Test du fichier manquant (Lignes 10-12) ---


def test_extract_movies_table_file_not_found():
    with patch.object(Config, "DB_PATH", "chemin/inexistant.db"):
        # 2. L'erreur venait probablement d'ici si tu avais écrit "extract()"
        result = extract_movies_table()
        assert result is None


def test_extract_movies_table_exception(setup_sqlite):
    """Force le passage dans le bloc 'except Exception as e'."""
    # On simule une erreur SQL en demandant une table qui n'existe pas
    # On patche TABLE_NAME avec un nom invalide
    with patch.object(Config, "TABLE_NAME", "table_inexistante_xyz"):
        # L'appel va déclencher une erreur sqlite3 ou polars
        # Ta fonction va l'attraper, print l'erreur et continuer (sans crash)
        extract_movies_table()

    # Vérification : le fichier de sortie ne doit pas avoir été créé
    # (ou il n'a pas été mis à jour)
    assert (
        not os.path.exists(Config.OUTPUT_PATH)
        or os.path.getsize(Config.OUTPUT_PATH) == 0
    )
