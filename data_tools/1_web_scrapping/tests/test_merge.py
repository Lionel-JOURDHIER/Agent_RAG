import os
import unittest

import polars as pl
from merge import merge_tmdb_rotten, slugify


class TestMerger(unittest.TestCase):
    def test_slugify(self):
        """Vérifie que la transformation en slug est correcte."""
        self.assertEqual(slugify("L'Arbre Enchan-té"), "larbre_enchan_te")
        self.assertEqual(slugify("  Hello World!  "), "hello_world")
        self.assertEqual(slugify("Manger 23 pommes"), "manger_23_pommes")
        self.assertEqual(slugify("Scream 6"), "scream_6")


# 2. Test d'intégration de la fusion (Lignes 28-110)
def test_merge_tmdb_rotten_full_coverage(tmp_path):
    # Création de fichiers temporaires pour le test
    ref_path = tmp_path / "ref.csv"
    idx_path = tmp_path / "idx.csv"
    out_path = tmp_path / "out.csv"

    # Données TMDB : un match strict, un match fallback, un sans match
    tmdb_data = pl.DataFrame(
        {
            "title": ["Scream", "The Thing", "Unknown Movie"],
            "release_date": ["1996-12-20", "1982-06-25", "2023-01-01"],
        }
    )

    # Données RT :
    # - "scream_1996" pour le match strict (Pass 1)
    # - "the_thing" pour le match fallback (Pass 2)
    # - "trash/path" pour tester le filtrage des "/"
    rt_data = pl.DataFrame(
        {
            "titre_extrait": ["scream_1996", "the_thing", "invalid/path"],
            "url_rotten": ["url_strict", "url_fallback", "url_invalid"],
        }
    )

    tmdb_data.write_csv(ref_path)
    rt_data.write_csv(idx_path)

    # Exécution de la fonction (C'est ici qu'on couvre les lignes 54-110)
    merge_tmdb_rotten(
        reference_path=str(ref_path),
        index_path=str(idx_path),
        output_path=str(out_path),
    )

    # Chargement du résultat pour vérification
    result = pl.read_csv(out_path)

    # --- Assertions pour valider la logique ---

    # Vérification Pass 1 (Strict)
    scream = result.filter(pl.col("title") == "Scream")
    assert scream["url_rotten"][0] == "url_strict"

    # Vérification Pass 2 (Fallback)
    thing = result.filter(pl.col("title") == "The Thing")
    assert thing["url_rotten"][0] == "url_fallback"

    # Vérification du filtrage (le chemin avec "/" doit être ignoré)
    assert "url_invalid" not in result["url_rotten"].to_list()

    # Vérification de l'export final
    assert os.path.exists(out_path)
    # Vérifie que les colonnes temporaires sont bien supprimées
    assert "slug_year" not in result.columns
    assert "year" not in result.columns
