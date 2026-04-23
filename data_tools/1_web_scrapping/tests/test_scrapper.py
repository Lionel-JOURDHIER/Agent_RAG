import os
import shutil
import unittest
from unittest.mock import patch

import pandas as pd

# On suppose que ton script s'appelle parser_sitemap.py
from scrapper import extraire_films_sitemaps


class TestExtractionSitemaps(unittest.TestCase):
    def setUp(self):
        """Configuration : Création d'un environnement de test factice."""
        self.test_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../rt_sitemaps")
        )
        os.makedirs(self.test_dir, exist_ok=True)
        self.output_csv = "test_index.csv"

        # XML de test (movie_0.xml)
        self.xml_content = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url><loc>https://www.rottentomatoes.com/m/toy_story</loc></url>
            <url><loc>https://www.rottentomatoes.com/m/iron_man/pictures</loc></url>
            <url><loc>https://www.rottentomatoes.com/m/avengers</loc></url>
            <url><loc>https://www.rottentomatoes.com/browse/movies_at_home</loc></url>
        </urlset>
        """

    def tearDown(self):
        """Nettoyage des fichiers créés pendant le test."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        if os.path.exists(self.output_csv):
            os.remove(self.output_csv)

    @patch("config.Config.INDEX_CSV", "test_index.csv")
    def test_extraction_complete(self):
        """Vérifie l'extraction, le filtrage et la sauvegarde en CSV."""

        # 1. Créer un fichier XML de test
        with open(
            os.path.join(self.test_dir, "movie_0.xml"), "w", encoding="utf-8"
        ) as f:
            f.write(self.xml_content)

        # 2. Lancer la fonction
        extraire_films_sitemaps()

        # 3. Vérifications
        self.assertTrue(os.path.exists(self.output_csv))

        df = pd.read_csv(self.output_csv)

        # On attend 2 films : toy_story et avengers
        # (iron_man/pictures est exclu, browse n'a pas /m/)
        self.assertEqual(len(df), 2)
        self.assertIn("toy_story", df["titre_extrait"].values)
        self.assertIn(
            "https://www.rottentomatoes.com/m/avengers", df["url_rotten"].values
        )

    @patch("config.Config.INDEX_CSV", "test_index.csv")
    def test_handle_empty_or_broken_xml(self):
        """Vérifie que le script ne plante pas si un fichier est corrompu."""

        # Créer un fichier corrompu
        with open(
            os.path.join(self.test_dir, "movie_0.xml"), "w", encoding="utf-8"
        ) as f:
            f.write("CECI N'EST PAS DU XML")

        # Ça ne doit pas lever d'exception grâce au try/except
        try:
            extraire_films_sitemaps()
        except Exception as e:
            self.fail(f"L'extraction a planté sur un fichier corrompu : {e}")

    @patch("config.Config.INDEX_CSV", "test_index.csv")
    def test_deduplication(self):
        """Vérifie que les doublons sont bien supprimés."""

        # XML avec deux fois le même film
        duplicate_content = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url><loc>https://www.rottentomatoes.com/m/duplicate</loc></url>
            <url><loc>https://www.rottentomatoes.com/m/duplicate</loc></url>
        </urlset>
        """
        with open(
            os.path.join(self.test_dir, "movie_0.xml"), "w", encoding="utf-8"
        ) as f:
            f.write(duplicate_content)

        extraire_films_sitemaps()

        df = pd.read_csv(self.output_csv)
        self.assertEqual(len(df), 1)  # Un seul au lieu de deux


if __name__ == "__main__":
    unittest.main()
