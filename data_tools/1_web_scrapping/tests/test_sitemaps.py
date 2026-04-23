import os
import shutil
import unittest
from unittest.mock import MagicMock, patch

# On importe la fonction depuis votre fichier
from sitemaps import download_rt_sitemaps


class TestDownloadRT(unittest.TestCase):
    def setUp(self):
        """Configuration avant chaque test : on s'assure que le dossier n'existe pas."""
        if os.path.exists("rt_sitemaps"):
            shutil.rmtree("rt_sitemaps")

    def tearDown(self):
        """Nettoyage après chaque test."""
        if os.path.exists("rt_sitemaps"):
            shutil.rmtree("rt_sitemaps")

    @patch("requests.get")
    def test_download_success(self, mock_get):
        """Teste si le script télécharge et sauvegarde correctement (Code 200)"""
        # On simule une réponse HTTP 200 avec un contenu fictif
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"<xml>test data</xml>"
        # Pour éviter une boucle infinie dans le test, on simule 200 puis 404
        mock_get.side_effect = [mock_response, MagicMock(status_code=404)]

        download_rt_sitemaps()

        # Vérifications
        self.assertTrue(os.path.exists("rt_sitemaps/movie_0.xml"))
        with open("rt_sitemaps/movie_0.xml", "rb") as f:
            self.assertEqual(f.read(), b"<xml>test data</xml>")

    @patch("requests.get")
    def test_stop_on_404(self, mock_get):
        """Teste si le script s'arrête immédiatement sur un 404"""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        download_rt_sitemaps()

        # Le dossier doit être créé, mais il ne doit pas y avoir de fichier movie_0.xml
        self.assertTrue(os.path.exists("rt_sitemaps"))
        self.assertFalse(os.path.exists("rt_sitemaps/movie_0.xml"))

    @patch("requests.get")
    def test_stop_on_403_forbidden(self, mock_get):
        """Teste si le script s'arrête en cas d'erreur 403 (blocage anti-bot)"""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_get.return_value = mock_response

        # On vérifie que la fonction s'exécute sans planter
        download_rt_sitemaps()

        # Le fichier ne doit pas exister
        self.assertFalse(os.path.exists("rt_sitemaps/movie_0.xml"))

    @patch("requests.get")
    def test_handle_network_exception(self, mock_get):
        """Teste la capture d'une exception (ex: DNS, Timeout, Connexion refusée)"""
        # On force requests.get à lever une erreur plutôt que de renvoyer une réponse
        mock_get.side_effect = Exception("Connexion impossible")

        # On vérifie que la fonction gère l'erreur via son bloc try/except
        try:
            download_rt_sitemaps()
        except Exception as e:
            self.fail(f"La fonction a levé une exception non gérée : {e}")

        # Le dossier doit être créé par os.makedirs, mais vide
        self.assertTrue(os.path.exists("rt_sitemaps"))
        self.assertFalse(os.path.exists("rt_sitemaps/movie_0.xml"))


if __name__ == "__main__":
    unittest.main()
