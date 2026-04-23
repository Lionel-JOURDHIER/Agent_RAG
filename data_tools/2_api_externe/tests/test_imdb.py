import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import requests
from imdb import get_imdb_id_from_tmdb, process_csv


class TestTMDBFetcher(unittest.TestCase):
    # --- Tests unitaires pour get_imdb_id_from_tmdb ---

    @patch("imdb.session.get")
    def test_get_imdb_id_success(self, mock_get):
        """Vérifie le succès d'une récupération d'ID IMDb."""
        # Simulation d'une réponse JSON de TMDB
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"imdb_id": "tt1234567"}
        mock_get.return_value = mock_response

        result = get_imdb_id_from_tmdb(550)  # Fight Club ID
        self.assertEqual(result, "tt1234567")

    @patch("imdb.session.get")
    def test_get_imdb_id_not_found(self, mock_get):
        """Vérifie le cas où le film existe mais n'a pas de lien IMDb."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"imdb_id": None}
        mock_get.return_value = mock_response

        result = get_imdb_id_from_tmdb(999999)
        self.assertEqual(result, "NOT_FOUND")

    def test_get_imdb_id_nan(self):
        """Vérifie que les valeurs NaN (Pandas) sont ignorées immédiatement."""
        result = get_imdb_id_from_tmdb(float("nan"))
        self.assertIsNone(result)

    # --- Tests d'intégration pour process_csv ---

    @patch("os.path.exists")
    @patch("pandas.read_csv")
    @patch("imdb.get_imdb_id_from_tmdb")
    @patch("pandas.DataFrame.to_csv")
    def test_process_csv_logic(self, mock_to_csv, mock_fetch, mock_read, mock_exists):
        """Vérifie que la boucle de traitement parcourt bien les lignes non traitées."""

        # 1. On simule un CSV d'entrée avec 2 films, un déjà fait, un à faire
        df_input = pd.DataFrame(
            {
                "tmdb_id": [101, 102],
                "imdb_id_fetched": ["tt00001", None],  # Le 102 est à faire
            }
        )
        mock_read.return_value = df_input
        mock_exists.return_value = False  # Force le chargement de TEMP_PATH

        # 2. On simule le retour de l'API pour le film manquant
        mock_fetch.return_value = "tt00002"

        # 3. Lancement
        process_csv()

        # Vérifications :
        # L'API ne doit avoir été appelée qu'une fois (pour l'ID 102)
        mock_fetch.assert_called_once_with(102)
        # On vérifie que la sauvegarde a bien eu lieu
        self.assertTrue(mock_to_csv.called)

    # --- TEST DES LIGNES 44-45, 60-63 (Erreurs API & Réseau) ---
    @patch("imdb.session.get")
    def test_get_imdb_id_errors(self, mock_get):
        # Lignes 44-45 : Simulation d'une erreur 500 (Server Error)
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response
        self.assertIsNone(get_imdb_id_from_tmdb(123))

        # Lignes 60-63 : Simulation d'une exception réseau (Timeout)
        mock_get.side_effect = requests.exceptions.Timeout("Timeout")
        self.assertIsNone(get_imdb_id_from_tmdb(123))

    @patch("imdb.Config")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    @patch("pandas.DataFrame.to_csv")
    def test_reprise_existante(
        self, mock_to_csv, mock_read_csv, mock_exists, mock_config
    ):
        """Couvre la ligne : 'Reprise depuis Config.OUTPUT_PATH'"""
        mock_config.OUTPUT_PATH = "output.csv"
        mock_exists.return_value = True

        # On simule un DF où tout est déjà rempli
        df_done = pd.DataFrame({"tmdb_id": [1], "imdb_id_fetched": ["tt123"]})
        mock_read_csv.return_value = df_done

        # Exécution : doit entrer dans le 'if exists' et le 'if not indices_to_process'
        process_csv()

        # Vérification
        mock_read_csv.assert_called_with("output.csv")
        # Ne doit pas sauvegarder puisqu'il n'y a rien à faire
        mock_to_csv.assert_not_called()

    @patch("imdb.Config")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    @patch("imdb.get_imdb_id_from_tmdb")
    @patch("pandas.DataFrame.to_csv")
    def test_batch_checkpoint_100(
        self, mock_to_csv, mock_fetch, mock_read_csv, mock_exists, mock_config
    ):
        """Couvre le bloc : 'if count % 100 == 0' (Sauvegarde intermédiaire)"""
        mock_config.OUTPUT_PATH = "output.csv"
        mock_config.TEMP_PATH = "temp.csv"
        mock_exists.return_value = False  # Force le chargement du fichier source

        # Création d'un DataFrame de 101 lignes à traiter
        df_large = pd.DataFrame(
            {"tmdb_id": range(101), "imdb_id_fetched": [None] * 101}
        )
        mock_read_csv.return_value = df_large
        mock_fetch.return_value = "tt999"

        process_csv()

        # Vérifications
        # to_csv doit être appelé 2 fois :
        # 1 fois pour le batch de 100 (ligne 122-134)
        # 1 fois pour la sauvegarde finale après la boucle
        self.assertEqual(mock_to_csv.call_count, 2)

    @patch("imdb.Config")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    @patch("imdb.get_imdb_id_from_tmdb")
    @patch("pandas.DataFrame.to_csv")
    def test_keyboard_interrupt_coverage(
        self, mock_to_csv, mock_fetch, mock_read, mock_exists, mock_config
    ):
        """Couvre le bloc 'except KeyboardInterrupt'."""
        mock_config.OUTPUT_PATH = "out.csv"
        mock_config.TEMP_PATH = "temp.csv"
        mock_exists.return_value = False

        # Un DataFrame avec 2 lignes à traiter
        df_test = pd.DataFrame({"tmdb_id": [1, 2], "imdb_id_fetched": [None, None]})
        mock_read.return_value = df_test

        # On simule un Ctrl+C dès le premier appel à l'API
        mock_fetch.side_effect = KeyboardInterrupt()

        # L'exécution doit attraper l'erreur et passer à la sauvegarde finale
        process_csv()

        # Vérifie que la sauvegarde finale (ligne 137 environ) a bien eu lieu malgré l'arrêt
        self.assertTrue(mock_to_csv.called)

    @patch("imdb.Config")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    @patch("pandas.DataFrame.to_csv")
    def test_column_initialization(
        self, mock_to_csv, mock_read, mock_exists, mock_config
    ):
        """Couvre le bloc 'if imdb_id_fetched not in df.columns'."""
        mock_config.OUTPUT_PATH = "out.csv"
        mock_config.TEMP_PATH = "temp.csv"
        mock_exists.return_value = False

        # On simule un DataFrame source qui n'a PAS encore la colonne de destination
        df_source = pd.DataFrame({"tmdb_id": [123]})
        mock_read.return_value = df_source

        # On simule une fin immédiate en disant qu'il n'y a rien à traiter (via un patch de mask)
        # Ou plus simple : on laisse le script tourner et on vérifie l'initialisation
        with patch("src.imdb.get_imdb_id_from_tmdb", return_value="tt_test"):
            process_csv()

        # On vérifie que la colonne a été créée
        # Le premier argument de la sauvegarde finale (df) doit avoir la colonne
        args, _ = mock_to_csv.call_args
        # Si on accède au dataframe qui a appelé to_csv
        self.assertIn("imdb_id_fetched", df_source.columns)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
