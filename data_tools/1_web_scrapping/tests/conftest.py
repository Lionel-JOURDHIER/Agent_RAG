import os
import sys
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_driver():
    """Driver Selenium mocké pour les tests unitaires."""
    driver = MagicMock()
    driver.find_element.return_value = MagicMock(text="résultat")
    return driver


@pytest.fixture(scope="session")
def real_driver():
    """Driver réel — uniquement pour les tests d'intégration."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    yield driver
    driver.quit()


# Ajoute le dossier 'src' au chemin de recherche des modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
