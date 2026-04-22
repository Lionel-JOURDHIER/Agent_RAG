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
