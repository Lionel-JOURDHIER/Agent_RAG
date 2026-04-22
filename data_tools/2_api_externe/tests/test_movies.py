import csv
import io
import re

from src.config import Config
from src.movies import (
    extract_row,
    fetch_chunk_to_csv,
    fetch_genre_map,
    fetch_page,
    resolve_chunks,
    split_midpoint,
)


# 1. Test d'une fonction logique simple (Maths de dates)
def test_split_midpoint():
    d1 = "2024-01-01"
    d2 = "2024-01-31"
    # Le milieu entre le 1er et le 31 est le 16
    assert split_midpoint(d1, d2) == "2024-01-16"


# 2. Test de l'extraction de données (Transformation dict -> dict)
def test_extract_row():
    movie_data = {"title": "Scream", "genre_ids": [27, 53], "poster_path": "/test.jpg"}
    genre_map = {27: "Horror", 53: "Thriller"}

    row = extract_row(movie_data, genre_map)

    assert row["title"] == "Scream"
    assert "Horror" in row["genres"]
    assert "w500/test.jpg" in row["poster_path"]


# 3. Test de l'appel API (avec Mock)
def test_fetch_genre_map_success(requests_mock):
    # On simule la réponse de TMDB
    mock_url = "https://api.themoviedb.org/3/genre/movie/list"  # Adapté selon ta Config.BASE_URL
    requests_mock.get(mock_url, json={"genres": [{"id": 27, "name": "Horror"}]})

    result = fetch_genre_map()

    assert result == {27: "Horror"}


def test_fetch_genre_map_error(requests_mock):
    # On utilise une expression régulière pour matcher l'URL peu importe les paramètres
    # Ou on spécifie l'URL exacte qui fait défaut dans l'erreur
    requests_mock.get(
        re.compile("api.themoviedb.org/3/genre/movie/list"), status_code=500
    )

    result = fetch_genre_map()
    assert result == {}


def test_fetch_page_params(requests_mock):
    # On utilise 'complete_qs=False' pour matcher l'URL même si les paramètres changent
    requests_mock.get(
        re.compile("api.themoviedb.org/3/discover/movie"),
        json={"results": [], "total_pages": 1},
    )

    # On vérifie que les dates sont bien passées dans l'URL
    fetch_page(1, date_from="2000-01-01", date_to="2000-12-31")

    assert requests_mock.called
    last_request = requests_mock.request_history[0]
    assert "primary_release_date.gte=2000-01-01" in last_request.url


import requests


def test_fetch_page_timeout(requests_mock):
    # On simule un timeout sur l'URL de discover
    requests_mock.get(re.compile("discover/movie"), exc=requests.exceptions.Timeout)

    # La fonction doit attraper l'erreur, print le message et retourner None
    result = fetch_page(1)
    assert result is None


def test_fetch_genre_map_http_error(requests_mock):
    # On simule une erreur 404
    requests_mock.get(re.compile("genre/movie/list"), status_code=404)

    result = fetch_genre_map()
    # La fonction retourne {} en cas d'erreur
    assert result == {}


def test_fetch_chunk_undated_only(requests_mock):
    requests_mock.get(
        re.compile("discover/movie"),
        json={
            "total_pages": 1,
            "results": [
                {"title": "Film avec date", "release_date": "2024-01-01"},
                {"title": "Film sans date", "release_date": None},
            ],
        },
    )

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=Config.CSV_COLUMNS)
    writer.writeheader()

    # On active le filtre undated_only
    count = fetch_chunk_to_csv(1, 1, {}, writer, undated_only=True)

    # Seul le film sans date doit être écrit
    assert count == 1
    assert "Film sans date" in output.getvalue()
    assert "Film avec date" not in output.getvalue()


def test_fetch_page_all_errors(requests_mock):
    url = re.compile("discover/movie")

    # Test ConnectionError
    requests_mock.get(url, exc=requests.exceptions.ConnectionError)
    assert fetch_page(1) is None

    # Test HTTPError (ex: 401 Unauthorized)
    requests_mock.get(url, status_code=401)
    assert fetch_page(1) is None

    # Test General RequestException
    requests_mock.get(url, exc=requests.exceptions.RequestException)
    assert fetch_page(1) is None


def test_resolve_chunks_recursive(requests_mock):
    # On simule un split :
    # Appel 1 (global) : 60 pages -> déclenche le split
    # Appel 2 (gauche) : 10 pages -> ok
    # Appel 3 (droite) : 10 pages -> ok
    requests_mock.get(
        re.compile("discover/movie"),
        [
            {"json": {"total_pages": 491}, "status_code": 200},
            {"json": {"total_pages": 10}, "status_code": 200},
            {"json": {"total_pages": 10}, "status_code": 200},
        ],
    )
    # On appelle la fonction, elle va entrer dans les lignes 100-116
    chunks = resolve_chunks("2024-01-01", "2024-01-10")
    assert len(chunks) == 2


def test_fetch_chunk_first_call_fails(requests_mock):
    requests_mock.get(re.compile("discover/movie"), status_code=500)
    # simulation d'un CSV writer en mémoire
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=Config.CSV_COLUMNS)

    count = fetch_chunk_to_csv(1, 1, {}, writer)
    assert count == 0  # On couvre la ligne 135


def test_fetch_chunk_multi_pages(requests_mock):
    # On simule 2 pages de résultats
    requests_mock.get(
        re.compile("discover/movie"),
        [
            {
                "json": {"total_pages": 2, "results": [{"title": "F1"}]},
                "status_code": 200,
            },
            {
                "json": {"total_pages": 2, "results": [{"title": "F2"}]},
                "status_code": 200,
            },
        ],
    )
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=Config.CSV_COLUMNS)
    writer.writeheader()

    fetch_chunk_to_csv(1, 1, {}, writer)  # Va exécuter les lignes 151-155
