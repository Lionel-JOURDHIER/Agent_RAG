import csv
import json
import time
from datetime import date, timedelta

import requests
from config import Config


def build_headers() -> dict:
    """Creation du Header avec les credentials pour l'accès à l'API TMDB."""
    return {
        "Authorization": f"Bearer {Config.API_KEY}",
        "Content-Type": "application/json",
        "Accept-Language": Config.LANGUAGE,
    }


def fetch_genre_map() -> dict[int, str]:
    """Recupération du mappage des genres par leur ID et nom {id: name}."""
    url = f"{Config.BASE_URL}/genre/movie/list"
    try:
        r = requests.get(url, headers=build_headers(), timeout=Config.TIMEOUT)
        r.raise_for_status()
        return {g["id"]: g["name"] for g in r.json().get("genres", [])}
    except requests.exceptions.RequestException as e:
        print(f"[genre_map] Erreur : {e}")
        return {}


def fetch_page(
    page: int, date_from: str | None = None, date_to: str | None = None
) -> dict | None:
    """Récupère une page. Avec filtre date_from/date_to."""
    url = f"{Config.BASE_URL}/discover/movie"
    params = {
        "with_genres": Config.HORROR_GENRE_ID,
        "sort_by": "primary_release_date.asc",
        "page": page,
    }
    if date_from:
        params["primary_release_date.gte"] = date_from
    if date_to:
        params["primary_release_date.lte"] = date_to
    try:
        r = requests.get(
            url, headers=build_headers(), params=params, timeout=Config.TIMEOUT
        )
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        print(f"  [HTTP {r.status_code}] page {page} : {e}")
    except requests.exceptions.ConnectionError:
        print("  Erreur de connexion.")
    except requests.exceptions.Timeout:
        print(f"  Timeout page {page}.")
    except requests.exceptions.RequestException as e:
        print(f"  Erreur inattendue : {e}")
    return None


def extract_row(movie: dict, genre_map: dict[int, str]) -> dict:
    """Extraite une ligne de données pour un film."""
    genre_list = [genre_map.get(gid, str(gid)) for gid in movie.get("genre_ids", [])]
    return {
        "title": movie.get("title", ""),
        "overview": movie.get("overview", ""),
        "release_date": movie.get("release_date"),
        "vote_average": movie.get("vote_average", ""),
        "popularity": movie.get("popularity", ""),
        "poster_path": (
            f"{Config.IMAGE_BASE_URL}/w500{movie['poster_path']}"
            if movie.get("poster_path")
            else ""
        ),
        "genres": json.dumps(genre_list, ensure_ascii=False),
    }


# ---------------------------------------------------------------------------
# Split récursif : découpe automatiquement si > PAGE_LIMIT
# ---------------------------------------------------------------------------


def split_midpoint(date_from: str, date_to: str) -> str:
    """Retourne la date au milieu de la fenêtre."""
    d1 = date.fromisoformat(date_from)
    d2 = date.fromisoformat(date_to)
    mid = d1 + (d2 - d1) / 2
    return mid.isoformat()


def resolve_chunks(
    date_from: str, date_to: str, depth: int = 0
) -> list[tuple[str, str]]:
    """
    Retourne une liste de (date_from, date_to) dont chacune < PAGE_LIMIT pages.
    Se subdivise récursivement si nécessaire.
    """
    indent = "  " * depth
    data = fetch_page(1, date_from, date_to)
    pages = data.get("total_pages", 0) if data else 0
    time.sleep(0.05)

    if pages <= Config.PAGE_LIMIT:
        print(f"{indent}[{date_from} → {date_to}]  {pages} pages  ✓")
        return [(date_from, date_to)]

    mid = split_midpoint(date_from, date_to)
    print(f"{indent}[{date_from} → {date_to}]  {pages} pages  → split au {mid}")

    left = resolve_chunks(date_from, mid, depth + 1)
    right = resolve_chunks(
        (date.fromisoformat(mid) + timedelta(days=1)).isoformat(), date_to, depth + 1
    )
    return left + right


# ---------------------------------------------------------------------------
# Fetch d'un chunk de films (garanti < PAGE_LIMIT)


def fetch_chunk_to_csv(
    idx: int,
    total: int,
    genre_map: dict[int, str],
    writer: csv.DictWriter,
    date_from: str | None = None,
    date_to: str | None = None,
    undated_only: bool = False,
) -> int:
    """Recupère un chunk de films et le stocke dans un fichier CSV."""
    first = fetch_page(1, date_from, date_to)
    if not first:
        return 0

    total_pages = first.get("total_pages", 1)
    count = 0

    def write_batch(movies):
        nonlocal count
        for m in movies:
            if undated_only and m.get("release_date"):
                continue  # on ne garde que les films sans date
            writer.writerow(extract_row(m, genre_map))
            count += 1

    write_batch(first.get("results", []))

    for page in range(2, total_pages + 1):
        print(f"  Chunk {idx}/{total}  page {page}/{total_pages}…", end="\r")
        data = fetch_page(page, date_from, date_to)
        if data:
            write_batch(data.get("results", []))
        time.sleep(0.05)

    return count


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    DATE_START = "1900-01-01"
    DATE_END = "2026-12-31"

    print("═" * 60)
    print("Phase 1 — Calcul des chunks (split récursif)…")
    print("═" * 60)
    chunks = resolve_chunks(DATE_START, DATE_END)
    print(f"\n→ {len(chunks)} chunks résolus.\n")

    print("═" * 60)
    print("Phase 2 — Récupération des genres…")
    print("═" * 60)
    genre_map = fetch_genre_map()
    print(f"  {len(genre_map)} genres chargés.\n")

    print("═" * 60)
    print("Phase 3 — Téléchargement des films…")
    print("═" * 60)

    grand_total = 0
    with open(Config.OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=Config.CSV_COLUMNS)
        writer.writeheader()

        total_chunks = len(chunks) + 1  # +1 pour le chunk sans date

        for i, (d_from, d_to) in enumerate(chunks, start=1):
            n = fetch_chunk_to_csv(i, total_chunks, genre_map, writer, d_from, d_to)
            grand_total += n
            print(
                f"  Chunk {i}/{total_chunks}  [{d_from} → {d_to}]  {n} films  (cumul : {grand_total})"
            )

    print(f"\n{'═' * 60}")
    print(f"✅  Terminé — {grand_total} films exportés → {Config.OUTPUT_PATH}")
