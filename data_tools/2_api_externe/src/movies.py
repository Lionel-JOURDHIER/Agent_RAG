import csv
import json
import time
from datetime import date, timedelta

import requests
from config import Config

# ---------------------------------------------------------------------------
# Services used to help fetch data from TMDB API.
# ---------------------------------------------------------------------------


def build_headers() -> dict:
    """
    Generate HTTP headers for TMDB API requests.

    Includes Bearer token authentication, content type specification,
    and language preferences based on global configuration.

    Returns:
        dict: A dictionary containing 'Authorization', 'Content-Type',
              and 'Accept-Language' fields.
    """
    return {
        "Authorization": f"Bearer {Config.API_KEY}",
        "Content-Type": "application/json",
        "Accept-Language": Config.LANGUAGE,
    }


def fetch_genre_map() -> dict[int, str]:
    """
    Fetches the mapping of movie genres from the remote API.

    Retrieves a list of genres containing IDs and names, then transforms
    it into a dictionary for efficient lookup.

    Returns:
        dict[int, str]: A dictionary where keys are genre IDs (int)
                        and values are genre names (str).
                        Returns an empty dict if the request fails.

    Raises:
        requests.exceptions.RequestException: Logged internally,
                                             not raised to caller.
    """
    # Construct endpoint URL using configuration constants
    url = f"{Config.BASE_URL}/genre/movie/list"
    try:
        # Perform GET request with injected headers and timeout safety
        r = requests.get(url, headers=build_headers(), timeout=Config.TIMEOUT)

        # Ensure we catch HTTP error status codes (4xx, 5xx)
        r.raise_for_status()

        # Parse JSON and build the map using dictionary comprehension
        data = r.json()
        return {g["id"]: g["name"] for g in data.get("genres", [])}

    except requests.exceptions.RequestException as e:
        # Log failure context for debugging purposes
        print(f"[genre_map] Erreur : {e}")
        return {}


def fetch_page(
    page: int, date_from: str | None = None, date_to: str | None = None
) -> dict | None:
    """
    Fetches a specific page of movie results from the discovery endpoint.

    Filters results by horror genre and orders them by release date.
    Optional date boundaries can be applied.

    Args:
        page (int): The page number to retrieve.
        date_from (str, optional): Start date in YYYY-MM-DD format. Defaults to None.
        date_to (str, optional): End date in YYYY-MM-DD format. Defaults to None.

    Returns:
        Optional[dict]: The JSON response from the API as a dictionary,
                        or None if an error occurs.
    """
    # Base endpoint for movie discovery
    url = f"{Config.BASE_URL}/discover/movie"

    # Default query parameters
    params = {
        "with_genres": Config.HORROR_GENRE_ID,
        "sort_by": "primary_release_date.asc",
        "page": page,
    }

    # Conditional filtering logic
    if date_from:
        params["primary_release_date.gte"] = date_from
    if date_to:
        params["primary_release_date.lte"] = date_to
    try:
        # Executing network request with configured timeout and headers
        r = requests.get(
            url, headers=build_headers(), params=params, timeout=Config.TIMEOUT
        )
        # Raise an exception for 4xx or 5xx status codes
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        # Specific handling for HTTP errors with status code visibility
        print(f"  [HTTP {r.status_code}] page {page} : {e}")
    except requests.exceptions.ConnectionError:
        print("  Erreur de connexion.")
    except requests.exceptions.Timeout:
        print(f"  Timeout page {page}.")
    except requests.exceptions.RequestException as e:
        print(f"  Erreur inattendue : {e}")
    return None


def extract_row(movie: dict, genre_map: dict[int, str]) -> dict:
    """
    Extracts and formats a single movie record from raw API data.

    Maps genre IDs to names, constructs full image URLs, and flattens
    the dictionary structure for consistent data processing or storage.

    Args:
        movie (dict[str, Any]): Raw movie data record from the API.
        genre_map (dict[int, str]): Reference mapping of genre IDs to names.

    Returns:
        dict[str, Any]: A cleaned dictionary containing formatted movie attributes.
    """
    # Map genre IDs to names; fallback to string ID if mapping is missing
    genre_list = [genre_map.get(gid, str(gid)) for gid in movie.get("genre_ids", [])]
    tmdb_id = movie.get("id", "")
    return {
        "tmdb_id": tmdb_id,
        "title": movie.get("title", ""),
        "overview": movie.get("overview", ""),
        "release_date": movie.get("release_date"),
        "vote_average": movie.get("vote_average", ""),
        "vote_count": movie.get("vote_count", ""),
        "popularity": movie.get("popularity", ""),
        # Ternary logic for conditional URL construction
        "poster_path": (
            f"{Config.IMAGE_BASE_URL}/w500{movie['poster_path']}"
            if movie.get("poster_path")
            else ""
        ),
        # Serialize list to JSON string to maintain data integrity in flat formats
        "genres": json.dumps(genre_list, ensure_ascii=False),
    }


# ---------------------------------------------------------------------------
# Recursive Date Segmentation & Pagination Strategy
# ---------------------------------------------------------------------------


def split_midpoint(date_from: str, date_to: str) -> str:
    """
    Calculates the middle date between two ISO format date strings.

    Args:
        date_from (str): The start date in 'YYYY-MM-DD' format.
        date_to (str): The end date in 'YYYY-MM-DD' format.

    Returns:
        str: The midpoint date in ISO format.
    """
    d1 = date.fromisoformat(date_from)
    d2 = date.fromisoformat(date_to)
    # Calculate the duration and find the halfway point
    mid = d1 + (d2 - d1) / 2
    return mid.isoformat()


def resolve_chunks(
    date_from: str, date_to: str, depth: int = 0
) -> list[tuple[str, str]]:
    """
    Recursively subdivides a date range until each chunk contains fewer
    pages than Config.PAGE_LIMIT.

    This ensures that the API's pagination limit (e.g., 500 pages) is never
    exceeded, allowing for full data extraction.

    Args:
        date_from (str): Start of the interval.
        date_to (str): End of the interval.
        depth (int): Current recursion depth for logging indentation.

    Returns:
        List[Tuple[str, str]]: A list of (start, end) date tuples ready for extraction.
    """
    indent = "  " * depth
    # Preliminary check to see the volume of data in this range
    data = fetch_page(1, date_from, date_to)
    pages = data.get("total_pages", 0) if data else 0

    # Throttling to respect API rate limits during the discovery phase
    time.sleep(0.05)

    # 1. Base case: the range is small enough to be processed
    if pages <= Config.PAGE_LIMIT:
        print(f"{indent}[{date_from} → {date_to}]  {pages} pages  ✓")
        return [(date_from, date_to)]

    # 2. Recursive step: split the range and resolve both halves
    mid = split_midpoint(date_from, date_to)
    print(f"{indent}[{date_from} → {date_to}]  {pages} pages  → split au {mid}")

    # 3. Calculate the day after mid to avoid overlapping results
    left = resolve_chunks(date_from, mid, depth + 1)
    right = resolve_chunks(
        (date.fromisoformat(mid) + timedelta(days=1)).isoformat(), date_to, depth + 1
    )
    return left + right


# ---------------------------------------------------------------------------
# Batch Processing & CSV Data Ingestion
# ---------------------------------------------------------------------------


def fetch_chunk_to_csv(
    idx: int,
    total: int,
    genre_map: dict[int, str],
    writer: csv.DictWriter,
    date_from: str | None = None,
    date_to: str | None = None,
    undated_only: bool = False,
) -> int:
    """
    Fetches all movies in a date-bounded chunk and writes them to a CSV file.

    Iterates through all available pages for the given date range, transforms
    each movie record, and saves it using the provided CSV writer.

    Args:
        idx (int): Current chunk index (for logging).
        total (int): Total number of chunks to process (for logging).
        genre_map (Dict[int, str]): Map for resolving genre names.
        writer (csv.DictWriter): CSV writer object connected to the output file.
        date_from (str, optional): Start date filter.
        date_to (str, optional): End date filter.
        undated_only (bool): If True, only saves movies without a release date.

    Returns:
        int: Total number of records successfully written to the CSV.
    """
    # Initialize the first page to determine the total page count
    first = fetch_page(1, date_from, date_to)
    if not first:
        return 0

    total_pages = first.get("total_pages", 1)
    count = 0

    def write_batch(movies):
        """Internal helper to process and write a list of movie records."""
        nonlocal count
        for m in movies:
            # Filter logic: skip if we only want undated movies but date exists
            if undated_only and m.get("release_date"):
                continue

            # Transform raw data and write to disk
            writer.writerow(extract_row(m, genre_map))
            count += 1

    # Process initial results
    write_batch(first.get("results", []))

    # Iterate through remaining pages (from page 2 to N)
    for page in range(2, total_pages + 1):
        # Inline progress update
        print(f"  Chunk {idx}/{total}  page {page}/{total_pages}…", end="\r")
        data = fetch_page(page, date_from, date_to)
        if data:
            write_batch(data.get("results", []))

        # Rate limiting to respect API provider's policy
        time.sleep(0.05)

    return count


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    # Configuration of the target extraction window
    DATE_START = "1900-01-01"
    DATE_END = "2026-12-31"

    print("═" * 60)
    print("Phase 1 — Calcul des chunks (split récursif)…")
    print("═" * 60)

    # Generate the list of date intervals that respect API page limits
    chunks = resolve_chunks(DATE_START, DATE_END)
    print(f"\n→ {len(chunks)} chunks résolus.\n")

    print("═" * 60)
    print("Phase 2 — Récupération des genres…")
    print("═" * 60)

    # Load genre mapping once to avoid redundant API calls during movie extraction
    genre_map = fetch_genre_map()
    print(f"  {len(genre_map)} genres chargés.\n")

    print("═" * 60)
    print("Phase 3 — Téléchargement des films…")
    print("═" * 60)

    grand_total = 0
    # Open file context with UTF-8 encoding for international character support
    with open(Config.TEMP_PATH, "w", newline="", encoding="utf-8") as f:
        # Initialize CSV DictWriter with predefined columns from Config
        writer = csv.DictWriter(f, fieldnames=Config.CSV_COLUMNS)
        writer.writeheader()

        # The total count includes timed chunks + potentially an undated chunk
        total_chunks = len(chunks) + 1

        # Main execution loop through each validated date interval
        for i, (d_from, d_to) in enumerate(chunks, start=1):
            # Fetch data for the current interval and write directly to CSV
            n = fetch_chunk_to_csv(i, total_chunks, genre_map, writer, d_from, d_to)
            grand_total += n

            # Real-time summary of the current extraction state
            print(
                f"  Chunk {i}/{total_chunks}  [{d_from} → {d_to}]  {n} films  (cumul : {grand_total})"
            )

    print(f"\n{'═' * 60}")
    print(f"✅  Terminé — {grand_total} films exportés → {Config.TEMP_PATH}")
