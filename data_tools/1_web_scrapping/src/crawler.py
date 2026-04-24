import csv
import json
import os
import re
import time

import polars as pl
import requests
from bs4 import BeautifulSoup
from config import Config


def clean_text(text):
    """
    Cleans and normalizes text for CSV compatibility.

    This function removes specific HTML tags (<em>), flattens multi-line strings,
    and collapses multiple whitespace characters into a single space.

    Args:
        text (str): The raw string to clean.

    Returns:
        str: The sanitized and trimmed string.
    """
    # Safety check for None or empty inputs
    if not text:
        return ""

    # Replace closing emphasis tags with a space to prevent word merging
    text = text.replace("</em>", " ")

    # Remove opening emphasis tags
    text = text.replace("<em>", "")

    # Remove carriage returns and replace newlines with spaces to protect CSV structure
    text = text.replace("\n", " ").replace("\r", "")

    # Normalize whitespace: convert multiple spaces/tabs into a single space
    text = re.sub(r"\s+", " ", text)

    # Final trim to remove leading and trailing whitespace
    return text.strip()


def get_info_bs4(session, url):
    """
    Scrapes detailed movie information from a Rotten Tomatoes page.

    Extracts title, release year, critics score, audience score, and
    the critics' consensus by combining JSON parsing and DOM analysis.

    Args:
        session (requests.Session): Persistent HTTP session for requests.
        url (str): The Rotten Tomatoes URL of the movie.

    Returns:
        dict: A dictionary containing extracted movie metadata.
    """
    # Initialize the data row with empty strings for all expected columns
    row = {col: "" for col in Config.RT_COLUMNS}
    row["url_rotten"] = url
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    try:
        # Perform the HTTP request
        response = session.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            # First pass: Standard HTML parsing
            soup = BeautifulSoup(response.text, "html.parser")

            # 1. SCORES: Extracted from a JSON blob in the script tags
            # This is the most robust method against UI changes.
            tag = soup.find("script", {"data-json": "mediaScorecard"})
            if tag:
                data = json.loads(tag.string)
                row["rt_tomatometer"] = data.get("criticsScore", {}).get("score", "")
                row["rt_audience_score"] = data.get("audienceScore", {}).get(
                    "score", ""
                )
            # 2. CONSENSUS: Locate the specific div for critics consensus
            consensus_container = soup.find("div", {"id": "critics-consensus"})
            if consensus_container:
                p_tag = consensus_container.find("p")
                if p_tag:
                    # Use the clean_text helper to sanitize the output
                    row["rt_critics_consensus"] = clean_text(
                        p_tag.get_text(strip=False)
                    )

            # 3. TITLES & METADATA: Using lxml and Regex for custom tags (<rt-text>)
            soup = BeautifulSoup(response.text, "lxml")

            # Extract Title via regex to handle custom 'slot="title"' attributes
            match = re.search(
                r'<rt-text[^>]*slot="title"[^>]*>(.*?)</rt-text>', response.text
            )
            if match:
                row["title"] = clean_text(match.group(1).strip())
                print("✅ Titre extrait via HTML ID")
            else:
                print(" ⚠️ Titre non trouvé")

            # Extract Year: Look for metadata-prop slots and validate year range
            for prop in soup.select('rt-text[slot="metadata-prop"]'):
                match = re.search(r"(\d{4})", prop.get_text(strip=True))
                if match and 1890 <= int(match.group(1)) <= 2030:
                    row["year"] = match.group(1)
                    break

            if not row.get("year"):
                print("  ⚠️ Aucune date valide trouvée.")

        else:
            print(f"  ✗ Erreur {response.status_code}")

    except Exception as e:
        print(f"  ✗ Erreur : {e}")

    return row


if __name__ == "__main__":  # pragma: no cover
    # 1. Load targets: Filter input to keep only valid URLs
    df = pl.read_csv(Config.INPUT_CSV).filter(pl.col("url_rotten").is_not_null())
    all_urls = df["url_rotten"].to_list()

    # 2. Resumption Logic: Check progress to avoid redundant work
    done_urls = set()
    file_exists = os.path.exists(Config.RAW_CSV)

    if file_exists:
        try:
            # Read existing output to identify completed URLs
            done_df = pl.read_csv(Config.RAW_CSV)
            if "url_rotten" in done_df.columns:
                done_urls = set(done_df["url_rotten"].to_list())
                print(f"🔄 Reprise : {len(done_urls)} URLs déjà traitées.")
        except Exception as e:
            print(f"🆕 Fichier de sortie vide ou corrompu, on repart de zéro. {e}")
            file_exists = False

    # 3. Filter out URLs that are already done
    urls_to_process = [u for u in all_urls if u not in done_urls]

    if not urls_to_process:
        print("✅ Tout est déjà à jour !")
        exit()

    # 4. Processing Loop with Session Persistence
    with requests.Session() as session:
        # Open file in append mode to protect existing data
        with open(
            Config.RAW_CSV,
            "a",
            newline="",
            encoding="utf-8",
        ) as f:
            writer = csv.DictWriter(f, fieldnames=Config.RT_COLUMNS)

            # Write header only for new files or empty files
            if not file_exists or os.stat(Config.RAW_CSV).st_size == 0:
                writer.writeheader()

            for i, url in enumerate(urls_to_process, start=1):
                # Inline progress tracking
                print(f"  [{i}/{len(urls_to_process)}] {url}", end="\r")

                # Fetch and parse page content
                result = get_info_bs4(session, url)
                writer.writerow(result)

                # Periodic disk flush for data safety
                if i % 5 == 0:
                    f.flush()

                # Throttling strategy: prevent IP blocking with periodic delays
                if i % 10 == 0:
                    time.sleep(1)

    print(f"\n✨ Terminé ! Données sauvegardées dans {Config.RAW_CSV}")
