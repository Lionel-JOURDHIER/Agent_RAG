import csv
import json
import time

import polars as pl
import requests
from bs4 import BeautifulSoup

INPUT_CSV = "data_tools/0_shared/data/horror_movies_merged.csv"
OUTPUT_CSV = "data_tools/0_shared/data/horror_movies_rt_scores.csv"

RT_COLUMNS = ["url_rotten", "rt_tomatometer", "rt_audience_score"]


def get_scores_bs4(session, url):
    row = {col: "" for col in RT_COLUMNS}
    row["url_rotten"] = url

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }

    try:
        response = session.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            tag = soup.find("script", {"data-json": "mediaScorecard"})

            if tag:
                data = json.loads(tag.string)
                row["rt_tomatometer"] = data.get("criticsScore", {}).get("score", "")
                row["rt_audience_score"] = data.get("audienceScore", {}).get(
                    "score", ""
                )
        else:
            print(f"  ✗ Erreur {response.status_code} sur {url}")

    except Exception as e:
        print(f"  ✗ Erreur réseau sur {url} : {e}")

    return row


if __name__ == "__main__":
    # Chargement
    df = pl.read_csv(INPUT_CSV).filter(pl.col("url_rotten").is_not_null())
    urls = df["url_rotten"].to_list()

    # Utilisation d'une Session pour réutiliser la connexion TCP (plus rapide)
    with requests.Session() as session:
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=RT_COLUMNS)
            writer.writeheader()

            for i, url in enumerate(urls, start=1):
                print(f"  [{i}/{len(urls)}] {url}", end="\r")

                result = get_scores_bs4(session, url)
                writer.writerow(result)

                # Très important avec BS4 : varier le délai pour ne pas être banni
                # Plus on va vite, plus on risque le blocage IP
                if i % 10 == 0:
                    time.sleep(1)
