import os

import pandas as pd
from bs4 import BeautifulSoup
from config import Config


def extraire_films_sitemaps():
    """
    Parses local Rotten Tomatoes XML sitemaps to extract movie URLs and slugs.

    The function scans a specific directory for XML files, extracts valid movie
    links using BeautifulSoup, removes duplicates, and saves the result to a CSV.
    """
    # 1. Path collection using a fixed range of indices
    fichiers_xml = []
    index = 0

    # Iterating through expected sitemap filenames (movie_0.xml to movie_23.xml)
    while index < 24:
        # Construct absolute path relative to the current script location
        entry_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), "../rt_sitemaps", f"movie_{index}.xml"
            )
        )
        if entry_path.endswith(".xml"):
            fichiers_xml.append(entry_path)
        index += 1
    print(fichiers_xml)
    print(f"Début de l'extraction sur {len(fichiers_xml)} fichiers...")

    # 2. Opening, reading, and parsing with BS4
    liste_films = []

    for fichier in fichiers_xml:
        try:
            # Read as binary to let BeautifulSoup handle encoding detection
            with open(fichier, "rb") as f:
                soup = BeautifulSoup(f, "xml")

            # Extract every URL inside <loc> tags
            for loc in soup.find_all("loc"):
                url = loc.text

                # Filter: Must contain movie prefix (/m/) and exclude sub-sections
                if "/m/" in url and "/pictures" not in url and "/reviews" not in url:
                    titre_url = url.split("/m/")[-1]
                    liste_films.append({"titre_extrait": titre_url, "url_rotten": url})

        except Exception as e:
            print(f"Erreur sur le fichier {fichier}: {e}")

    # 3. Data processing and Export
    # Drop duplicates to ensure index integrity
    df_index = pd.DataFrame(liste_films).drop_duplicates(subset=["url_rotten"])
    df_index.to_csv(Config.INDEX_CSV, index=False, encoding="utf-8")
    print(
        f"Extraction terminée : {len(df_index)} films répertoriés dans index_rotten_tomatoes.csv"
    )


if __name__ == "__main__":  # pragma: no cover
    extraire_films_sitemaps()
