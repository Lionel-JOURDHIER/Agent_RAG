import os

import pandas as pd
from bs4 import BeautifulSoup
from config import Config


def extraire_films_sitemaps():
    # 1. Collecte des paths avec os.scandir + filtre manuel
    fichiers_xml = []
    index = 0

    while index < 24:
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

    # 2. Ouverture, lecture et parsing BS4
    liste_films = []

    for fichier in fichiers_xml:
        try:
            with open(fichier, "rb") as f:
                soup = BeautifulSoup(f, "xml")

            for loc in soup.find_all("loc"):
                url = loc.text

                if "/m/" in url and "/pictures" not in url and "/reviews" not in url:
                    titre_url = url.split("/m/")[-1]
                    liste_films.append({"titre_extrait": titre_url, "url_rotten": url})

        except Exception as e:
            print(f"Erreur sur le fichier {fichier}: {e}")

    df_index = pd.DataFrame(liste_films).drop_duplicates(subset=["url_rotten"])
    df_index.to_csv(Config.INDEX_CSV, index=False, encoding="utf-8")
    print(
        f"Extraction terminée : {len(df_index)} films répertoriés dans index_rotten_tomatoes.csv"
    )


if __name__ == "__main__":
    extraire_films_sitemaps()
