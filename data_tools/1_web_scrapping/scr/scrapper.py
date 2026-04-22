import glob
import xml.etree.ElementTree as ET

import pandas as pd


def extraire_films_sitemaps():
    liste_films = []
    # On cherche tous les fichiers .xml dans le dossier (ajustez le chemin si besoin)
    fichiers_xml = glob.glob("rt_sitemaps/movie_*.xml")

    print(f"Début de l'extraction sur {len(fichiers_xml)} fichiers...")

    for fichier in fichiers_xml:
        try:
            tree = ET.parse(fichier)
            root = tree.getroot()
            # Namespace standard des sitemaps
            ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

            # On cherche toutes les balises <loc>
            for loc in root.findall(".//ns:loc", ns):
                url = loc.text

                # FILTRE : On ne prend que les URLs de films (contiennent /m/)
                # Et on exclut les images, les critiques (/critic/) ou les célébrités (/celebrity/)
                if "/m/" in url and "/pictures" not in url and "/reviews" not in url:
                    # Extraction du titre à partir de l'URL
                    # Exemple: https://www.rottentomatoes.com/m/avatar_the_way_of_water -> avatar_the_way_of_water
                    titre_url = url.split("/m/")[-1]

                    liste_films.append({"titre_extrait": titre_url, "url_rotten": url})
        except Exception as e:
            print(f"Erreur sur le fichier {fichier}: {e}")

    # Création du DataFrame
    df_index = pd.DataFrame(liste_films)

    # Suppression des doublons éventuels
    df_index = df_index.drop_duplicates(subset=["url_rotten"])

    # Sauvegarde du fichier de référence
    df_index.to_csv("index_rotten_tomatoes.csv", index=False, encoding="utf-8")
    print(
        f"Extraction terminée : {len(df_index)} films répertoriés dans index_rotten_tomatoes.csv"
    )


if __name__ == "__main__":
    extraire_films_sitemaps()
