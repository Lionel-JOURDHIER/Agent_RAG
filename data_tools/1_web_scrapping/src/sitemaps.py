import os

import requests


def download_rt_sitemaps():
    """récupère les fichiers xml pour les films du site rotten tomatoes."""
    # Créer un dossier pour stocker les sitemaps
    os.makedirs("rt_sitemaps", exist_ok=True)

    # Header pour éviter d'être bloqué (on simule un vrai navigateur)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # IL y a aujourd'hhui 23 pages de xml pour le sitemaps.
    index = 0
    while index < 24:
        url = f"https://www.rottentomatoes.com/sitemaps/movie_{index}.xml"
        filename = f"rt_sitemaps/movie_{index}.xml"

        print(f"Tentative de téléchargement : {url}...")
        try:
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                with open(filename, "wb") as f:
                    f.write(response.content)
                print(f"Succès : {filename} sauvegardé.")
                index += 1
            elif response.status_code == 404:
                print("Fin des sitemaps détectée (404).")
                break
            else:
                print(f"Erreur {response.status_code} sur {url}")
                break
        except Exception as e:
            print(f"Erreur lors du téléchargement : {e}")
            break


if __name__ == "__main__":
    download_rt_sitemaps()
