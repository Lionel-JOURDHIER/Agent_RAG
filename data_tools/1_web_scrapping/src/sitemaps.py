import os

import requests


def download_rt_sitemaps():
    """
    Downloads movie sitemap XML files from Rotten Tomatoes.

    The function iterates through numbered sitemap URLs, simulates a browser
    request via headers to avoid blocking, and saves each file locally
    for offline parsing.
    """
    # Ensure the destination directory exists
    os.makedirs("rt_sitemaps", exist_ok=True)

    # Browser simulation headers to prevent basic anti-scraping blocks
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # Rotten Tomatoes currently hosts approximately 23-24 movie sitemap pages
    index = 0
    while index < 24:
        url = f"https://www.rottentomatoes.com/sitemaps/movie_{index}.xml"
        filename = f"rt_sitemaps/movie_{index}.xml"

        print(f"Tentative de téléchargement : {url}...")
        try:
            # Perform GET request with a 10-second timeout
            response = requests.get(url, headers=headers, timeout=10)

            # Check if the request was successful
            if response.status_code == 200:
                with open(filename, "wb") as f:
                    f.write(response.content)
                print(f"Succès : {filename} sauvegardé.")
                index += 1

            # Gracefully stop if the page does not exist
            elif response.status_code == 404:
                print("Fin des sitemaps détectée (404).")
                break

            # Handle other HTTP errors (e.g., 403 Forbidden, 500 Server Error)
            else:
                print(f"Erreur {response.status_code} sur {url}")
                break
        except Exception as e:
            # Catch network-related errors (DNS, Connection Refused, etc.)
            print(f"Erreur lors du téléchargement : {e}")
            break


if __name__ == "__main__":  # pragma: no cover
    download_rt_sitemaps()
