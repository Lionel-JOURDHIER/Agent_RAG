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
    if not text:
        return ""
    # Remplacer </em> par un espace
    text = text.replace("</em>", " ")
    # Supprimer <em>
    text = text.replace("<em>", "")
    # Supprimer les retours à la ligne qui cassent le format CSV
    text = text.replace("\n", " ").replace("\r", "")
    # Nettoyer les espaces doubles créés par le remplacement de </em>
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def get_info_bs4(session, url):
    """Recupère les informations de la page Rotten Tomatoes pour l'url donnée"""
    row = {col: "" for col in Config.RT_COLUMNS}
    row["url_rotten"] = url
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    try:
        response = session.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            # 1. SCORES (Toujours via le JSON mediaScorecard car c'est le plus simple)
            tag = soup.find("script", {"data-json": "mediaScorecard"})
            if tag:
                data = json.loads(tag.string)
                row["rt_tomatometer"] = data.get("criticsScore", {}).get("score", "")
                row["rt_audience_score"] = data.get("audienceScore", {}).get(
                    "score", ""
                )

            # 2. CONSENSUS (Via le DOM HTML que tu as envoyé)
            # On cherche le div id="critics-consensus" puis le paragraphe à l'intérieur
            consensus_container = soup.find("div", {"id": "critics-consensus"})
            if consensus_container:
                p_tag = consensus_container.find("p")
                if p_tag:
                    row["rt_critics_consensus"] = clean_text(
                        p_tag.get_text(strip=False)
                    )

            # --- 1. Extraction du Titre ---
            # On cible rt-text avec l'attribut slot="title"
            soup = BeautifulSoup(response.text, "lxml")

            # Titre
            match = re.search(
                r'<rt-text[^>]*slot="title"[^>]*>(.*?)</rt-text>', response.text
            )
            if match:
                row["title"] = clean_text(match.group(1).strip())
                print("✅ Titre extrait via HTML ID")
            else:
                print(" ⚠️ Titre non trouvé")

            # Métadonnées (année)
            for prop in soup.select('rt-text[slot="metadata-prop"]'):
                match = re.search(r"(\d{4})", prop.get_text(strip=True))
                if match and 1890 <= int(match.group(1)) <= 2030:
                    row["year"] = match.group(1)
                    break

            # Si aucune date n'a été validée, row["year"] reste à None
            if not row.get("year"):
                print("  ⚠️ Aucune date valide trouvée.")

        else:
            print(f"  ✗ Erreur {response.status_code}")

    except Exception as e:
        print(f"  ✗ Erreur : {e}")

    return row


if __name__ == "__main__":  # pragma: no cover
    # Chargement
    df = pl.read_csv(Config.INPUT_CSV).filter(pl.col("url_rotten").is_not_null())
    all_urls = df["url_rotten"].to_list()

    # 2. Vérifier ce qui a déjà été fait (REPRISE)
    done_urls = set()
    file_exists = os.path.exists(Config.OUTPUT_CSV)

    if file_exists:
        try:
            # On lit les URLs déjà présentes dans le fichier de sortie
            done_df = pl.read_csv(Config.OUTPUT_CSV)
            if "url_rotten" in done_df.columns:
                done_urls = set(done_df["url_rotten"].to_list())
                print(f"🔄 Reprise : {len(done_urls)} URLs déjà traitées.")
        except Exception as e:
            print(f"🆕 Fichier de sortie vide ou corrompu, on repart de zéro. {e}")
            file_exists = False
    # 3. Filtrer les URLs restantes
    urls_to_process = [u for u in all_urls if u not in done_urls]

    if not urls_to_process:
        print("✅ Tout est déjà à jour !")
        exit()

    # Utilisation d'une Session pour réutiliser la connexion TCP (plus rapide)
    with requests.Session() as session:
        with open(
            Config.OUTPUT_CSV,
            "a",
            newline="",
            encoding="utf-8",
        ) as f:
            writer = csv.DictWriter(f, fieldnames=Config.RT_COLUMNS)
            # N'écrire le header QUE si le fichier est nouveau
            if not file_exists or os.stat(Config.OUTPUT_CSV).st_size == 0:
                writer.writeheader()
            # # Test avec 1 url :
            # result = get_scores_bs4(
            #     session, "https://www.rottentomatoes.com/m/you_should_have_left"
            # )
            # writer.writerow(result)

            for i, url in enumerate(urls_to_process, start=1):
                print(f"  [{i}/{len(urls_to_process)}] {url}", end="\r")

                result = get_info_bs4(session, url)
                writer.writerow(result)

                if i % 5 == 0:
                    f.flush()

                # Très important avec BS4 : varier le délai pour ne pas être banni
                # Plus on va vite, plus on risque le blocage IP
                if i % 10 == 0:
                    time.sleep(1)

    print(f"\n✨ Terminé ! Données sauvegardées dans {Config.OUTPUT_CSV}")
