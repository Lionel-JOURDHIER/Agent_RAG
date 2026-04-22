import os

import polars as pl
from config import Config

# --- CONFIGURATION ---
OUTPUT_FILE = "datatools/0_shared/data/movies_database.csv"


def extract_movies_table():
    # 1. Vérification du fichier
    if not os.path.exists(Config.DB_PATH):
        print(f"Erreur : '{Config.DB_PATH}' introuvable.")
        return

    # 2. Extraction ciblée
    # On utilise l'URI absolue pour SQLite
    uri = f"sqlite://{os.path.abspath(Config.DB_PATH)}"

    print(f"Extraction de la table '{Config.TABLE_NAME}' en cours...")

    try:
        # Lecture directe de la table spécifique
        df = pl.read_database_uri(query=f"SELECT * FROM {Config.TABLE_NAME}", uri=uri)

        # 3. Sauvegarde en CSV
        df.write_csv(Config.OUTPUT_PATH)
        print(f"✅ Terminé ! {len(df)} lignes exportées dans '{Config.OUTPUT_PATH}'.")

    except Exception as e:
        print(f"❌ Erreur : {e}")
        print(
            "Note : Vérifiez que le nom de la table est bien 'movies' (parfois 'titles' ou 'movie' selon le dataset)."
        )


if __name__ == "__main__":  # pragma: no cover
    extract_movies_table()
