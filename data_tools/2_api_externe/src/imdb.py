import os

import pandas as pd
import requests
from config import Config
from movies import build_headers

# Initialize a persistent HTTP session
# This object will handle connection pooling and cookie persistence (if any)
session = requests.Session()


def get_imdb_id_from_tmdb(tmdb_id):
    """
    Retrieves the IMDb external identifier for a given TMDB movie ID.

    Queries the TMDB '/external_ids' endpoint to find cross-referenced IDs.
    Handles potential data type inconsistencies from source data.

    Args:
        tmdb_id (Union[int, float, str]): The source TMDB identifier.

    Returns:
        Optional[str]: The IMDb ID (e.g., 'tt1234567') if found,
                      "NOT_FOUND" if the API responded but no IMDb ID exists,
                      or None if a network or API error occurred.
    """
    # Check for NaN values often encountered in pandas DataFrames
    if pd.isna(tmdb_id):
        return None

    # On s'assure que c'est un entier propre
    try:
        # Normalize input to a clean integer string
        movie_id = int(float(tmdb_id))
        url = f"https://api.themoviedb.org/3/movie/{movie_id}/external_ids"
        header = build_headers()

        # Perform the request using the global persistent session
        response = session.get(url, headers=header, timeout=5)

        # Handle non-200 HTTP status codes
        if response.status_code != 200:
            print(f"⚠️ Erreur API {response.status_code} pour l'ID {movie_id}")
            return None

        data = response.json()
        imdb_id = data.get("imdb_id")

        # Specific check for missing mapping in a successful API response
        if not imdb_id:
            print(
                f"🔍 TMDB ID {movie_id} trouvé, mais IMDb ID est VIDE dans la réponse."
            )
            return "NOT_FOUND"
        # Success case
        print(f"✅ Match ! TMDB:{movie_id} -> IMDb:{imdb_id}")
        return imdb_id

    except Exception as e:
        # Catch network timeouts, connection resets, or parsing errors
        print(f"❌ Erreur réseau : {e}")
    return None


def process_csv():
    """
    Orchestrates the movie data enrichment process with a resume capability.

    Loads a dataset, identifies missing IMDb IDs, fetches them from TMDB API,
    and performs periodic disk saves to prevent data loss.
    """
    # 1. Load Data: Resume from output if it exists, otherwise start from source
    if os.path.exists(Config.OUTPUT_PATH):
        print(f"🔄 Reprise depuis {Config.OUTPUT_PATH}")
        df = pd.read_csv(Config.OUTPUT_PATH)
    else:
        print(f"🆕 Chargement du fichier source {Config.TEMP_PATH}")
        df = pd.read_csv(Config.TEMP_PATH)

    # Ensure the destination column exists and is initialized correctly
    if "imdb_id_fetched" not in df.columns:
        df["imdb_id_fetched"] = None

    # Cast to object type to allow mixed types (strings and None)
    df["imdb_id_fetched"] = df["imdb_id_fetched"].astype(object)

    # Dynamic column identification
    col_id = "tmdb_id" if "tmdb_id" in df.columns else "id"

    # Filter: Only process rows that haven't been successfully fetched yet
    mask_todo = df["imdb_id_fetched"].isna()
    indices_to_process = df.index[mask_todo].tolist()

    if not indices_to_process:
        print("✅ Tout est déjà traité !")
        return

    print(f"🚀 Début du traitement de {len(indices_to_process)} films...")

    count = 0
    success_in_batch = 0  # Compteur pour la centaine en cours
    total_success = 0  # Compteur global

    try:
        for idx in indices_to_process:
            tm_id = df.at[idx, col_id]

            # Network call to resolve the external ID
            imdb_id = get_imdb_id_from_tmdb(tm_id)
            df.at[idx, "imdb_id_fetched"] = imdb_id

            # Success tracking (ignoring nulls and explicit "NOT_FOUND" markers)
            if imdb_id and imdb_id != "NOT_FOUND":
                success_in_batch += 1
                total_success += 1

            count += 1

            # Checkpoint: Save every 100 rows to mitigate data loss risk
            if count % 100 == 0:
                print("-" * 50)
                print(f"💾 SAUVEGARDE INTERMÉDIAIRE : {count} films analysés")
                print(
                    f"📈 Sur les 100 derniers : {success_in_batch} IDs IMDb trouvés ✅"
                )
                print(f"📊 Total succès depuis le début : {total_success}")
                print("-" * 50)

                # Physical disk write
                df.to_csv(Config.OUTPUT_PATH, index=False)

                # Réinitialisation du compteur de batch
                success_in_batch = 0

    except KeyboardInterrupt:
        # Graceful handling of manual termination
        print("\n🛑 Arrêt demandé. Sauvegarde finale...")

    # Final persistent save
    df.to_csv(Config.OUTPUT_PATH, index=False)
    print(f"✨ Terminé ! Total final : {total_success} IDs récupérés.")


if __name__ == "__main__":
    process_csv()
