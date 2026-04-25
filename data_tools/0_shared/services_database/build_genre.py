"""
data_tools/0_shared/services_database/build_genre.py
─────────────────────
Source : raw_data/horror_movies_kaggle.csv (colonne genres)
Sortie : data/genres.csv
    id_genre (AUTO_INCREMENT géré par la BDD, absent du CSV)
    genre_name    (VARCHAR 50)
Sortie : data/filmgenres.csv
    id_film_genre (AUTO_INCREMENT géré par la BDD, absent du CSV)
    tmdb_id (INT FK),
    id_genre (SMALLINT FK)

"""

import re
import sys
from pathlib import Path

import pandas as pd

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from services_database.config import Config
from services_database.export import export_to_csv


def build_genres() -> pd.DataFrame:
    """
    Parses raw genre strings, flattens them, and creates a unique genre reference table.

    Returns:
        tuple: (df_unique, df_final)
            - df_unique: Reference table with unique genre names.
            - df_final: Exploded mapping table with raw names for later linking.
    """
    print("Lecture : %s", Config.INPUT_CSV_TMDB)
    df = pd.read_csv(
        Config.INPUT_CSV_TMDB,
        usecols=["genres", "tmdb_id"],
        low_memory=False,
    )

    # 1. Initial Cleanup
    df = df.dropna(subset=["genres"])

    def clean_genres(text):
        """Extracts genre names between quotes using regex."""
        # Finds double-quoted strings, fallback to single-quoted
        found = re.findall(r'"([^"]*)"', text)
        if not found:
            # Repli si le format est différent (ex: [Horror, Drama])
            found = re.findall(r"\'([^\']*)\'", text)
        return found

    # 2. Extract and Flatten
    df["genre_list"] = df["genres"].apply(clean_genres)
    # Transform list [A, B] into separate rows for A and B
    df_final = df.explode("genre_list")

    # 3. Final Cleaning
    df_final["genre_list"] = df_final["genre_list"].str.strip()
    df_final = df_final[df_final["genre_list"] != ""]

    # 4. Create Reference Table
    df_unique = df_final[["genre_list"]].drop_duplicates().reset_index(drop=True)
    df_unique = df_unique.rename(columns={"genre_list": "genre_name"})
    df_unique["genre_name"] = df_unique["genre_name"].str[:50]

    return df_unique, df_final


def build_filmgenres(df_unique, df_final):
    """
    Creates the junction table (Many-to-Many) between movies and genres.

    Args:
        df_unique (pd.DataFrame): The reference table of genres with unique names.
        df_final (pd.DataFrame): The exploded DataFrame containing movie IDs and genre names.

    Returns:
        pd.DataFrame: A junction table with tmdb_id and numeric id_genre.
    """
    # 1. Create a numeric ID mapping from the reference table
    genre_mapping = {name: i + 1 for i, name in enumerate(df_unique["genre_name"])}

    # 2. Map raw names to numeric IDs in the junction table
    df_film_genres = df_final.copy()
    df_film_genres["id_genre"] = df_film_genres["genre_list"].map(genre_mapping)
    df_film_genres = df_film_genres[["tmdb_id", "id_genre"]]
    return df_film_genres


if __name__ == "__main__":
    df_genres, df_final = build_genres()
    df_film_genres = build_filmgenres(df_genres, df_final)

    export_to_csv(df_genres, Config.CSV_GENRES)
    export_to_csv(df_film_genres, Config.CSV_GENRES)
