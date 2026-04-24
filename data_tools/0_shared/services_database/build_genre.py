"""
build_collections.py
─────────────────────
Source : kaggle (colonne genres)
Sortie : genres.csv
    id_genre (AUTO_INCREMENT géré par la BDD, absent du CSV)
    genre_name    (VARCHAR 50)
Sortie : filmgenres.csv
    id_film_genre (AUTO_INCREMENT géré par la BDD, absent du CSV)
    tmdb_id (INT FK),
    id_genre (SMALLINT FK)

"""

import re

import pandas as pd
from config import Config


def build_genres() -> pd.DataFrame:
    print("Lecture : %s", Config.INPUT_CSV_TMDB)
    df = pd.read_csv(
        Config.INPUT_CSV_TMDB,
        usecols=["genres", "tmdb_id"],
        low_memory=False,
    )
    # 1. Nettoyage initial : suppression des colonnes vides
    df = df.dropna(subset=["genres"])

    # 2. Extraction robuste des genres
    # On cherche tout ce qui se trouve entre guillemets pour éviter les erreurs de parsing
    # La regex [a-zA-Z\s\-&]+ capture les mots, espaces, tirets et esperluettes (ex: Sci-Fi)
    def clean_genres(text):
        # On extrait les mots à l'intérieur des doubles ou simples guillemets
        found = re.findall(r'"([^"]*)"', text)
        if not found:
            # Repli si le format est différent (ex: [Horror, Drama])
            found = re.findall(r"\'([^\']*)\'", text)
        return found

    df["genre_list"] = df["genres"].apply(clean_genres)

    # 3. On "explose" la liste pour avoir une ligne par genre
    df_final = df.explode("genre_list")

    # 4. Nettoyage final et déduplication
    # On supprime les éventuels genres vides et on strip
    df_final["genre_list"] = df_final["genre_list"].str.strip()
    df_final = df_final[df_final["genre_list"] != ""]

    # On garde les valeurs uniques pour genres.csv
    df_unique = df_final[["genre_list"]].drop_duplicates().reset_index(drop=True)

    # Renommage + limitation à 50 caractères (VARCHAR 50)
    df_unique = df_unique.rename(columns={"genre_list": "genre_name"})
    df_unique["genre_name"] = df_unique["genre_name"].str[:50]

    # Export
    df_unique.to_csv(Config.CSV_GENRES, index=False, encoding="utf-8")

    print(f"Export → {Config.CSV_GENRES} ({len(df_unique)} lignes)")
    return df_unique, df_final


def build_filmgenres(df_unique, df_final):
    genre_mapping = {name: i + 1 for i, name in enumerate(df_unique["genre_name"])}

    # 2. Application du mapping sur le DataFrame "explosé"
    # On part du principe que df_exploded contient la colonne "genre_names" avec les noms bruts
    df_film_genres = df_final.copy()
    df_film_genres["id_genre"] = df_film_genres["genre_list"].map(genre_mapping)

    # 3. On ne garde que les colonnes pour filmgenres.csv
    df_film_genres = df_film_genres[["tmdb_id", "id_genre"]]

    # Export
    df_film_genres.to_csv(Config.CSV_FILMGENRES, index=False)
    print(f"Export → {Config.CSV_FILMGENRES} ({len(df_film_genres)} lignes)")


if __name__ == "__main__":
    df_unique, df_final = build_genres()
    build_filmgenres(df_unique, df_final)
