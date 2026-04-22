import re

import polars as pl


def slugify(title: str) -> str:
    """Convertit un titre naturel en slug RT : lowercase, alphanum + underscores."""
    title = title.lower()
    title = re.sub(r"[''`]", "", title)  # apostrophes
    title = re.sub(r"[^a-z0-9]+", "_", title)  # tout ce qui n'est pas alphanum → _
    title = title.strip("_")
    return title


# --- Chargement ---
tmdb = pl.read_csv("data_tools/0_shared/data/horror_movies_tmdb.csv")
rt = pl.read_csv("data_tools/1_web_scrapping/index_rotten_tomatoes.csv")

# --- Clé de jointure : slug normalisé ---
tmdb = tmdb.with_columns(
    pl.col("title").map_elements(slugify, return_dtype=pl.String).alias("slug")
)

rt = rt.with_columns(
    pl.col("titre_extrait")
    .str.to_lowercase()
    .str.replace_all(r"[^a-z0-9]+", "_")
    .str.strip_chars("_")
    .alias("slug")
)

# --- Jointure ---
merged = tmdb.join(rt.select(["slug", "url_rotten"]), on="slug", how="left")

matched = merged.filter(pl.col("url_rotten").is_not_null())
unmatched = merged.filter(pl.col("url_rotten").is_null())

print(f"Total TMDB     : {len(tmdb)}")
print(f"Matchés RT     : {len(matched)}  ({100 * len(matched) / len(tmdb):.1f}%)")
print(f"Sans match RT  : {len(unmatched)}")

# --- Export ---
merged.drop("slug").write_csv("data_tools/0_shared/data/horror_movies_merged.csv")
print("✅  horror_movies_merged.csv exporté")
