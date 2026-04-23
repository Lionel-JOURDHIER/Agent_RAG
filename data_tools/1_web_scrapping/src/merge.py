import re

import polars as pl
from config import Config
from unidecode import unidecode


def slugify(title: str) -> str:
    title = unidecode(title)  # é→e, ü→u, ô→o ...
    title = title.lower()
    title = re.sub(r"[''`]", "", title)
    title = re.sub(r"[^a-z0-9]+", "_", title)
    title = title.strip("_")
    return title


# --- Chargement ---
tmdb = pl.read_csv(Config.REFERENCE_CSV)
rt = pl.read_csv(Config.INDEX_CSV)

# --- Nettoyage RT : supprimer les /videos et autres sous-chemins ---
rt = rt.filter(~pl.col("titre_extrait").str.contains("/"))

# --- Normalisation slug RT (tirets et autres → underscores) ---
rt = rt.with_columns(
    pl.col("titre_extrait")
    .str.to_lowercase()
    .str.replace_all(r"[^a-z0-9]+", "_")
    .str.strip_chars("_")
    .alias("slug")
)

# --- TMDB : slug de base + slug avec année (pour désambiguïsation RT) ---
tmdb = tmdb.with_columns(
    [
        pl.col("title").map_elements(slugify, return_dtype=pl.String).alias("slug"),
        pl.col("release_date").str.slice(0, 4).alias("year"),
    ]
)
tmdb = tmdb.with_columns((pl.col("slug") + "_" + pl.col("year")).alias("slug_year"))

# --- PASS 1 : match précis slug + année ---
merged = tmdb.join(
    rt.select(["slug", "url_rotten"]).rename({"slug": "slug_year"}),
    on="slug_year",
    how="left",
)

# --- PASS 2 : fallback slug seul pour les non-matchés ---
unmatched = merged.filter(pl.col("url_rotten").is_null()).drop("url_rotten")
fallback = unmatched.join(rt.select(["slug", "url_rotten"]), on="slug", how="left")

final = pl.concat(
    [
        merged.filter(pl.col("url_rotten").is_not_null()),
        fallback,
    ]
)

# --- Stats ---
matched = final.filter(pl.col("url_rotten").is_not_null())
unmatched_final = final.filter(pl.col("url_rotten").is_null())

print(f"Total TMDB     : {len(tmdb)}")
print(f"Matchés RT     : {len(matched)}  ({100 * len(matched) / len(tmdb):.1f}%)")
print(f"Sans match RT  : {len(unmatched_final)}")

# --- Export ---
final.drop(["slug", "slug_year", "year"]).write_csv(Config.INPUT_CSV)
print("✅  horror_movies_merged.csv exporté")
