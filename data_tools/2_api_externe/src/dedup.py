import polars as pl
from config import Config

df = pl.read_csv(Config.OUTPUT_PATH)
before = len(df)

df = df.unique(subset=["title", "release_date"])


after = len(df)
df.write_csv(Config.OUTPUT_PATH)

print(
    f"✅  {before} → {after} lignes  ({before - after} doublons supprimés) → {Config.OUTPUT_PATH}"
)
