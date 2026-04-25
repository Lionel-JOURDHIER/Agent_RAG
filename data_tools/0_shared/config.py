"""
Fichier de configuration de la base de donnée et du  projet
"""

import os

from dotenv import load_dotenv

load_dotenv()


# VERSION SUPABASE
class Config_bdd:
    DATABASE_URL = (
        f"postgresql+psycopg2://{os.getenv('SUPABASE_USER')}:{os.getenv('SUPABASE_PASSWORD')}"
        f"@{os.getenv('SUPABASE_HOST')}:{os.getenv('SUPABASE_PORT')}/{os.getenv('SUPABASE_DB')}"
        f"?sslmode=require"  # ← sslmode reste dans l'URL, c'est un param psycopg2
    )


## Version POSTGRES / DOCKER
# class Config_bdd:
#     DATABASE_URL = (
#         f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
#         f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
#     )


## Version de test SQLITE
# class Config_bdd:
#     DATABASE_URL = "sqlite:///sqlite/horror_db.sqlite"
