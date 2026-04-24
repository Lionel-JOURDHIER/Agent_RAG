"""
0_shared/database.py
Helpers de connexion et d'initialisation de la base de données.

Supporte :
  - Supabase  (PostgreSQL via psycopg2 ou asyncpg)
  - SQLite    (développement local)
"""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from tables.base import Base
from tables.collections import Collection  # noqa: F401
from tables.film_genres import FilmGenre  # noqa: F401
from tables.films import Film  # noqa: F401
from tables.genres import Genre  # noqa: F401

# Correction selon l'image (realisateurs au lieu de person)
from tables.realisateurs import Realisateur  # noqa: F401

# Ajout des fichiers de scores pour qu'ils soient créés
from tables.scores_imdb import ScoreImdb  # noqa: F401
from tables.scores_rt import ScoreRt  # noqa: F401
from tables.scores_tmdb import ScoreTmdb  # noqa: F401


def get_engine(database_url: str, echo: bool = False) -> Engine:
    """
    Retourne un engine SQLAlchemy configuré.

    Exemples d'URL :
        Supabase  → "postgresql+psycopg2://user:password@host:5432/postgres"
        SQLite    → "sqlite:///horror_db.sqlite"
        SQLite    → "sqlite:///:memory:"  (tests)
    """
    return create_engine(database_url, echo=echo, future=True)


def get_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Retourne une factory de sessions liée à l'engine fourni."""
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def init_db(database_url: str, echo: bool = False) -> Engine:
    """
    Crée toutes les tables si elles n'existent pas encore.
    À appeler une seule fois au démarrage du pipeline.

    Returns:
        Engine prêt à l'emploi.
    """
    engine = get_engine(database_url, echo=echo)
    Base.metadata.create_all(engine)
    return engine
