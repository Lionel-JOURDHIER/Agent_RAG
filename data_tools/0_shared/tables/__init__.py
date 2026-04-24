"""
0_shared/__init__.py
Point d'entrée du module.

Usage :
    from 0_shared import Film, Person, Genre, FilmPerson
    from 0_shared import PersonRole, DataSource
    from 0_shared import init_db, get_engine, get_session
"""

from .base import DataSource, PersonRole, TimestampMixin
from .genre import FilmGenreLink, Genre
from .film import Film
from .person import Person
from .film_person import FilmPerson
from .database import get_engine, get_session, init_db

__all__ = [
    # Types
    "DataSource",
    "PersonRole",
    "TimestampMixin",
    # Modèles
    "Genre",
    "FilmGenreLink",
    "Film",
    "Person",
    "FilmPerson",
    # Database helpers
    "get_engine",
    "get_session",
    "init_db",
]