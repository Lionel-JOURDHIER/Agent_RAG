"""
models/genres.py
─────────────────
PK : id_genre (SMALLINT, AUTO_INCREMENT)
Note : le CSV genres.csv ne contient pas id_genre — il sera généré
       automatiquement à l'ingestion (range 1-19, cohérent avec film_genres.csv).
"""

from base import Base
from sqlalchemy import Column, SmallInteger, String
from sqlalchemy.orm import relationship


class Genre(Base):
    """
    Lookup table for movie genres.

    Stores unique genre names with optimized storage using SmallInteger.
    Maintains a many-to-many relationship with films through FilmGenre.
    """

    __tablename__ = "genres"

    # Primary key using a small integer to save disk space and memory
    id_genre = Column(SmallInteger, primary_key=True, autoincrement=True)

    # Genre name restricted to 50 characters, must be unique
    genre_name = Column(String(50), nullable=False, unique=True)

    # Inverse relationship to the association table
    # Using string reference "FilmGenre" to avoid import issues
    film_genres = relationship("FilmGenre", back_populates="genre")
