from config import Config_bdd
from init_db import init_db

# Définition de l'URL pour SQLite
# Cela créera un fichier nommé 'horror_db.sqlite' dans le dossier courant


if __name__ == "__main__":
    print("Initialisation de la base de données...")

    # On appelle init_db qui exécute Base.metadata.create_all(engine)
    engine = init_db(Config_bdd.DATABASE_URL, echo=True)

    print(f"Base de données générée avec succès : {Config_bdd.DATABASE_URL}")
