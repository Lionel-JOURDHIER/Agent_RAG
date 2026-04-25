# Migration HorRAGor → Supabase

## 1. Créer le projet Supabase

1. Aller sur https://supabase.com → **New project**
2. Remplir :
   - **Name** : `horragor`
   - **Database Password** : choisir un mot de passe fort (le noter !)
   - **Region** : `West EU (Paris)`
3. Attendre ~2 min que le projet soit prêt

---

## 2. Récupérer la connection string

**Dashboard → bouton "Connect" (en haut à droite)**
→ Onglet **"Transaction pooler"**

Tu verras une string de ce format :
```
postgresql://postgres.XXXXX:[YOUR-PASSWORD]@aws-1-eu-west-3.pooler.supabase.com:6543/postgres
```

---

## 3. Mettre à jour le `.env`

```bash
SUPABASE_USER=postgres.XXXXX          
SUPABASE_PASSWORD=TON_MOT_DE_PASSE
SUPABASE_DB=postgres
SUPABASE_HOST=aws-1-eu-west-3.pooler.supabase.com   
SUPABASE_PORT=6543
```

> ⚠️ Le préfixe (`aws-0` ou `aws-1`) dépend de ton projet — copie-le exactement depuis la connection string.

---

## 4. `config.py`

```python
import os
from dotenv import load_dotenv

load_dotenv()

class Config_bdd:
    DATABASE_URL = (
        f"postgresql+psycopg2://{os.getenv('SUPABASE_USER')}:{os.getenv('SUPABASE_PASSWORD')}"
        f"@{os.getenv('SUPABASE_HOST')}:{os.getenv('SUPABASE_PORT')}/{os.getenv('SUPABASE_DB')}"
        f"?sslmode=require"
    )
```

---

## 5. `init_db.py` — passer `prepared_statement_cache_size` via `connect_args`

Le pooler Supabase ne supporte pas les prepared statements — ce paramètre doit être
passé à SQLAlchemy, pas dans l'URL (psycopg2 ne le reconnaît pas dans l'URL).

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def get_engine(database_url: str):
    return create_engine(
        database_url,
        connect_args={"prepared_statement_cache_size": 0},  # ← obligatoire avec le pooler
    )

def get_session_factory(engine):
    return sessionmaker(bind=engine, class_=Session)
```

---

## 6. Tester la connexion

```bash
uv run python3 -c "
from config import Config_bdd
from init_db import get_engine
from sqlalchemy import text

engine = get_engine(Config_bdd.DATABASE_URL)
with engine.connect() as conn:
    print('✅ Connexion Supabase OK')
    print(conn.execute(text('SELECT version()')).scalar())
"
```

---

## 7. Initialiser le schéma et ingérer

```bash
# Créer toutes les tables
uv run init_db.py

# Lancer l'ingestion complète
uv run ingest_db.py
```

---

## 8. Vérifier dans Supabase

**Dashboard → Table Editor** — toutes les tables doivent apparaître avec leurs données.

Ou via **Dashboard → SQL Editor** :

```sql
SELECT 'films'        AS table_name, COUNT(*) AS lignes FROM films       UNION ALL
SELECT 'realisateurs',               COUNT(*)            FROM realisateurs UNION ALL
SELECT 'genres',                     COUNT(*)            FROM genres       UNION ALL
SELECT 'film_genres',                COUNT(*)            FROM film_genres  UNION ALL
SELECT 'score_imdb',                 COUNT(*)            FROM score_imdb   UNION ALL
SELECT 'score_rt',                   COUNT(*)            FROM score_rt     UNION ALL
SELECT 'score_tmdb',                 COUNT(*)            FROM score_tmdb;
```

---

## 9. Sécuriser les credentials

```bash
# .gitignore — vérifier que ces lignes sont présentes
.env
*.sqlite
```

Pour les déploiements (Railway, Render…), injecter les variables d'environnement
directement dans le dashboard du service — ne jamais committer le `.env`.

---

## Résumé des fichiers modifiés

| Fichier | Changement |
|---------|-----------|
| `.env` | Credentials pooler Supabase |
| `config.py` | `?sslmode=require` dans l'URL |
| `init_db.py` | `prepared_statement_cache_size=0` dans `connect_args` |
| Docker | Plus nécessaire pour la prod |