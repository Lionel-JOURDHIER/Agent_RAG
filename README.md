# HorRAGor

## Installation
### Création des environnement et uv 
```bash
(cd data_tools/0_shared  && uv sync)
(cd data_tools/1_web_scrapping  && uv sync)
(cd data_tools/2_api_externe  && uv sync)
(cd data_tools/3_loal_files  && uv sync)
(cd data_tools/4_database  && uv sync)
(cd data_tools/5_big_data  && uv sync)
```

## Ingestion Multimodale
### 1_web_scrapping : 
#### Collecte des ficher .xml listant tout le site Rotten_tomatoes : 
Le site Rotten_tomatoes propose dans son sitemaps l'ensemble des sites regroupant les urls. 
Les sites de films ont une extension movies_*.xml.

depuis la racine du projet, executer : 
```bash
(cd data_tools/1_web_scrapping  && uv run src/sitemaps.py)
```

cela génèrera l'ensemble des fichier xml dans le dossier `rt_sitemaps`

#### Extraction des sites de films : 
Une fois les fichiers .xml dans le dossier `rt_sitemaps`, on peut extraire les urls de chaque film. 

depuis la racine du projet, executer : 
```bash
(cd data_tools/1_web_scrapping  && uv run src/scrapper.py)
```

cela génèrera un fichier : `./data_tools/1_web_scrapping/data/index_rotten_tomatoes.csv`

**NOTA IMPORTANTE** : Avant d'aller plus loin il faut avoir déjà exécuté l'extraction des films de la partie **2_api_externe**. 

#### Prétraitement de la base de donnée
le fichier .csv comprend pres de 250 000 entrée, tout les films ne sont pas des films d'horreurs. 
Il faut donc verifier si les films sont ou nom deja présent dans la base de donnée 

```bash
(cd data_tools/1_web_scrapping  && uv run src/merge.py)
```
cela génèrera un fichier : `./data_tools/1_web_scrapping/data/horror_movies_merged.csv`

#### Extraction des information du site rotten_tomatoes.
Une fois le fichier .csv extrait, on peut extraire les informations des films depuis le site rotten_tomatoes. 
les informations recherchées sont les suivantes : 
- Score Critique
- Score Audience
- Concensus Critique
- Titre du film
- Année du film

depuis la racine du projet, executer : 
```bash
(cd data_tools/1_web_scrapping  && uv run src/crawler.py)
```

cela génèrera un fichier : `./data_tools/1_web_scrapping/data/horror_movies_rt_scores_raw.csv`

### 2_api_externe : 
### Recupération des films d'horreurs à partir de l'API TMDB. 
Recupération des films d'horreurs à partir de l'API TMDB. 

depuis la racine du projet, exécuter :
```bash
(cd data_tools/2_api_externe && uv run src/movies.py)
```

cela génèrera un fichier : `./data_tools/2_api_externe/data/horror_movies_tmdb.csv`

#### Suppression des doublons
Suppression des doublons dans le fichier CSV.
```bash
(cd data_tools/2_api_externe && uv run src/dedup.py)
```
cela génèrera un fichier : `./data_tools/2_api_externe/data/horror_movies_tmdb_raw.csv`

#### Extraction de l'ID IMDB
Extraction de l'ID IMDB depuis l'API pour augmenter la précision des données. 
```bash
(cd data_tools/2_api_externe && uv run src/imdb.py)
```
cela met à jour le fichier : `./data_tools/2_api_externe/data/horror_movies_tmdb_raw.csv`

#### Pipeline complet : 
```bash
(cd data_tools/2_api_externe && uv run src/movies.py)
(cd data_tools/2_api_externe && uv run src/dedup.py)

```

### 3_local_files : 
#### Suppression des doublons
Suppression des doublons dans le fichier CSV.
```bash
(cd data_tools/3_local_files && uv run src/dedup.py)
```
cela génèrera un fichier : `./data_tools/3_local_files/data/horror_movies_kaggle.csv`


### 4_database : 
#### Extraction des données depuis la base de données
Extraction complète de la table movies de la base de donnée en intégrant le nom du réalisateur.
```bash
(cd data_tools/4_database && uv run src/db.py)
```
cela génèrera un fichier : `./data_tools/4_database/data/horror_movies_database.csv`

### 5_big_data : 
recupération d'information depuis les fichier developpeur big_data de IMDB, 
on utilise le dataset "title.ratings.tsv" et "title.basics.tsv" qui contient des informations sur les films et les ratings
```bash
(cd data_tools/5_big_data && uv run src/extraction.py)
```
cela génèrera un fichier : `./data_tools/5_big_data/data/horror_movies_imdb_scores.csv`

## Stratégie de Fusion et Réconciliation (MDM) :  
### 0_shared/data : 
Ce dossier contient l'ensemble des fichiers CSV générés par les scripts précédents.

### Netoyage et uniformaistion des données :
#### Fichier Rottentomatoes : 
Nettoyage du fichier horror_movies_rt_scores_raw.csv selon les règles suivantes :

  1. Doublons exacts (url_rotten ou title+year)  : suppression
  2. Lignes sans title ET sans year              : suppression
  3. rt_tomatometer zéros                        : conservés (0% valide sur RT)
  4. rt_audience_score zéros                     : conservés (0% valide sur RT)
  5. year float64                                : → Int64 (entier nullable)
  6. id_tertiaire                                : slug(title)_year ajouté

On execute ce nettoyage avec le script `rt_cleaner.py`.
```bash
(cd data_tools/0_shared && uv run services/rt_cleaner.py)
```

cela génèrera un fichier : `./data_tools/0_shared/raw_data/horror_movies_rt_scores.csv`

#### Fichier TMDB : 
Nettoyage de horror_movies_tmdb_raw.csv selon les règles suivantes :

  1. vote_average  : 0.0 → NaN
  2. Doublons exacts (title + release_date) : garder la ligne avec le moins de NaN
  3. genres        : inchangé
  4. Films futurs  : inchangés
  5. url_rotten    : inchangé
  6. popularity    : 0.0 → NaN
  7. overview      : inchangé
  8. title null    : suppression de la ligne
  9. suppr \n\r    : suppression des retour à la ligne
  +  id_tertiaire  : slug(title)_year ajouté 

On execute ce nettoyage avec le script `tmdb_cleaner.py`.
```bash
(cd data_tools/0_shared && uv run services/tmdb_cleaner.py)
```
cela génèrera un fichier : `./data_tools/0_shared/raw_data/horror_movies_tmdb.csv`

#### Fichier Kaggle : 
Nettoyage de horror_movies.csv selon les règles suivantes :

  1.  Unnamed: 0 (index fantôme)          : suppression
  2.  adult (100% False)                  : suppression
  3.  budget <1000 et >0 (unités mixtes)  : → NaN
  4.  budget et revenue zéros             : → NaN
  5.  vote_average et vote_count zéros    : → NaN
      + incohérences croisées             : → NaN sur le champ incohérent
  6.  runtime zéros                       : → NaN  (< 10 min conservés)
  7.  popularity zéros                    : conservés
  8.  status                              : inchangé
  9.  id_tertiaire                        : slug(title)_year en première colonne

On execute ce nettoyage avec le script `kaggle_cleaner.py`.
```bash
(cd data_tools/0_shared && uv run services/kaggle_cleaner.py)
```
cela génèrera un fichier : `./data_tools/0_shared/raw_data/horror_movies_kaggle.csv`

#### Fichier Database : 
Nettoyage de horror_movies_database.csv selon les règles suivantes :

  1. budget < 1000 et > 0 (unités mixtes)  : → NaN
  2. budget et revenue zéros               : → NaN
  3. vote_average et vote_count zéros      : → NaN
  4. popularity zéros                      : conservés
  5. colonne `id` séquentielle             : supprimée  (`uid` = vrai ID TMDB)
  6. id_tertiaire                          : slug(title)_year ajouté


On execute ce nettoyage avec le script `db_cleaner.py`.
```bash
(cd data_tools/0_shared && uv run services/db_cleaner.py)
```
cela génèrera un fichier : `./data_tools/0_shared/raw_data/horror_movies_db.csv`

#### Fichier IMDB_scores : 
Nettoyage de horror_movies_imdb_scores.csv selon les règles suivantes :

  1. title null (1 ligne)          : suppression
  2. id_tertiaire                  : non généré (pas de colonne année)
  3. numVotes                      : inchangé (float64 conservé)
  4. genres format virgule         : normalisé → "Horror, Drama" (virgule + espace)
  5. primaryTitle null             : primaryTitle = title


On execute ce nettoyage avec le script `imdb_cleaner.py`.
```bash
(cd data_tools/0_shared && uv run services/imdb_cleaner.py)
```
cela génèrera un fichier : `./data_tools/0_shared/raw_data/horror_movies_tmdb.csv`

### Création des tables format csv
#### Création de la table collections.csv : 
Source : raw_data/horror_movies_kaggle.csv (colonnes collection + collection_name)
Sortie : data/collections.csv
  id_collection (AUTO_INCREMENT géré par la BDD, absent du CSV)
  tmdb_collection_id (INT)
  collection_name    (VARCHAR 60)

On execute la création avec le script `build_collection.py`.
```bash
(cd data_tools/0_shared && uv run services_database/build_collection.py)
```
cela génèrera un fichier : `./data_tools/0_shared/data/collections.csv`

#### Création des tables genre et film_genres : 
Source : raw_data/horror_movies_kaggle.csv (colonne genres)
Sortie : data/genres.csv
    id_genre (AUTO_INCREMENT géré par la BDD, absent du CSV)
    genre_name    (VARCHAR 50)
Sortie : data/filmgenres.csv
    id_film_genre (AUTO_INCREMENT géré par la BDD, absent du CSV)
    tmdb_id (INT FK),
    id_genre (SMALLINT FK)
  
On execute la création avec le script `build_genre.py`.
```bash
(cd data_tools/0_shared && uv run services_database/build_genre.py)
```
cela génèrera deux fichiers CSV : 
  `./data_tools/0_shared/data/genre.csv`
  `./data_tools/0_shared/data/film_genres.csv`

#### Création de la table Réalisateurs : 
Source : raw_data/horror_movies_db (colonnes director_id + name)
Sortie : data/realisateurs.csv
  director_id (INT PK)
  name        (VARCHAR 50)

On execute la création avec le script `build_realisateur.py`.
```bash
(cd data_tools/0_shared && uv run services_database/build_realisateur.py)
```

cela génèrera un fichier : `./data_tools/0_shared/data/realisateurs.csv`

#### Création de la table Scores IMDB : 
Source : raw_data/horror_movies_imdb_scores.csv (tconst + title + averageRating + numVotes)
Sortie : data/scores_imdb.csv
    id_score_imdb (AUTO_INCREMENT géré par la BDD, absent du CSV)
    tconst (VARCHAR(10) FK),
    title (VARCHAR(150)),
    average_rating (DECIMAL(3,1)),
    num_votes (INT)

On execute la création avec le script `build_scores_imdb.py`.
```bash
(cd data_tools/0_shared && uv run services_database/build_scores_imdb.py)
```

cela génèrera un fichier : `./data_tools/0_shared/data/scores_imdb.csv`

#### Création de la table Scores RT : 
Source : raw_data/horror_movies_rt_scores.csv (colonnes id_tertiaire + url_rotten + rt_tomatometer + rt_audience_score + rt_critics_consensus)
Sortie : data/scores_rt.csv
    id_score_rt (AUTO_INCREMENT géré par la BDD, absent du CSV)
    id_tertiaire (VARCHAR(200) FK),
    url_rotten (VARCHAR(120) UK),
    rt_tomatometer (SMALLINT),
    rt_audience_score (SMALLINT),
    rt_critics_consensus (VARCHAR(285))

On execute la création avec le script `build_scores_rt.py`.
```bash
(cd data_tools/0_shared && uv run services_database/build_scores_rt.py)
```

cela génèrera un fichier : `./data_tools/0_shared/data/scores_rt.csv`

#### Création de la table Scores TMDB : 
Source :
    raw_data/horror_movies_tmdb.csv (tmdb_id + vote_average + vote_count + popularity)

Sortie : data/scores_tmdb.csv
    id_score_tmdb (AUTO_INCREMENT géré par la BDD, absent du CSV)
    tmdb_id (INT FK),
    vote_average (DECIMAL(3,1)),
    vote_count (INT),
    popularity (DECIMAL(10,4))

On execute la création avec le script `build_scores_tmdb.py`.
```bash
(cd data_tools/0_shared && uv run services_database/build_scores_tmdb.py)
```

cela génèrera un fichier : `./data_tools/0_shared/data/scores_tmdb.csv`

#### Création de la tables films.csv : 
Source :
    raw_data/horror_movies_tmdb.csv (tmdb_id + imdb_id_fetched + id_tertiaire + title + release_date + overview + poster_path)
    raw_data/horror_movies_kaggle.csv (id_collection + original_title + original_language + status + runtime + tagline + budget + revenue)
    raw_data/horror_movies_db.csv (director_id)

Sortie : data/scores_tmdb.csv
    tmdb_id (INT PK),
    director_id (INT FK),
    id_collection (INT FK),
    imdb_id(VARCHAR(10) UK),
    id_tertiaire(VARCHAR(255), UK),
    title (VARCHAR(200), NON NULL),
    original_title (VARCHAR(200)),
    original_language (CHAR(2)),
    release_date (DATE),
    status (VARCHAR(15)),
    runtime (SMALLINT),
    overview (TEXT),
    tagline (VARCHAR(260)),
    poster_path (VARCHAR(65)),
    budget (BIGINT),
    revenue (BIGINT),

On execute la création avec le script `build_films.py`.
```bash
(cd data_tools/0_shared && uv run services_database/build_films.py)
```

cela génèrera un fichier : `./data_tools/0_shared/data/films.csv`

## Modélisation et Persistance des Données
### Structuration de la base de données : 
Le projet est structuré en tables : 

- `collections` : contient les collections de films.
- `films` : contient les films, avec leurs informations principales
- `genres` : contient les genres des films
- `realisateurs` : contient les réalisateurs des films
- `scores_rt` : contient les scores RT des films
- `scores_imdb` : contient les scores IMDb des films
- `scores_tmdb` : contient les scores TMDB des films
- `film_genres` : contient les relations entre les genres et les films

### Creation de la base de donnée : 
Dans le cadre de la création de la base de données, nous avons utilisé SQLite pour un premier test de stockage des données. 
Cela permet d'avoir une vision directe des defauts éventuels d'intégration de la bdd. 

Pour changer le type de base de données il faut changer le paramètre DATABASE_URL dans le fichier 0_shared/config
Ce paramètre fait partie de la classe Config_bdd qui est une classe de configuration.

ci joint des exemples pour différentes base de données : 
```python 
# SQLITE
DATABASE_URL = "sqlite:///sqlite/horror_db.sqlite"
# POSTGRES DOCKER
DATABASE_URL = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)
# POSTGRES SUPABASE
DATABASE_URL = (
        f"postgresql+psycopg2://{os.getenv('SUPABASE_USER')}:{os.getenv('SUPABASE_PASSWORD')}"
        f"@{os.getenv('SUPABASE_HOST')}:{os.getenv('SUPABASE_PORT')}/{os.getenv('SUPABASE_DB')}"
        f"?sslmode=require" 
    )

```

On execute la création avec le script `init_db.py`.
```bash
(cd data_tools/0_shared/ && uv run init_db.py)
```

cela génèrera un fichier : `./data_tools/0_shared/sqlite/horror_db.sqlite`

### Ingestion des données de l'ensemble de la base de donnée : 
Pour faire l'ingestion des données dans l'ensemble de la base de données `ingest_db.py`.

```bash
(cd data_tools/0_shared/ && uv run ingest_db.py)
```
Ce fichier regroupe les pipelines intégrés aux fichiers : 
- `services_database.ingest_collection.py`
- `services_database.ingest_genre.py`
- `services_database.ingest_films.py`
- `services_database.ingest_realisateur.py`
- `services_database.ingest_scores_imdb.py`
- `services_database.ingest_scores_rt.py`
- `services_database.ingest_scores_tmdb`