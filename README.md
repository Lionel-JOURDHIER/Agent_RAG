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

## 1_web_scrapping : 
### Collecte des ficher .xml listant tout le site Rotten_tomatoes : 
Le site Rotten_tomatoes propose dans son sitemaps l'ensemble des sites regroupant les urls. 
Les sites de films ont une extension movies_*.xml.

depuis la racine du projet, executer : 
```bash
(cd data_tools/1_web_scrapping  && uv run src/sitemaps.py)
```

cela génèrera l'ensemble des fichier xml dans le dossier `rt_sitemaps`

### Extraction des sites de films : 
Une fois les fichiers .xml dans le dossier `rt_sitemaps`, on peut extraire les urls de chaque film. 

depuis la racine du projet, executer : 
```bash
(cd data_tools/1_web_scrapping  && uv run src/scrapper.py)
```

cela génèrera un fichier : `./data_tools/1_web_scrapping/data/index_rotten_tomatoes.csv`

**NOTA IMPORTANTE** : Avant d'aller plus loin il faut avoir déjà exécuté l'extraction des films de la partie **2_api_externe**. 

### Prétraitement de la base de donnée
le fichier .csv comprend pres de 250 000 entrée, tout les films ne sont pas des films d'horreurs. 
Il faut donc verifier si les films sont ou nom deja présent dans la base de donnée 

```bash
(cd data_tools/1_web_scrapping  && uv run src/merge.py)
```

### Extraction des information du site rotten_tomatoes.
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

cela génèrera un fichier : `./data_tools/0_shared/data/horror_movies_rt_scores.csv`

## 2_api_externe : 
### Recupération des films d'horreurs à partir de l'API TMDB. 
Recupération des films d'horreurs à partir de l'API TMDB. 

depuis la racine du projet, exécuter :
```bash
(cd data_tools/2_api_externe && uv run src/movies.py)
```

cela génèrera un fichier : `./data_tools/0_shared/data/horror_movies_tmdb.csv`

### Suppression des doublons
Suppression des doublons dans le fichier CSV.
```bash
(cd data_tools/2_api_externe && uv run src/dedup.py)
```

### Extraction de l'ID IMDB
Extraction de l'ID IMDB depuis l'API pour augmenter la précision des données. 
```bash
(cd data_tools/2_api_externe && uv run src/imdb.py)
```

## 3_local_files : 
### Suppression des doublons
Suppression des doublons dans le fichier CSV.
```bash
(cd data_tools/3_local_files && uv run src/dedup.py)
```
cela génèrera un fichier : `./data_tools/0_shared/data/horror_movies_kaggle.csv`

### Selection des informations pertinantes
Choix des colonnes pertinentes pour l'analyse des données. 
```bash
(cd data_tools/3_local_files && uv run src/processor.py)
```
cela génèrera un fichier : `./data_tools/0_shared/data/horror_movies_kaggle.csv`

## 4_database : 
### Extraction des données depuis la base de données
Extraction complète de la table movies de la base de donnée en intégrant le nom du réalisateur.
```bash
(cd data_tools/4_database && uv run src/db.py)
```
cela génèrera un fichier : `./data_tools/0_shared/data/horror_movies_database.csv`

## 5_big_data : 
recupération d'information depuis les fichier developpeur big_data de IMDB, 
on utilise le dataset "title.ratings.tsv" et "title.basics.tsv" qui contient des informations sur les films et les ratings
```bash
(cd data_tools/5_big_data && uv run src/extraction.py)
```
cela génèrera un fichier : `./data_tools/0_shared/data/horror_movies_imdb_scores.csv`

## Synthèse des données et architecture de la base de donnée. 
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
  6. id_tertiaire                                : slug(title)_year en première colonne

On execute ce nettoyage avec le script `rt_cleaner.py`.
```bash
(cd data_tools/0_shared/services/rt_cleaner.py)
```

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
  +  id_tertiaire  : slug(title)_year ajouté en première colonne

On execute ce nettoyage avec le script `tmdb_cleaner.py`.
```bash
(cd data_tools/0_shared/services/tmdb_cleaner.py)
```


### Services de traitement des données : 
Pour pouvoir regrouper les différents csv, on uttilise un prétraitement des données pour pouvoir comparer les données. 