# HorRAGor

## Installation

## 2_api_externe : 
### Recupération des films d'horreurs à partir de l'API TMDB. 
Recupération des films d'horreurs à partir de l'API TMDB. 

depuis la racine du projet, exécuter :
```bash
(cd data_tools/2_api_externe && uv run src/movies.py)
```

cela génèrera un fichier : `./data_tools/0_shared/data/horror_movies_tmdb.csv`

### Suppression des doublons
```bash
(cd data_tools/2_api_externe && uv run src/dedup.py)
```

### Extraction
```bash
(cd data_tools/2_api_externe && uv run src/dedup.py)
```

## 3_local_files : 
### suppression des doublons des films d'horreurs dans un fichier CSV.
```bash
(cd data_tools/3_local_files && uv run src/dedup.py)
```
cela génèrera un fichier : `./data_tools/0_shared/data/horror_movies_kaggle.csv`

### export de la table csv avec les bonnes tables.
```bash
(cd data_tools/3_local_files && uv run src/processor.py)
```
cela génèrera un fichier : `./data_tools/0_shared/data/horror_movies_kaggle.csv`

## 4_database : 
```bash
(cd data_tools/4_database && uv run src/db.py)
```
cela génèrera un fichier : `./data_tools/0_shared/data/horror_movies_database.csv`

## 5_big_data : 
```bash
(cd data_tools/5_big_data && uv run src/extraction.py)
```
cela génèrera un fichier : `./data_tools/0_shared/data/horror_movies_imdb_scores.csv`