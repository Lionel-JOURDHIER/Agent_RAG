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