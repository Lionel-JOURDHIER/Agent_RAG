# HorRAGor

## Installation

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
- Concenssus Critique
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
```bash
(cd data_tools/2_api_externe && uv run src/dedup.py)
```

### Extraction de l'ID IMDB
```bash
(cd data_tools/2_api_externe && uv run src/imdb.py)
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