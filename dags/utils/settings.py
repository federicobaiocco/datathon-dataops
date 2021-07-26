DATASET_URLS = {
    "TITLE_RATINGS": "https://datasets.imdbws.com/title.ratings.tsv.gz",
    "TITLE_CREW": "https://datasets.imdbws.com/title.crew.tsv.gz",
    "TITLE_BASICS": "https://datasets.imdbws.com/title.basics.tsv.gz",
    "NAME_BASICS": "https://datasets.imdbws.com/name.basics.tsv.gz",
}

TITLE_TYPE = "movie"
TITLE_BASICS_USED_COLUMNS = ["tconst", "startYear", "genres", "runtimeMinutes"]
TITLE_BASICS_YEAR_FROM = 2015
TITLE_BASICS_YEAR_TO = 2020

RESUME_GROUPED_BY = ["startYear", "genres"]
RESUME_AGG_FUNCTIONS_DICT = {
    "runtimeMinutes": "mean",
    "averageRating": "mean",
    "numVotes": "sum",
}

TITLE_CREW_USED_COLUMNS = ["startYear", "genres", "directors", "writers"]

OUTPUT_FILE_PATH = "/home/airflow/resultados.csv"
