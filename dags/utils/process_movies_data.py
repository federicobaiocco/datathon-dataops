from pandas import DataFrame, read_csv
from utils.settings import (
    DATASET_URLS,
    TITLE_TYPE,
    TITLE_BASICS_USED_COLUMNS,
    TITLE_BASICS_YEAR_FROM,
    TITLE_BASICS_YEAR_TO,
    RESUME_GROUPED_BY,
    RESUME_AGG_FUNCTIONS_DICT,
    TITLE_CREW_USED_COLUMNS,
    OUTPUT_FILE_PATH,
)
import logging

logger = logging.getLogger(__name__)


def process_title_basics_df(title_basics_df: DataFrame) -> DataFrame:
    """Clean title_basics_df by removing NaN values and set the corresponding dtypes.
    Once the df is clean, 2 masks are applied:
        - movie_mask: Get the titles with titleType = TITLE_TYPE (defined in settings.py)
        - date_mask: Get titles with startYear between TITLE_BASICS_YEAR_FROM and TITLE_BASICS_YEAR_TO

    :param title_basics_df: original title_basics_df
    :return df: clean title_basics_df
    """

    df = title_basics_df.copy()
    df = df[df.startYear.notna()]

    movie_mask = df.titleType == TITLE_TYPE
    date_mask = df.startYear.between(
        TITLE_BASICS_YEAR_FROM, TITLE_BASICS_YEAR_TO, inclusive=True
    )
    df = df[movie_mask & date_mask][TITLE_BASICS_USED_COLUMNS]

    return df


def get_title_ratings(
    clean_title_basics_df: DataFrame, title_ratings_df: DataFrame
) -> DataFrame:
    """Merge clean title_basics_df with title_ratings_df.
    After the merge, NaN values are dropped.
    Genres column is exploded in order to get one row per genre.

    :param clean_title_basics_df: title_basics_df without NaN values and correct dtypes
    :param title_ratings_df: pandas dataframe with columns avgRating, runtimeMinutes, numVotes and tconst
    :return titles_data: Clean df containing titles with avgRating, runtimeMinutes and numVotes
    """

    title_data = clean_title_basics_df.copy()
    title_ratings = title_ratings_df.copy()

    # Merge dataframes
    titles_data = title_data.merge(title_ratings, how="left", on="tconst")

    # Drop na values
    titles_data = titles_data[titles_data.genres.notna()]

    # Convert genres column to list and explode it
    titles_data["genres"] = titles_data["genres"].apply(lambda x: x.split(","))
    titles_data = titles_data.explode("genres")

    # Set correct dtypes
    titles_data["runtimeMinutes"] = titles_data["runtimeMinutes"].astype(float)

    return titles_data


def get_year_genre_resume(titles_data_df: DataFrame) -> DataFrame:
    """Get resume grouping by columns specified in RESUME_GROUPED_BY constant and
    aggregated with functions specified in RESUME_AGG_FUNCTIONS_DICT.

    :param titles_data_df: Clean dataframe with titles data
    :return year_genre_resume: resume grouped by RESUME_GROUPED_BY columns
    """

    df = titles_data_df.copy()
    year_genre_resume = df.groupby(RESUME_GROUPED_BY).agg(RESUME_AGG_FUNCTIONS_DICT)

    return year_genre_resume


def get_id2name_dict(name_basics_df: DataFrame) -> dict:
    """Create a dict that can be used to easly translate from director id to director name.

    :param name_basics_df: DataFrame containing columns nconst and primaryName of directors
    :return id2name_dict: A dict containing nconst as keys and primaryName as values
    """

    id2name_dict = (
        name_basics_df[["nconst", "primaryName"]]
        .set_index("nconst")
        .to_dict()["primaryName"]
    )
    return id2name_dict


def director_id2name(ids, id2name_dict: dict) -> str:
    """Translate director ids to names.
        - When there is only one director, its name is returns
        - When more than 1 director is received, they are translated to names as a string separated by ;

    :param ids: row with id or ids of directors
    :return names: A str containing director names separated with ;
    """
    names = ""
    if str(type(ids)) == "<class 'numpy.ndarray'>":
        for id in ids:
            names += f"{id2name_dict[id]}; "
        if names.endswith("; "):
            names = names[:-2]
    else:
        names += f"{id2name_dict[ids]}"

    return names


def get_crew_data_by_group(
    clean_title_basics_df: DataFrame,
    title_crew_df: DataFrame,
    director_id2name_dict: dict,
) -> tuple:
    """Compute crew data.
    Clean NaN values in received dfs, process them and compute:
        - Number of directors per group (grouped by RESUME_GROUPED_BY)
        - Number of writers per group (grouped by RESUME_GROUPED_BY)
        - Top director names per group (grouped by RESUME_GROUPED_BY): Directors with most titles in the group

    :param clean_title_basics_df: title_basics_df cleaned as defined in process_title_basics_df()
    :param title_crew_df: DataFrame with columns tconst, directors and writers
    :param director_id2name_dict: dict generated as defined in get_id2name_dict()
    :return tuple of 3 DataFrames: n_directors, top_directors, n_writers
    """

    # Merge dataframes
    df = clean_title_basics_df.merge(title_crew_df, how="left", on="tconst")

    # Remove NaN values
    df = df[df["genres"].notna()]
    df = df[df["directors"].notna()]

    # Keep used columns
    df = df[TITLE_CREW_USED_COLUMNS]

    # Transform genres and directors to lists and explode them
    df["genres"] = df["genres"].apply(lambda x: x.split(","))
    df["directors"] = df["directors"].apply(lambda x: x.split(","))
    df = df.explode("genres")
    df = df.explode("directors")

    # Compute number of director per group
    logger.info("Processing n_directors")
    n_directors = df.groupby(RESUME_GROUPED_BY).agg({"directors": "nunique"})

    # Compute top directors per group
    logger.info("Processing top_directors")
    top_directors = df.groupby(RESUME_GROUPED_BY).agg({"directors": lambda x: x.mode()})

    # Compute number of writers per group
    logger.info("Processing n_writers")
    n_writers = df.groupby(RESUME_GROUPED_BY).agg({"writers": "nunique"})

    # Process top directors names (translate ids to names)
    top_directors["directors"] = top_directors["directors"].apply(
        lambda x: director_id2name(x, director_id2name_dict)
    )

    return n_directors, top_directors, n_writers


def run_pipeline():
    """Execute the whole pipeline in order to get the output"""

    # Read name_basics_df
    logger.info("Loading NAME_BASICS csv")
    name_basics_df = read_csv(DATASET_URLS["NAME_BASICS"], sep="\t", na_values="\\N")

    # Get dict used to easily translate from director ids to names
    logger.info("Creating director_id2name_dict")
    director_id2name_dict = get_id2name_dict(name_basics_df)
    del name_basics_df

    # Read title_basics_df and clean it
    logger.info("Loading TITLE_BASICS csv")
    title_basics_df = read_csv(DATASET_URLS["TITLE_BASICS"], sep="\t", na_values="\\N")
    logger.info("Processing title_basics_df")
    clean_title_basics = process_title_basics_df(title_basics_df)
    del title_basics_df

    # Read title_ratings df and merge it with the clean_title_basics df
    logger.info("Loading TITLE_RATINGS csv")
    title_ratings_df = read_csv(
        DATASET_URLS["TITLE_RATINGS"], sep="\t", na_values="\\N"
    )
    logger.info("Processing title ratings")
    clean_title_ratings = get_title_ratings(clean_title_basics, title_ratings_df)
    del title_ratings_df

    # Compute resume containing avg rating, runtimeMinutes and numVotes
    logger.info("Computing year_genre resume")
    year_genre_resume = get_year_genre_resume(clean_title_ratings)
    del clean_title_ratings

    # Read title_crew df and get resume of crews by year/genre
    logger.info("Loading TITLE_CREW csv")
    title_crew_df = read_csv(DATASET_URLS["TITLE_CREW"], sep="\t", na_values="\\N")
    logger.info("Processing crew data by group")
    n_directors, top_directors, n_writers = get_crew_data_by_group(
        clean_title_basics, title_crew_df, director_id2name_dict
    )
    del title_crew_df

    # Merge data in order to get desired output
    logger.info("Creating output")
    output = (
        year_genre_resume.merge(
            n_writers, how="left", left_index=True, right_index=True
        )
        .merge(n_directors, how="left", left_index=True, right_index=True)
        .merge(top_directors, how="left", left_index=True, right_index=True)
    )
    # Output to desired format and dtypes
    output = output.reset_index()
    logger.info("Renaming output columns")
    output.columns = [
        "startYear",
        "genres",
        "runtimeMinutes",
        "averageRating",
        "numVotes",
        "numWriters",
        "numDirectors",
        "topDirectors",
    ]

    logger.info("Round output to 2 decimals")
    output["runtimeMinutes"] = output["runtimeMinutes"].apply(lambda x: round(x, 2))
    output["averageRating"] = output["averageRating"].apply(lambda x: round(x, 2))

    logger.info("Casting correspondent columns to int")
    output["numVotes"] = output["numVotes"].astype(int)
    output["numDirectors"] = output["numDirectors"].fillna(0).astype(int)
    output["startYear"] = output["startYear"].astype(int)
    output["numWriters"] = output["numVotes"].astype(int)

    # Dump output to csv without headers
    logger.info("Dumping output to csv")
    output.to_csv(OUTPUT_FILE_PATH, index=None)
    logger.info(f"Done. Results stored at {OUTPUT_FILE_PATH}")
