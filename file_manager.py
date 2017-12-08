import pandas as pd
import ast
from collections import defaultdict

MOVIES_FILEPATH = "movies_demo.csv"
movies_inner_cols = ["belongs_to_collection", "genres", "production_companies", "production_countries", "spoken_languages"]
movies_json_cols = ["belongs_to_collection"]
movies_array_cols = [("genres", "id", "movies", "tmdb"),
                     ("production_companies", "id", "movies", "tmdb"),
                     ("production_countries", "iso_3166_1", "movies", "tmdb"),
                     ("spoken_languages", "iso_639_1", "movies", "tmdb")]

CREDITS_FILEPATH = "credits_demo.csv"
credits_inner_cols = ["cast", "crew"]
credits_array_cols = [("cast", "id", "movies", "tmdb"), ("crew", "id", "movies", "tmdb")]

KEYWORDS_FILEPATH = "keywords_demo.csv"
keywords_inner_cols = ["keywords"]
keywords_array_cols = [("keywords", "id", "movies", "tmdb")]

LINKS_FILEPATH = "links_demo.csv"

RATINGS_FILEPATH = "ratings_demo.csv"


def read_file(filename, json_cols=list()):
    data = pd.read_csv(filename)
    for column in json_cols:
        for i, x in enumerate(data[column]):
            if not pd.isnull(x):
                data[column][i] = ast.literal_eval(x)
    return data



#split json columns from elemental columns
def split_json_columns(data, columns, row_id, id_append):
    jsons = {}
    row_id_name = row_id + "_" + id_append
    for column in columns:
        col_data = data[column]
        col_df = defaultdict(list)
        for i, json in enumerate(col_data):
            if not pd.isnull(json):
                outer_id = data[row_id][i]
                for key in json:
                    col_df[key].append(json[key])
                col_df[row_id_name].append(outer_id)
        jsons[column] = pd.DataFrame(col_df)
        data.drop(column, axis=1, inplace=True)
    return jsons


def split_array_columns(data, data_id_name, array_columns):
    split_cols = {}
    for end_table, end_id, mid_table, mid_id_append in array_columns:       # cez vse stolpce ki vsebujejo seznam
        mid_df = pd.DataFrame()                            # skupna tabela kombinacij id-jev
        mid_df_name = mid_table + "_" + end_table       # ime vmesne tabele (npr. movies_keywords)
        end_df = pd.DataFrame()                           # skupna tabela vseh elementov znotraj seznama
        end_df_name = end_table                          # ime tabele/seznama objektov
        col_data = data[end_table]                         # trenutni stolpec
        for i, array in enumerate(col_data):                # cez vsako vrstico v stolpcu
            if len(array) > 0:                           # ce ni prazen
                data_id = data[data_id_name][i]          # id trenutnega stolpca
                end_id_name = "id_" + end_table        # novo ime stolpca tabele (npr. id_keywords)
                end_table_row = pd.DataFrame(array)      # pretvori seznam v dataframe
                end_table_row.rename(index=str, columns={end_id: end_id_name}, inplace=True) # spremeni ime stolpca (id -> id_keywords)
                mid_id_name = "id_" + mid_id_append  # novo ime stolpca vmesne tabele (npr. id_tmdb)
                mid_table_row = pd.DataFrame({mid_id_name: [data_id for i in range(end_table_row.shape[0])], end_id_name: end_table_row[end_id_name]})  # generiraj vmesno tabelo
                mid_df = pd.concat([mid_df, mid_table_row]) # pripni k skupni tabeli
                end_df = pd.concat([end_df, end_table_row])
        data.drop(end_table, axis=1, inplace=True)
        split_cols[mid_df_name] = mid_df  # vstavi tabelo v slovar {"movies_keywords": vmesna_tabela, "keywords": tabela, ...}
        split_cols[end_df_name] = end_df
    return split_cols


#convert json arrays and jsons to dataframe
def json_to_dataframe(jsons):
    data = defaultdict(list)
    for json in jsons:
        # if not empty array
        if isinstance(json, dict):
            for key in json:
                data[key].append(json[key])
    dataframe = pd.DataFrame(data)
    return dataframe


# ready for DB
def read_movies():
    movies_raw = read_file(MOVIES_FILEPATH, movies_inner_cols)
    split_array_tables = split_array_columns(movies_raw, "id", movies_array_cols)
    split_json_tables = split_json_columns(movies_raw, movies_json_cols, "id", "tmdb")
    movies_raw.rename(index=str, columns={"id": "id_tmdb", "imdb_id": "id_imdb"}, inplace=True)
    movies_table = {"movies": movies_raw}
    # push split_array_tables, split_json_tables, movies_table


# ready for DB
def read_keywords():
    keywords_raw = read_file(KEYWORDS_FILEPATH, keywords_inner_cols)
    split_array_tables = split_array_columns(keywords_raw, "id", keywords_array_cols)
    # push split_array_tables


#most likely ready for DB
def read_credits():
    credits = read_file(CREDITS_FILEPATH, credits_inner_cols)
    split_cols = split_array_columns(credits, "id", credits_array_cols)
    movies_cast = split_cols["movies_cast"]
    movies_crew = split_cols["movies_crew"]
    cast = split_cols["cast"]
    crew = split_cols["crew"]
    movies_cast = pd.concat([movies_cast, cast["credit_id"]], axis=1, join="outer")
    actors = pd.concat([cast["id_cast"], cast["name"], cast["gender"], cast["profile_path"]], axis=1, join="outer")
    cast.drop(columns=["cast_id", "id_cast", "gender", "profile_path", "name"], axis=1, inplace=True)
    movies_crew = pd.concat([movies_crew, crew["credit_id"]], axis=1, join="outer")
    coworkers = pd.concat([crew["id_crew"], crew["name"], crew["gender"], crew["profile_path"]], axis=1, join="outer")
    crew.drop(columns=["name", "gender", "id_crew", "profile_path"], axis=1, inplace=True)


    credits_tables = {
        "movies_cast": movies_cast,
        "cast": cast,
        "actors": actors,
        "movies_crew": movies_crew,
        "crew": crew,
        "coworkers": coworkers
    }



# ready for DB
def read_links():
    links = read_file(LINKS_FILEPATH)
    links.rename(index=str, columns={"movieId": "id_movie", "imdbId": "id_imdb", "tmdbId": "id_tmdb"}, inplace=True)
    links_table = {"links": links}
    # push links_table


# ready for DB
def read_ratings():
    ratings = read_file(RATINGS_FILEPATH)
    ratings.rename(index=str, columns={"userId": "id_user","movieId": "id_movie"}, inplace=True)
    ratings_table = {"ratings": ratings}
    # push ratings_table


if __name__ == "__main__":
    #read_movies()
    #read_keywords()
    read_credits()
    #read_links()
    #read_ratings()