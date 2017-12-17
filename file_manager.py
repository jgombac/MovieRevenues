import pandas as pd
import ast
from collections import defaultdict
import db_connector as db
import sqlalchemy as sl
import urllib.parse
import data_types as dt
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
                data[column][i] = json["id"]
                #col_df[row_id_name].append(outer_id)
        jsons[column] = pd.DataFrame(col_df)
        #data.drop(column, axis=1, inplace=True)
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
def read_movies(dbinstance):
    movies_raw = read_file(MOVIES_FILEPATH, movies_inner_cols)
    split_array_tables = split_array_columns(movies_raw, "id", movies_array_cols)
    split_json_tables = split_json_columns(movies_raw, movies_json_cols, "id", "tmdb")
    movies_raw.rename(index=str, columns={"id": "id_tmdb", "imdb_id": "id_imdb"}, inplace=True)
    # VNESENO
    #for key, value in split_array_tables['movies_production_companies'].iterrows():
      #  dbinstance.recieve_dataobject(dt.DataType.PRODUCIRA.value, value)
    # VNESENO
    # for key, value in split_json_tables['belongs_to_collection'].iterrows():
    #     dbinstance.recieve_dataobject(dt.DataType.COLLECTION.value, value)
    # VNESENO
    #for index, row in movies_raw.iterrows():
    #   dbinstance.recieve_dataobject(dt.DataType.MOVIE.value, row)

    # VNESENO
    #for index, row in split_array_tables['genres'].iterrows():
     #   dbinstance.recieve_dataobject(dt.DataType.GENRES.value, row)
    # VNESENO
    #for index, row in split_array_tables['production_companies'].iterrows():
     #   dbinstance.recieve_dataobject(dt.DataType.PRODUCTION_COMPANY.value, row)

    #for index, row in split_array_tables['production_countries'].iterrows():
     #   dbinstance.recieve_dataobject(dt.DataType.PRODUCTION_COUNTRY.value, row)
        # VNESENO
    #for index, row in split_array_tables['spoken_languages'].iterrows():
        #dbinstance.recieve_dataobject(dt.DataType.SPOKEN_LANGUAGES.value, row)
    # VNESENO
  #  for index,row in split_array_tables['movies_spoken_languages'].iterrows():
    #    dbinstance.recieve_dataobject(dt.DataType.SPOKEN_LANGUAGES_JEZIKU.value, row)
    # VNESENO
   # for index,row  in split_array_tables['movies_genres'].iterrows():
    #    dbinstance.recieve_dataobject(dt.DataType.VSTILU.value, row)
    # VNESENO
    #for key,value in split_array_tables['movies_production_countries'].iterrows():
     #   dbinstance.recieve_dataobject(dt.DataType.PRODUCIRA.value, value)


 #   for key, value in split_array_tables['movies_production_countries'].iterrows():
#        dbinstance.recieve_dataobject(dt.DataType.PRODUCIRA_COUNTRY.value, value)


def read_keywords(dbinstance):
    keywords_raw = read_file(KEYWORDS_FILEPATH, keywords_inner_cols)
    split_array_tables = split_array_columns(keywords_raw, "id", keywords_array_cols)
    #vneseno
    #for key, value in split_array_tables['keywords'].iterrows():
     #   dbinstance.recieve_dataobject(dt.DataType.KEYWORDS.value, value)
    # vneseno
    #for key,value in split_array_tables['movies_keywords'].iterrows():
     #   dbinstance.recieve_dataobject(dt.DataType.KEYWORDS_VMESNA.value, value)

    #
    # push split_array_tables


def split_credits(data):
    # id_person, name, gender, profile_path, id_tmdb
    people = []
    # id_credit, order, character, !id_tmdb, !id_person
    cast = []
    # id_credit, job, department, !id_tmdb, !id_person
    crew = []

    for i, cast_array in enumerate(data["cast"]):
        tmdb_id = data["id"][i]
        for cast_obj in cast_array:
            people.append({"id_person": cast_obj["id"], "name": cast_obj["name"], "gender": cast_obj["gender"], "profile_path": cast_obj["profile_path"]})
            cast.append({"id_credit": cast_obj["credit_id"], "order": cast_obj["order"], "character": cast_obj["character"], "id_tmdb": tmdb_id, "id_person": cast_obj["id"]})

    for i, crew_array in enumerate(data["crew"]):
        tmdb_id = data["id"][i]
        for crew_obj in crew_array:
            people.append({"id_person": crew_obj["id"], "name": crew_obj["name"], "gender": crew_obj["gender"], "profile_path": crew_obj["profile_path"]})
            crew.append({"id_credit": crew_obj["credit_id"], "job": crew_obj["job"], "department": crew_obj["department"], "id_tmdb": tmdb_id, "id_person": crew_obj["id"]})

    return {"people": pd.DataFrame(people), "cast": pd.DataFrame(cast), "crew": pd.DataFrame(crew)}



# ready for DB
def read_credits(dbinstance):
    credits = read_file(CREDITS_FILEPATH, credits_inner_cols)
    credit_tables = split_credits(credits)
    #VNESENO
    #for key,value in credit_tables['cast'].iterrows():
     #   dbinstance.recieve_dataobject(dt.DataType.CREDITS.value, value)
    #for key, value in credit_tables['crew'].iterrows():
    #    dbinstance.recieve_dataobject(dt.DataType.CREDITS.value, value)
    #vneseno
    #for key, value in credit_tables['cast'].iterrows():
     #   dbinstance.recieve_dataobject(dt.DataType.CAST.value, value)
    #vneseno
    #for key, value in credit_tables['crew'].iterrows():
     #   dbinstance.recieve_dataobject(dt.DataType.CREW.value, value)
    #VNESENO
    #for key,value in credit_tables['people'].iterrows():
       #dbinstance.recieve_dataobject(dt.DataType.PEOPLE.value, value)



# ready for DB
def read_links(dbinstance):
    links = read_file(LINKS_FILEPATH)
    links.rename(index=str, columns={"movieId": "id_movie", "imdbId": "id_imdb", "tmdbId": "id_tmdb"}, inplace=True)
    links_table = {"links": links}
    for key,value in links_table['links'].iterrows():
        dbinstance.recieve_dataobject(dt.DataType.LINKS.value, value)

    # push links_table


# ready for DB
def read_ratings(dbinstance):
    ratings = read_file(RATINGS_FILEPATH)
    ratings.rename(index=str, columns={"userId": "id_user","movieId": "id_movie"}, inplace=True)
    ratings_table = {"ratings": ratings}

    #for key,value in ratings_table['ratings'].iterrows():
      #  dbinstance.recieve_dataobject_with_key(dt.DataType.RATINGS.value,key,value)




if __name__ == "__main__":
    coninstance = db.DB_connector("DSN=MOVIESDB;UID=admin_python;PWD=Python123")
    #read_movies(coninstance)
    #read_keywords(coninstance)
    #read_links(coninstance)
    #read_ratings(coninstance)
    #read_credits(coninstance)
    coninstance.close_connection()

