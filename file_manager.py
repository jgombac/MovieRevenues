import pandas as pd
import ast
from collections import defaultdict

MOVIES_FILEPATH = "movies_demo.csv"
movies_json_cols = ["belongs_to_collection", "genres", "production_companies", "production_countries", "spoken_languages"]

CREDITS_FILEPATH = "credits.csv"
credits_json_cols = ["cast", "crew"]


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
def split_json_cols(data, columns):
    jsons = []
    for column in columns:
        jsons.append(data[column])
        data.drop(column, axis=1, inplace=True)
    return jsons, data


#convert json arrays and jsons to dataframe
def json_to_dataframe(jsons):
    data = defaultdict(list)
    # check if array of jsons
    if isinstance(jsons[0], list):
        for json_array in jsons:
            for json in json_array:
                for key in json:
                    data[key].append(json[key])
    else:
        for json in jsons:
            # if not empty array
            if isinstance(json, dict):
                for key in json:
                    data[key].append(json[key])
    dataframe = pd.DataFrame(data)
    return dataframe


def read_movies():
    movies_raw = read_file(MOVIES_FILEPATH, movies_json_cols)
    movies_array_cols = [("genres", "id", "movies", "tmdb"), ("production_companies", "id", "movies", "tmdb")]
    split_cols = split_array_columns(movies_raw, "id", movies_array_cols)
    print(split_cols)
    jsons, data = split_json_cols(movies_raw, movies_json_cols)
    json_dict = {column: json_to_dataframe(jsons[i]) for i, column in enumerate(movies_json_cols)}

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
        split_cols[mid_df_name] = mid_df  # vstavi tabelo v slovar {"movies_keywords": vmesna_tabela, "keywords": tabela, ...}
        split_cols[end_df_name] = end_df
    return split_cols



KEYWORDS_FILEPATH = "keywords_demo.csv"


def read_keywords():
    keywords_json_cols = ["keywords"]
    # (koncna_tabela, id_koncne, vmesna_tabela, dodatek_vmesni)
    keywords_array_cols = [("keywords", "id", "movies", "tmdb")]
    keywords_raw = read_file(KEYWORDS_FILEPATH, keywords_json_cols)
    split_cols = split_array_columns(keywords_raw, "id", keywords_array_cols)
    print(split_cols)
    jsons, data = split_json_cols(keywords_raw, keywords_json_cols)
    json_dict = {column: json_to_dataframe(jsons[i]) for i, column in enumerate(keywords_json_cols)}



def read_credits():
    credits = read_file(CREDITS_FILEPATH, credits_json_cols)
    jsons, data = split_json_cols(credits, credits_json_cols)
    json_dict = {column: json_to_dataframe(jsons[i]) for i, column in enumerate(credits_json_cols)}
    print(json_dict["cast"]["name"])

def read_links():
    links = read_file(LINKS_FILEPATH)


def read_ratings():
    ratings = read_file(RATINGS_FILEPATH)


if __name__ == "__main__":
    read_movies()
    #read_keywords()
    #read_credits()
    #read_links()
    #read_ratings()