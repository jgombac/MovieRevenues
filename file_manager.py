import pandas as pd
import ast
from collections import defaultdict

MOVIES_FILEPATH = "movies_demo.csv"
movies_json_cols = ["belongs_to_collection", "genres", "production_companies", "production_countries", "spoken_languages"]

CREDITS_FILEPATH = "credits_demo.csv"
credits_json_cols = ["cast", "crew"]

KEYWORDS_FILEPATH = "keywords_demo.csv"
keywords_json_cols = ["keywords"]

LINKS_FILEPATH = "links_demo.csv"

RATINGS_FILEPATH = "ratings_demo.csv"


def read_file(filename, json_cols=list()):
    data = pd.read_csv(filename)
    for column in json_cols:
        for i, x in enumerate(data[column]):
            if pd.notna(x):
                data[column][i] = ast.literal_eval(x)
    return data


#split json columns from elemental columns
def split_columns(data, columns):
    jsons = []
    for column in columns:
        jsons.append(data[column])
        data.drop(column, axis=1, inplace=True)
    return jsons, data




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
    movies = read_file(MOVIES_FILEPATH, movies_json_cols)
    jsons, data = split_columns(movies, movies_json_cols)
    json_dict = {column: json_to_dataframe(jsons[i]) for i, column in enumerate(movies_json_cols)}


def read_keywords():
    keywords = read_file(KEYWORDS_FILEPATH, keywords_json_cols)
    jsons, data = split_columns(keywords, keywords_json_cols)
    json_dict = {column: json_to_dataframe(jsons[i]) for i, column in enumerate(keywords_json_cols)}


def read_credits():
    credits = read_file(CREDITS_FILEPATH, credits_json_cols)
    jsons, data = split_columns(credits, credits_json_cols)
    json_dict = {column: json_to_dataframe(jsons[i]) for i, column in enumerate(credits_json_cols)}


def read_links():
    links = read_file(LINKS_FILEPATH)

def read_ratings():
    ratings = read_file(RATINGS_FILEPATH)


if __name__ == "__main__":
    read_movies()
    read_keywords()
    read_credits()
    read_links()
    read_ratings()