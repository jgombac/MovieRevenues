import pandas as pd
import ast

MOVIES_FILEPATH = "movies_demo.csv"
movies_json_cols = ["belongs_to_collection", "genres", "production_companies", "production_countries", "spoken_languages"]

CREDITS_FILEPATH = "credits_demo.csv"
credits_json_cols = ["cast", "crew"]

KEYWORDS_FILEPATH = "keywords_demo.csv"
keywords_json_cols = ["keywords"]


def read_file(filename, json_cols):
    file = pd.read_csv(filename)
    for column in json_cols:
        for i, x in enumerate(file[column]):
            if pd.notna(x):
                file[column][i] = ast.literal_eval(x)
    print(file)


if __name__ == "__main__":
    read_file(KEYWORDS_FILEPATH, keywords_json_cols)
    read_file(CREDITS_FILEPATH, credits_json_cols)
    read_file(MOVIES_FILEPATH, movies_json_cols)