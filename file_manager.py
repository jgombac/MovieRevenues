import pandas as pd
import ast

MOVIES_FILEPATH = "movies_demo.csv"


def readFiles():
    movies = pd.read_csv(MOVIES_FILEPATH)
    json_cols = ["belongs_to_collection", "genres", "production_companies", "production_countries", "spoken_languages"]
    for column in json_cols:
        for i, x in enumerate(movies[column]):
            if pd.notna(x):
                movies[column][i] = ast.literal_eval(x)
    print(movies)

    #vsak json stolpec locis v samostojno tabelo
    #ga pobrises iz te tabele in vse zapises v bazo
    #tej stolpci so ze transformirani v slovarje





if __name__ == "__main__":
    readFiles()