#WARNING: vse poizvedbe morajo biti filtrirane na to ali ima filem definiran budget in revenue
# Potrebujejo filter: num_unique, all_keywords, all_actors,
# Ne potrebujejo filtra
# column_types

import pyodbc as po
import pandas as pd

class DB(object):

    def __init__(self):
        self.connection = po.connect("DSN=MOVIESLOCAL;UID=root;PWD=")
        self.cur = self.connection.cursor()

    def close(self):
        self.connection.close()

    def movies(self):
        sql = "select ID_TMDB, ORIGINAL_TITLE, ORIGINAL_LANGUAGE, ID_COLLECTION, ADULT, BUDGET, REVENUE from movies where BUDGET > 0 and REVENUE > 0"
        return pd.read_sql(sql, self.connection)

    def actors(self):
        sql = "select distinct mov.ID_TMDB, mov.ORIGINAL_TITLE, cr.ID_CREDIT, ca.charact, pl.NAME from movies as mov join credit as cr on (cr.ID_TMDB = mov.ID_TMDB)"+\
              " join cast as ca on (ca.id_credit = cr.ID_CREDIT) join people as pl on (pl.ID_PERSON = cr.ID_PERSON) where mov.REVENUE > 0 and mov.BUDGET > 0"
        return pd.read_sql(sql, self.connection)

    def directors(self):
        sql = "select distinct mov.ID_TMDB, mov.ORIGINAL_TITLE, cr.ID_CREDIT, ca.job, pl.NAME from movies as mov join credit as"+\
              " cr on (cr.ID_TMDB = mov.ID_TMDB) join crew as ca on (ca.id_credit = cr.ID_CREDIT) join people as pl on (pl.ID_PERSON = cr.ID_PERSON)"+\
              " where mov.REVENUE > 0 and mov.BUDGET > 0 and ca.job = 'Director'"
        return pd.read_sql(sql, self.connection)


if __name__ == "__main__":
    db = DB()
    movies = db.movies()
    print(movies)

    db.close()

# stevilo unikatnih vrstic v podanih stolpcih in tabelah
# vhod: dict {"ime_tabele1": ["stolpec1", "stolpec2"], "ime_tabele2": ["stolpec1"]}
# izhod: int
def num_unique(tables, columns):
    pass

# slovar imen stolpcev in pripadajoci podatkovni tipi vseh tabel
# vhod: /
# izhod: dict { "ime_tabele1": [["ime_stolpca1", "integer"], ["ime_stolpca2", "string"]], ...}
def column_types():
    pass


# seznam vseh igralcev
# vhod: /
# izhod: list [{id_igralca1: "ime_igralca1"}, {id_igralca2: "ime_igralca2"}]
def all_actors():
    pass

# seznam vseh kljucnih besed
# vhod: /
# izhod: list [{id_KB1: "besedilo_KB1"}, {id_KB2: "besedilo_KB2"}]
def all_keywords():
    pass




