#WARNING: vse poizvedbe morajo biti filtrirane na to ali ima filem definiran budget in revenue
# Potrebujejo filter: num_unique, all_keywords, all_actors,
# Ne potrebujejo filtra
# column_types

import pyodbc as po
import pandas as pd
import time
import threading

class DB(object):

    def __init__(self):
        self.connection = po.connect("DSN=MOVIESLOCAL;UID=root;PWD=")
        self.cur = self.connection.cursor()

    def close(self):
        self.connection.close()

    def movies(self):
        sql = "SELECT distinct ID_TMDB, (ID_COLLECTION IS NOT NULL) AS IN_COLLECTION, ORIGINAL_LANGUAGE, BUDGET, REVENUE FROM movies where REVENUE > 1000 and BUDGET > 1000"
        return pd.read_sql(sql, self.connection)

    def actors(self):
        sql = "select pl.ID_PERSON, pl.NAME from people as pl inner join " +\
            "(select distinct cr.ID_PERSON from cast as ca inner join " +\
            "(select cr.ID_CREDIT, cr.ID_PERSON from credits as cr inner join " +\
            "(select mov.ID_TMDB from movies as mov where mov.REVENUE > 1000 and mov.BUDGET > 1000) " +\
            "as mov on (cr.ID_TMDB = mov.ID_TMDB)) as cr on (ca.id_credit = cr.ID_CREDIT)) as ca on (ca.ID_PERSON = pl.ID_PERSON)"
        return pd.read_sql(sql, self.connection)

    def movie_actors(self, id_tmdb):
        sql = "select pl.ID_PERSON, pl.NAME from people as pl inner join " +\
                "(select distinct cr.ID_PERSON from cast as ca inner join " +\
                "(select cr.ID_CREDIT, cr.ID_PERSON from credits as cr inner join " +\
                "(select mov.ID_TMDB from movies as mov where mov.ID_TMDB = "+id_tmdb+") " +\
                "as mov on (cr.ID_TMDB = mov.ID_TMDB))" +\
                "as cr on (ca.id_credit = cr.ID_CREDIT)) " +\
                "as ca on (ca.ID_PERSON = pl.ID_PERSON)"
        return pd.read_sql(sql, self.connection)

    def directors(self):
        sql = "select pl.ID_PERSON, pl.NAME from people as pl inner join " +\
                "(select distinct cr.ID_PERSON from crew as ca inner join " +\
                "(select cr.ID_CREDIT, cr.ID_PERSON from credits as cr inner join " +\
                "(select mov.ID_TMDB from movies as mov where mov.REVENUE > 1000 and mov.BUDGET > 1000) " +\
                "as mov on (cr.ID_TMDB = mov.ID_TMDB))" +\
                "as cr on (ca.id_credit = cr.ID_CREDIT) where ca.job = 'Director') " +\
                "as ca on (ca.ID_PERSON = pl.ID_PERSON)"
        return pd.read_sql(sql, self.connection)

    def movie_director(self, id_tmdb):
        sql = "select pl.ID_PERSON, pl.NAME from people as pl inner join " +\
                "(select distinct cr.ID_PERSON from crew as ca inner join " +\
                "(select cr.ID_CREDIT, cr.ID_PERSON from credits as cr inner join " +\
                "(select mov.ID_TMDB from movies as mov where mov.ID_TMDB = "+id_tmdb+") " +\
                "as mov on (cr.ID_TMDB = mov.ID_TMDB))" +\
                "as cr on (ca.id_credit = cr.ID_CREDIT) where ca.job = 'Director') " +\
                "as ca on (ca.ID_PERSON = pl.ID_PERSON) limit 1"
        return pd.read_sql(sql, self.connection)


MOVIES = None
ACTORS = None
DIRECTORS = None


def movie_actors():
    print("act start")
    db = DB()
    try:
        for i, val in MOVIES.iterrows():
            if i > 10:
                db.close()
                print("act end")
                return
            actors = db.movie_actors(str(val["ID_TMDB"]))
    except:
        db.close()
        print("act err")

def movie_directors():
    print("dir start")
    db = DB()
    try:
        for i, val in MOVIES.iterrows():
            if i > 10:
                db.close()
                print("dir end")
                return
            director = db.movie_director(str(val["ID_TMDB"]))
    except:
        db.close()
        print("dir err")

def init():
    global MOVIES, ACTORS, DIRECTORS
    db = DB()
    try:
        t = time.time()
        MOVIES = db.movies()
        #ACTORS = db.actors()
        #DIRECTORS = db.directors()
        print("MAIN DONE")

        at = threading.Thread(target=movie_actors)
        dt = threading.Thread(target=movie_directors)
        at.start()
        dt.start()
        print((time.time() - t), "s")
        db.close()
    except Exception as e:
        print(e)
        db.close()

if __name__ == "__main__":
    init()




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




