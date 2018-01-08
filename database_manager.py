#WARNING: vse poizvedbe morajo biti filtrirane na to ali ima filem definiran budget in revenue
# Potrebujejo filter: num_unique, all_keywords, all_actors,
# Ne potrebujejo filtra
# column_types

import pyodbc as po
import pandas as pd
import numpy as np
import time
import tensorflow as tf
from multiprocessing import Queue
import threading
import traceback

class DB(object):

    def __init__(self):
        self.connection = po.connect("DSN=MOVIESLOCAL;UID=root;PWD=")
        self.cur = self.connection.cursor()

    def close(self):
        self.connection.close()

    def movies(self, offset):
        sql = "SELECT distinct ID_TMDB, (ID_COLLECTION IS NOT NULL) AS IN_COLLECTION, ORIGINAL_LANGUAGE, BUDGET, REVENUE FROM movies where REVENUE > 1000 and BUDGET > 1000 limit ?, 10"
        return pd.read_sql(sql, self.connection, params=[offset])

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
                "(select mov.ID_TMDB from movies as mov where mov.ID_TMDB = ?) " +\
                "as mov on (cr.ID_TMDB = mov.ID_TMDB))" +\
                "as cr on (ca.id_credit = cr.ID_CREDIT)) " +\
                "as ca on (ca.ID_PERSON = pl.ID_PERSON)"
        return self.cur.execute(sql, id_tmdb).fetchall()

    def directors(self):
        sql = "select pl.ID_PERSON, pl.NAME from people as pl inner join " +\
                "(select distinct cr.ID_PERSON from crew as ca inner join " +\
                "(select cr.ID_CREDIT, cr.ID_PERSON from credits as cr inner join " +\
                "(select mov.ID_TMDB from movies as mov where mov.REVENUE > 1000 and mov.BUDGET > 1000) " +\
                "as mov on (cr.ID_TMDB = mov.ID_TMDB))" +\
                "as cr on (ca.id_credit = cr.ID_CREDIT) where ca.job = 'Director') " +\
                "as ca on (ca.ID_PERSON = pl.ID_PERSON)"
        return pd.read_sql(sql, self.connection)

    def movie_directors(self, id_tmdb):
        sql = "select pl.ID_PERSON, pl.NAME from people as pl inner join " +\
                "(select distinct cr.ID_PERSON from crew as ca inner join " +\
                "(select cr.ID_CREDIT, cr.ID_PERSON from credits as cr inner join " +\
                "(select mov.ID_TMDB from movies as mov where mov.ID_TMDB = ?) " +\
                "as mov on (cr.ID_TMDB = mov.ID_TMDB))" +\
                "as cr on (ca.id_credit = cr.ID_CREDIT) where ca.job = 'Director') " +\
                "as ca on (ca.ID_PERSON = pl.ID_PERSON)"
        return self.cur.execute(sql, id_tmdb).fetchall()


MOVIES = None
ACTORS = None
DIRECTORS = None

def movies(offset, q):
    db = DB()
    try:
        print("fetch movies")
        movies = db.movies(offset)
        actors = movie_actors(movies)
        #directors = movie_directors(movie_ids)

        db.close()

        cat_act = categorize_actors(actors)
        #rint(cat_act)
        movies.set_index("ID_TMDB", inplace=True)

        joined = pd.concat([movies, cat_act], axis=1, join_axes=[movies.index])
        q.put(joined)
        return
    except:
        db.close()
        print(traceback.print_exc())

def movie_actors(movies):
    db = DB()
    try:
        m_movies = pd.concat([movies[["ID_TMDB"]], pd.DataFrame({"actors":[None]})], axis=1)
        #m_movies["actors"] = pd.Series()
        for i, val in m_movies.iterrows():
            actors = db.movie_actors(val["ID_TMDB"])
            m_movies.ix[i, "actors"] = "|".join([x[0] for x in actors])
        db.close()
        print("act fetched")
        return m_movies
    except:
        db.close()
        print("act err", traceback.print_exc())

def movie_directors(movies):
    db = DB()
    try:
        movies["directors"] = pd.Series()
        for i, val in movies.iteritems():
            directors = db.movie_directors(val)
            movies["directors"][i] = "|".join([x[0] for x in directors])
        db.close()
        print("dir fetched")
    except:
        db.close()
        print("dir err", traceback.print_exc())


def categorize_actors(actors):
    print("cat act")
    data = actors[["ID_TMDB", "actors"]]
    cleaned = data.set_index("ID_TMDB").actors.str.split("|", expand=True).stack()
    cats = pd.get_dummies(cleaned, prefix='a').groupby(level=0).sum()
    return cats
    #print(cats)

def categorize_directors(movies):
    print("cat dir")
    data = movies[["ID_TMDB", "directors"]]
    cleaned = data.set_index("ID_TMDB").directors.str.split("|", expand=True).stack()
    cats = pd.get_dummies(cleaned, prefix='a').groupby(level=0).sum()
    print(cats)

def movie_worker():
    threads = []
    q = Queue()
    for i in range(0, 40, 10):
        t = threading.Thread(target=movies, args=[i, q])
        threads.append(t)
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    movie_data = None
    for i in range(len(threads)):
        result = q.get(i)
        if movie_data is None:
            movie_data = result
        else:
            movie_data = pd.concat([movie_data, result], ignore_index=True)

    movie_data.fillna(value=0.0, inplace=True)
    print(movie_data)


    print("movies fetched")

# def actor_worker():
#     movie_actors()
#     categorize_actors()
#
# def director_worker():
#     movie_directors()
#     categorize_directors()


def init():
    global MOVIES, ACTORS, DIRECTORS
    db = DB()
    try:
        ti = time.time()
        #MOVIES = db.movies()
        #ACTORS = db.actors()
        #DIRECTORS = db.directors()
        #print("MAIN DONE")
        #transform_table(DIRECTORS)

        threads = []
        mt = threading.Thread(target=movie_worker)
        #at = threading.Thread(target=actor_worker)
        #dt = threading.Thread(target=director_worker)
        threads.append(mt)
        #threads.append(at)
        #threads.append(dt)
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        #MOVIES.set_index("ID_TMDB", inplace=True)
        #MOVIES.drop(["actors", "directors"], axis=1, inplace=True)
        #print(MOVIES)

        print((time.time() - ti), "s")
        db.close()
    except Exception as e:
        print(e)
        db.close()

if __name__ == "__main__":
    init()

