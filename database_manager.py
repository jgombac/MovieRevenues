#WARNING: vse poizvedbe morajo biti filtrirane na to ali ima filem definiran budget in revenue
# Potrebujejo filter: num_unique, all_keywords, all_actors,
# Ne potrebujejo filtra
# column_types

import pyodbc as po
import pandas as pd
import time
from multiprocessing import Queue
import threading
import traceback

class DB(object):

    def __init__(self):
        self.connection = po.connect("DSN=MOVIESLOCAL;UID=root;PWD=")
        self.cur = self.connection.cursor()

    def close(self):
        self.connection.close()

    def movies(self):
        sql = "SELECT distinct ID_TMDB, (ID_COLLECTION IS NOT NULL) AS IN_COLLECTION, ORIGINAL_LANGUAGE, BUDGET, REVENUE FROM movies where REVENUE > 1000 and BUDGET > 1000"
        return pd.read_sql(sql, self.connection)

    def all(self):
        sql = "SELECT m.* FROM mov_act_dir as m where m.ID_TMDB not in (21525, 41227, 80379)"
        return pd.read_sql(sql, self.connection)

    def actors(self):
        sql = "select pl.ID_PERSON, pl.NAME from people as pl inner join " +\
            "(select distinct cr.ID_PERSON from cast as ca inner join " +\
            "(select cr.ID_CREDIT, cr.ID_PERSON from credits as cr inner join " +\
            "(select mov.ID_TMDB from movies as mov where mov.REVENUE > 1000 and mov.BUDGET > 1000) " +\
            "as mov on (cr.ID_TMDB = mov.ID_TMDB)) as cr on (ca.id_credit = cr.ID_CREDIT)) as ca on (ca.ID_PERSON = pl.ID_PERSON)"
        return pd.read_sql(sql, self.connection)

    def movies_with_actors(self, offset, chunk):
        sql = "select mov_act.ID_TMDB, group_concat(mov_act.ID_PERSON separator '|') as actors from " +\
            "(select pl.ID_TMDB, pl.ID_PERSON from cast as ca inner join " +\
            "(select cr.ID_TMDB, cr.ID_CREDIT, pl.ID_PERSON from people as pl inner join " +\
            "(select mov.ID_TMDB, cr.ID_CREDIT, cr.ID_PERSON from credits as cr inner join " +\
            "(select mov.ID_TMDB from movies as mov where mov.REVENUE > 1000 and mov.BUDGET > 1000) as mov " +\
            "on (mov.ID_TMDB = cr.ID_TMDB)) as cr " +\
            "on (cr.ID_PERSON = pl.ID_PERSON)) as pl " +\
            "on  (pl.ID_CREDIT = ca.id_credit) " +\
            "order by pl.ID_TMDB ASC) as mov_act " +\
            "group by mov_act.ID_TMDB limit ?, ?"
        return pd.read_sql(sql, self.connection, params=[offset, chunk])

    def movies_with_directors(self, offset, chunk):
        sql = "select mov_dir.ID_TMDB, group_concat(mov_dir.ID_PERSON separator '|') as directors from " +\
            "(select pl.ID_TMDB, pl.ID_PERSON from crew as ca inner join " +\
            "(select cr.ID_TMDB, cr.ID_CREDIT, pl.ID_PERSON from people as pl inner join " +\
            "(select mov.ID_TMDB, cr.ID_CREDIT, cr.ID_PERSON from credits as cr inner join " +\
            "(select mov.ID_TMDB from movies as mov where mov.REVENUE > 1000 and mov.BUDGET > 1000) as mov " +\
            "on (mov.ID_TMDB = cr.ID_TMDB)) as cr " +\
            "on (cr.ID_PERSON = pl.ID_PERSON)) as pl " +\
            "on  (pl.ID_CREDIT = ca.id_credit) where ca.job = 'Director' " +\
            "order by pl.ID_TMDB ASC) as mov_dir " +\
            "group by mov_dir.ID_TMDB limit ?, ?"
        return pd.read_sql(sql, self.connection, params=[offset, chunk])


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



def categorize(data, column, q):
    data = data[["ID_TMDB", column]]
    cleaned = data.set_index("ID_TMDB")[column].str.split("|", expand=True).stack()
    cats = pd.get_dummies(cleaned, prefix=column).groupby(level=0).sum()
    q.put(cats)
    return

def movies(q):
    db = DB()
    try:
        result = db.movies()
        db.close()
        result.set_index("ID_TMDB", inplace=True)
        q.put(result)
        return
    except:
        db.close()
        traceback.print_exc()

def movie_worker(queue):
    q = Queue()
    t = threading.Thread(target=movies, args=[q])
    t.start()
    t.join()
    result = q.get(0)
    queue.put(result)
    return

def movies_with_actors(offset, chunk, q):
    db = DB()
    try:
        result = db.movies_with_actors(offset, chunk)
        db.close()
        q.put(result)
        return result
    except:
        db.close()
        print("act err", traceback.print_exc())

def movies_with_directors(offset, chunk, q):
    db = DB()
    try:
        result = db.movies_with_directors(offset, chunk)
        db.close()
        q.put(result)
        return result
    except:
        db.close()
        print("act err", traceback.print_exc())

def actor_worker(queue):
    q = Queue()
    threads = []
    # Fetch results in chunks of 500
    for i in range(0, 5000, 500):
        t = threading.Thread(target=movies_with_actors, args=[i, 500, q])
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print("data fetched")
    q1 = Queue()
    threads1 = []
    for i in range(len(threads)):
        result = q.get(i)
        t = threading.Thread(target=categorize, args=[result, "actors", q1])
        threads1.append(t)

    for t in threads1:
        t.start()
    for t in threads1:
        t.join()

    joined = None
    for i in range(len(threads1)):
        cats = q1.get(i)
        if joined is None:
            joined = cats
        else:
            joined = pd.concat([joined, cats])
    joined.fillna(value=0.0, inplace=True)
    queue.put(joined)
    return

def director_worker(queue):
    q = Queue()
    threads = []
    # Fetch results in chunks of 500
    for i in range(0, 5000, 500):
        t = threading.Thread(target=movies_with_directors, args=[i, 500, q])
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print("data fetched")
    q1 = Queue()
    threads1 = []
    for i in range(len(threads)):
        result = q.get(i)
        t = threading.Thread(target=categorize, args=[result, "directors", q1])
        threads1.append(t)

    for t in threads1:
        t.start()
    for t in threads1:
        t.join()

    joined = None
    for i in range(len(threads1)):
        cats = q1.get(i)
        if joined is None:
            joined = cats
        else:
            joined = pd.concat([joined, cats])
    joined.fillna(value=0.0, inplace=True)
    queue.put(joined)
    return

def init():
    global MOVIES, ACTORS, DIRECTORS
    db = DB()
    try:
        ti = time.time()
        threads = []
        queue = Queue()
        t = threading.Thread(target=movie_worker, args=[queue])
        t1 = threading.Thread(target=actor_worker, args=[queue])
        t2 = threading.Thread(target=director_worker, args=[queue])
        threads.append(t)
        threads.append(t1)
        threads.append(t2)

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        joined = None
        for i in range(len(threads)):
            result = queue.get(i)
            if joined is None:
                joined = result
            else:
                joined = pd.concat([joined, result], axis=1)
        joined.fillna(value=0.0, inplace=True)
        joined.reset_index(inplace=True)
        joined.to_feather("train_save.feather")

        print(joined)

        print((time.time() - ti), "s")
        db.close()
    except Exception as e:
        print(e)
        db.close()

def read_train_data():
    data = pd.read_feather("train_save.feather", nthreads=8)
    return

def prepare_categorical(data, column):
    cleaned = data[column].str.split("|", expand=True).stack()
    cats = pd.get_dummies(cleaned, prefix=column).groupby(level=0).sum()
    return cats

def fetch_data():
    db = DB()
    try:
        result = db.all()
        db.close()
        return result
    except:
        db.close()
        print("act err", traceback.print_exc())

def dummies(v, q):
    q.put(pd.get_dummies(v, columns=["actor", "director"]).groupby(v.index).max())

def prepare_data(data):
    data.set_index("ID_TMDB", inplace=True)

    df_split = {x: data.loc[x] for x in data.index.unique()}

    #threaded
    # q = Queue()
    # threads = []
    # for k, v in df_split.items():
    #     t = threading.Thread(target=dummies, args=[v, q])
    #     t.start()
    #     threads.append(t)
    #
    # for t in threads:
    #     t.join()
    #
    # result = pd.DataFrame()
    # for i in range(len(threads)):
    #     result = pd.concat([result, q.get(i)])

    #non threaded
    # df = df_split[23830]#.to_frame().reset_index()
    # print(type(df), df)
    # return
    print(df_split)
    result = pd.DataFrame()
    for k, v in df_split.items():
        dummies = pd.get_dummies(v, columns=["actor", "director"]).groupby(v.index).max()
        result = pd.concat([result, dummies])

    #result = pd.get_dummies(data, columns=["actor", "director"]).groupby(data.index).max()
    result.fillna(value=0.0, inplace=True)
    result.reset_index(inplace=True)
    print(result)
    result.to_feather("train_data.feather")

if __name__ == "__main__":
    ti = time.time()
    data = fetch_data()
    prepare_data(data)
    print(time.time() - ti, "s")
    #init()
    #read_train_data()

