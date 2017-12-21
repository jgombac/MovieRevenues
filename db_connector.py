import pyodbc as po
import pandas as pan
import data_types as dt
import time
class DB_connector:

    def __init__(self,conns):
        self.constring = conns
        self.connection = po.connect(self.constring)
        self.curs = self.connection.cursor()

    def close_connection(self):
        self.connection.close()
    def parse_date(self,datestring):
        if isinstance(datestring, str):
            i = datestring.split('-')[::-1]
            tmp = i[1]
            i[1] = i[2]
            i[2] = tmp
            return ''.join(i)
        return None

    def recieve_dataobject(self,type,obj):
            if type == 1:
                id = obj['id_tmdb']
                belongs = None if pan.isnull(obj['belongs_to_collection']) else obj['belongs_to_collection']
                adult = 1 if obj['adult'] == "TRUE" else 0
                budget = None if pan.isnull(obj['budget']) or not isinstance(obj["budget"], int) else int(obj['budget'])
                homepage = None if pan.isnull(obj['homepage']) else obj['homepage']
                imdb = None if pan.isnull(obj['id_imdb']) else obj['id_imdb']
                original_l = None if pan.isnull(obj['original_language']) else obj['original_language']
                original_t = None if pan.isnull(obj['original_title']) else obj['original_title']
                overv =  None if pan.isnull(obj['overview']) else obj['overview']
                popularity = None if pan.isnull(obj['popularity']) else obj['popularity']
                poster_path = None if pan.isnull(obj['poster_path']) else obj['poster_path']
                # release = self.parse_date(obj['release_date'])
                release = obj['release_date'] if isinstance(obj["release_date"], str) else None
                revenue = None if pan.isnull(obj['revenue']) else obj['revenue']
                runtime = None if pan.isnull(obj['runtime']) else obj['runtime']
                status = None if pan.isnull(obj['status']) else obj['status']
                tagline = None if pan.isnull(obj['tagline']) else obj['tagline']
                vid = 0 if (pan.isnull(obj['video']) or obj['video'] == "FALSE") else 1
                vote_avg = None if pan.isnull(obj['vote_average']) else float(obj['vote_average'])
                vote_count = None if pan.isnull(obj['vote_count']) else int(obj['vote_count'])
                data = (id,belongs,adult,budget,homepage,imdb,original_l,original_t,overv,popularity,poster_path,release,
                        revenue,runtime,status,tagline,vid,vote_avg,vote_count)
                #print(data)
                self.insert_db_mov(data)
            elif type == 2:
                backdrop = obj['backdrop_path']
                id = obj['id']
                name = obj['name']
                poster_path = obj['poster_path']
                data = (id,name,poster_path,backdrop)
                self.insert_db_coll(data)
            elif type == 3.1:
                id = int(obj['id_keywords'])
                id_mov = int(obj['id_tmdb'])
                data = (id,id_mov)
                self.insert_db_keywords_vmesna(data)
            elif type == 3.2:
                id = int(obj['id'])
                name = obj['name']
                data = (id, name)
                self.insert_db_keywords(data)
            elif type == 4:
                id_mov = int(obj['id_movie'])
                id_imdb = int(obj['id_tmdb'])
                id_tmdb = int(obj['id_imdb'])
                data = (id_mov, id_imdb,id_tmdb)
                self.insert_db_links(data)
            elif type == 6:
                id_gen= int(obj['id_genres'])
                gen_name = obj['name']
                data = (id_gen,gen_name)
                self.insert_db_genres(data)
            elif type == 7:
                 id_prod = int(obj['id_production_companies'])
                 prod_name = obj['name']
                 data = (id_prod, prod_name)
                 self.insert_db_production_companies(data)
            elif type == 8:
                try:
                    prod = obj['id_production_countries']
                    name = obj['name']
                    data = (prod, name)
                    self.insert_db_production_country(data)
                except Exception as e:
                    print("Parsing error", e)
            elif type == 9:
                gender = int(obj['gender'])
                id_person = int(obj['id_person'])
                prof = obj['profile_path']
                name = obj['name']
                dat = (id_person,name,gender,prof)
                self.insert_db_production_people(dat)
            elif type == 10:
                try:
                    id  = obj['id_spoken_languages']
                    name = obj['name']
                    data = (id, name)
                    self.insert_db_spoken_languages(data)
                except Exception as e:
                    print("Parsing error", e)
            elif type == 13:
                try:
                    id = int(obj['id_tmdb'])
                    name = obj['id_spoken_languages']
                    data = (id, name)
                    self.insert_db_spoken_languages_vmesna(data)
                except Exception as e:
                    print("Parsing error", e)
            elif type == 11:
                try:
                    id_prod = int(obj['id_production_companies'])
                    id_tmdb = int(obj['id_tmdb'])
                    data =(id_tmdb,id_prod)
                    self.insert_db_producira(data)
                except Exception as e:
                    print("Error parsing values", e)
            elif type == 12:
                try:
                    id_tmdb = int(obj['id_tmdb'])
                    id_production = obj['id_production_countries']
                    data = (id_tmdb,id_production)
                    self.insert_db_producira_country(data)
                except Exception as e:
                    print("Error parsing values", e)
            elif type == 14:
                try:
                    id_tmdb = int(obj['id_tmdb'])
                    genres = int(obj['id_genres'])
                    data = (id_tmdb, genres)
                    self.insert_genres(data)
                except Exception as e:
                    print("Error parsing values", e)
            elif type == 15:
                id_tmdb = int(obj['id_tmdb'])
                id_person = int(obj['id_person'])
                id_cred = obj['id_credit']
                data = (id_tmdb, id_person,id_cred)
                self.insert_db_credits(data)
            elif type == 16:
                id_person = obj['id_person']
                id_cred = obj['id_credit']
                chara = obj['character']
                order = obj['order']
                data = (order,chara,id_person)
                self.insert_db_cast(data)
            elif type == 17:
                department = obj['department']
                job = obj['job']
                id_person = obj['id_person']
                data = (job, department, id_person)
                self.insert_db_crew(data)



    def recieve_dataobject_with_key(self,type,key,obj):
        if type == 5:
            id_usr = int(obj['id_user'])
            id_mov = int(obj['id_movie'])
            id_rating = int(obj['rating'])
            timestamp = obj['timestamp']
            timestamp_real = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(timestamp )))
            data = (key,id_usr, id_mov, id_rating, timestamp_real)
            self.insert_db_ratings(data)

    def insert_db_keywords(self,data):
        self.curs.execute("insert ignore into KEYWORDS(ID_KEYWORD,NAME) values (?,?)", data)
        self.curs.commit()


    def insert_db_keywords_vmesna(self,data):
        self.curs.execute("insert ignore into POD_OZNAKO(ID_KEYWORD,ID_TMDB) values (?,?)", data)
        self.curs.commit()

    def insert_db_mov(self,data):
        try:
            self.curs.execute('insert ignore into MOVIES(ID_TMDB,ID_COLLECTION,ADULT,BUDGET,HOMEPAGE,IMDB_ID,ORIGINAL_LANGUAGE,'
                              'ORIGINAL_TITLE,OVERVIEW,POPULARITY,POSTER_PATH,RELEASE_DATE,REVENUE,RUNTIME,STATUS,TAGLINE,'
                              'VIDEO,VOTE_AVERAGE,VOTE_COUNT)'
                              'values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);',data)
            self.curs.commit()
        except:
            print("ERROR INSERTING", data)

    def insert_db_coll(self,data):
        self.curs.execute("insert ignore into COLLECTIONS(ID_COLLECTION,NAME,POSTER_PATH,BACKDROP_PATH) values (?,?,?,?);", data)
        self.curs.commit()

    def insert_db_links(self, data):
        self.curs.execute("insert into LINKS(ID_MOVIE,ID_TMDB,ID_IMDB) values (?,?,?);",
                          data)
        self.curs.commit()

    def insert_db_ratings(self,data):
        self.curs.execute("insert into RATINGS(ID_RATING,ID_MOVIE,ID_USER,RATING,TIMESTAMP) values (?,?,?,?,?);",
                          data)
        self.curs.commit()

    def insert_db_genres(self,data):
        self.curs.execute("insert ignore into GENRES(ID_GENRE,NAME) values (?,?);",
                          data)
        self.curs.commit()

    def insert_db_production_companies(self, data):
        try:
            self.curs.execute("insert ignore into PRODUCTION_COMPANIES(ID_PRODUCTION_COMPANY,NAME) values (?,?);",
                              data)
            self.curs.commit()
        except Exception as e:
            print("SQL error", e)

    def insert_db_production_country(self, data):
        try:
            self.curs.execute("insert ignore into PRODUCTION_COUNTRIES(ISO_3166_1,NAME) values (?,?);",
                              data)
            self.curs.commit()
        except Exception as e:
            print("SQL error", e)

    def insert_db_production_people(self, data):
        self.curs.execute("insert ignore into PEOPLE(ID_PERSON,NAME,GENDER,PROFILE_PATH) values (?,?,?,?);",
                          data)
        self.curs.commit()

    def insert_db_spoken_languages(self, data):
        try:
            self.curs.execute("insert ignore into SPOKEN_LANGUAGES (ISO_639_1,NAME) values (?,?);",
                              data)
            self.curs.commit()
        except Exception as e:
            print("SQL error", e)

    def insert_db_spoken_languages_vmesna(self, data):
        try:
            self.curs.execute("insert ignore into V_JEZIKU (ID_TMDB,ISO_639_1) values (?,?);",
                                  data)
            self.curs.commit()
        except Exception as e:
            print("SQL error", e)

    def insert_db_producira(self, data):
        self.curs.execute("insert into PRODUCIRA (ID_TMDB,ID_PRODUCTION_COMPANY) values (?,?);",
                              data)

        self.curs.commit()
    def insert_db_producira_country(self, data):
        try:
            self.curs.execute("insert into PRODUCIRANO_V (ID_TMDB,ISO_3166_1) values (?,?);",
                                  data)
            self.curs.commit()
        except Exception as e:
            print("SQL error", e)
    def insert_genres(self, data):
        try:
            self.curs.execute("insert ignore into V_STILU (ID_TMDB,ID_GENRE) values (?,?);",
                              data)
            self.curs.commit()
        except Exception as e:
            print("SQL ERROR", e)
    def insert_db_credits(self, data):
        self.curs.execute("insert into CREDIT (ID_TMDB,ID_PERSON,ID_CREDIT) values (?,?,?);",
                          data)
        self.curs.commit()

    def insert_db_cast(self, data):
        self.curs.execute("insert into CAST (ordr,charact,id_person) values (?,?,?);",
                          data)
        self.curs.commit()
    def insert_db_crew(self, data):
        self.curs.execute("insert ignore into CREW (job,department,id_person) values (?,?,?);",
                          data)
        self.curs.commit()

    def is_valid(self,obj,type):
        return isinstance(obj,pan.Series) and isinstance(type,int)
