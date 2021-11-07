# Project Athena
'''
Owner: Efstratios Pahis
Contributors:
Description:
'''

import pymysql
import os
import sqlalchemy
import Utils.Settings as st


class DataBaseUtils:

    # Constructor: Globally defines the relevant paths for Athena Analytics
    def __init__(self):
        self.app_data_local_path = os.path.join(os.path.expanduser('~'),
                                                'AppData\\Local\\AthenaAnalytics')
        self.db_local_path = os.path.join(
            self.app_data_local_path, '\\LocalStorage')
        self.db_local = os.path.join(self.db_local_path, "\\AthenaLocalDB.db")

    # Creates all local Files needed for AthenaAnalytics
    # Directory for local sqlite3 file
    def create_local_folders(self):
        if not os.path.exists(self.db_local_path):
            # Create directory to store local Database
            os.makedirs(self.db_local_path)

    # Creates mysql engine to pass as parameter for eample dataframe.to_sql()'''
    def create_db_engine(self, local: bool):
        if local:
            db_engine = self.db_local
        else:
            db_engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                                                 .format(user=st.ATHENA_CLOUD_DB_USER,
                                                         pw=st.ATHENA_CLOUD_DB_PW,
                                                         host=st.ATHENA_CLOUD_DB_HOST,
                                                         db=st.ATHENA_CLOUD_DB_DBNAME))
        return db_engine

    # Returns the a connection object for the local AthenaAnalytics DB'''
    def __connect_local_db(self):
        conn = None
        try:
            #conn = sqlite3.connect(self.db_local)
            print('e')

            return conn
        except:
            print('e')
        finally:
            if conn:
                conn.close()

    # Returns the a connection object for the cloud AthenaAnalytics DB'''
    def __connect_cloud_db(self):
        # "mysql.strpah.dreamhosters.com" #os.environ[SQL_HOST]
        host = st.ATHENA_CLOUD_DB_HOST
        # "ep_athena_admin" #os.environ[SQL_USER]
        user = st.ATHENA_CLOUD_DB_USER
        # ProjectAthena!?.12345 #os.environ[SQL_PASS]
        passwd = st.ATHENA_CLOUD_DB_PW
        # "athenadatabase" #os.environ[SQL_DB]
        database = st.ATHENA_CLOUD_DB_DBNAME
        db = pymysql.connect(host, user, passwd, database)
        return db

    # Executes SQL Statements Tables based on passed SQL Statement for the cloud AthenaAnalytics DB.
    # In case User wants to read data from table, she can use fetchone parameter to fetch a single row
    # of the db or the fetchall parameter to fetch all rows of the db
    def execute_sql(self, sql_statement, local: bool, fetchone=False, fetchall=False):
        if local:
            db = self.__connect_local_db(DataBaseUtils)
        else:
            db = self.__connect_cloud_db(DataBaseUtils)
        try:
            with db.cursor() as cursor:
                cursor.execute(sql_statement)
                db.commit()
                if fetchone:
                    return cursor.fetchone()
                elif fetchall:
                    return cursor.fetchall()
            return True
        except TypeError as e:
            if local:
                e = 'sqlite3.Error'
            else:
                #e = pymysql.Error
                print('test')
            print(e)
            return False
        finally:
            db.close()
