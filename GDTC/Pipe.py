# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Class: Pipe
# Description: A pipe is a class that encapsulates the parameters of the DB and
#              any information relative to the location and result of data.

import psycopg2

class Pipe():

    def __init__(self, host, port, user, password, db, table):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.table = table

    def connectDB(self):

        # Connect with db
        with psycopg2.connect(host=self.host, port=self.port, database=self.db, user=self.user, password=self.password) as conn:
            self.conn = conn

    def execQuery(self, sql_query):       
        # Execute query
        with self.conn.cursor() as cur:
            cur.execute(sql_query)
        
            if cur.description == None:
                return None
            else:
                return cur.fetchall()        

    def closeDB(self):
        # Close connection with DB
        with self.conn as conn:
            conn.close()
            
