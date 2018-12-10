# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain

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
        try:
            self.conn = psycopg2.connect(host=self.host, port=self.port, database=self.db, user=self.user, password=self.password)
        except:
            print('Error trying to connect to {} on {}'.format(self.db, self.user + ':' + self.password + '@' + self.host + ':' + self.port))

    def execQuery(self, sql_query):       
        # Execute query
        cur = self.conn.cursor()
        cur.execute(sql_query)
        
        if cur.description == None:
            return None
        else:
            return cur.fetchall()        

    def closeDB(self):
        # Close connection with DB
        self.conn.cursor().close()
