# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Class: Db
# Description: A Db is a class that encapsulates the parameters of the DB and
#              any information relative to the location and result of data.

import psycopg2

class Db():

    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def connect(self):
        """
        Get a psycopg2 connection object to the database where the table is.
        """

        with psycopg2.connect(
           port = self.port,
           database = self.database,
           user = self.user,
           password = self.password) as connection:
               connection.set_client_encoding('utf-8')
               self.connection = connection

        return self.connection   

    def executeQuery(self, sql):
        with self.connect().cursor() as cur:
            cur.execute(sql)
            self.connection.commit()