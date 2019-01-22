# Author: Víctor Fernández Melic
# Project: IAAA GeoData Toolchain
# Class: Db
# Description: A Db is a class that encapsulates the parameters of the DB and
#              any information relative to the location and result of data.

import psycopg2

class Db():

    def __init__(self, host, port, user, password, database, table, update_id):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.table = table
        self.update_id = update_id

    def __str__(self):
        return "BD object(%s,%s,%s,%s,%s,%s,%s)"%(self.host, self.port, self.user, self.password, self.database, self.table, self.update_id)