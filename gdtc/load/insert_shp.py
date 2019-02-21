import numpy
import os
import subprocess
import psycopg2

from osgeo import gdal
from osgeo import ogr

from subprocess import call

from ..conversion.shp2sql import SHP2SQL
from ..load.exec_sql import ExecSQL
from ..gdtc_aux.db import Db

class InsertSHP():
    """
    Insert SHP file into postgis db
    """

    def __init__(self, file_name, db, *params):
        self.file_name = file_name
        self.db = db
        self.params = params

    def run(self):
        # Generate sql file
        SHP2SQL(
            self.file_name,
            self.params
        ).run()

        sql = ''' DROP TABLE IF EXISTS {table} '''.format(table=self.file_name)
        self.db.executeQuery(sql)

        ExecSQL(
            file_name='{file_name}'.format(file_name=self.file_name),
            db=self.db
        ).run()

        return Db(
                host=self.db.host,
                port=self.db.port,
                user=self.db.user,
                password=self.db.password,
                database=self.db.database,
                table=self.file_name,
                update_id=self.db.update_id
        )
