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

    def __init__(self, file_name, db, coord_sys):
        self.file_name = file_name
        self.table = self.file_name
        self.db = db
        self.coord_sys = coord_sys

    def run(self):
        # Generate sql file
        SHP2SQL(
            self.file_name,
            self.table,
            self.coord_sys,
        ).run()

        sql = ''' DROP TABLE IF EXISTS {table} '''.format(table=self.table)
        self.db.executeQuery(sql)

        ExecSQL(
            file_name='{file_name}'.format(file_name=self.file_name),
            db=self.db
        ).run()

        return self.table