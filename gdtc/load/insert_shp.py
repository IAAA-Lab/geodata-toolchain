import numpy
import os
import subprocess
import psycopg2

from osgeo import gdal
from osgeo import ogr

from subprocess import call

from ..conversion.shp2sql import SHP2SQL
from ..load.exec_sql import ExecSQL

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

        ExecSQL(
            file_name='{file_name}'.format(file_name=self.file_name),
            db=self.db
            ).run()
