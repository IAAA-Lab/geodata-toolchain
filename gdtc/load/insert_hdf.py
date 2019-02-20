import numpy
import os
import subprocess
import psycopg2

from osgeo import gdal
from osgeo import ogr

from subprocess import call

from ..conversion.hdf2tif import HDF2TIF
from ..conversion.tif2sql import TIF2SQL
from ..load.exec_sql import ExecSQL
from ..gdtc_aux.db import Db

class InsertHDF():

    def __init__(self, file_name, coord_sys, db):
        self.file_name = file_name
        self.coord_sys = coord_sys
        self.db = db

    def run(self):

        HDF2TIF(
            file_name='{file_name}'.format(file_name=self.file_name),
            layer_num="1"
            ).run() 

        TIF2SQL(
            file_name='{file_name}'.format(file_name=self.file_name),
            coord_sys=self.coord_sys, db=self.db, layer_path='{file_name}'.format(file_name=self.file_name)
            ).run()

        ExecSQL(
            file_name='{file_name}'.format(file_name=self.file_name),
            db=self.db
            ).run()
