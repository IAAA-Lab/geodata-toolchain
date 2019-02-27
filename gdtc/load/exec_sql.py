import numpy
import os
import subprocess
import psycopg2

from ..conversion.hdf2tif import  HDF2TIF
from ..conversion.tif2sql import TIF2SQL
from ..gdtc_aux.db import Db

from osgeo import gdal
from osgeo import ogr

from subprocess import call

# Define Task for inserting HDF file into PostgreSQL DB
class ExecSQL():
    """
    Executes an SQL file into the db
    """
    def __init__(self, file_name, db):
        self.file_name = file_name
        self.db = db

    def run(self):        
        # Insert into PostgreSQL/Postgis db
        with open(self.file_name + '.sql', "r") as file:
            sql = file.read()
            self.db.executeQuery(sql)
