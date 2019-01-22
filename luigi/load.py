import luigi
from conversion import HDF2TIF, TIF2SQL
from documents import SQL
from PostgresTarget import PostgresTarget
from DB import Db
from osgeo import gdal
from osgeo import ogr
import numpy
import os
import subprocess
import psycopg2
from subprocess import call

from DBParameter import DBParameter

# Define Task for inserting HDF file into PostgreSQL DB
class execSQL(luigi.Task):
    """
    Inserts an HDF file into the target Db
    """

    task_namespace = 'load'

    file_name = luigi.Parameter()
    db = DBParameter()

    def requires(self):
        return SQL(self.file_name)

    def output(self):
        return PostgresTarget(self.db)
    
    def run(self):        
        # Insert into PostgreSQL/Postgis db
        with open(self.file_name + '.sql', "r") as file:
            sql = file.read()
            self.output().connect().executeQuery(sql)
            