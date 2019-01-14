import luigi
from conversion import HDF2TIF
from PostgresTarget import PostgresTarget
from DB import Db
from osgeo import gdal
from osgeo import ogr
import numpy
import os
import subprocess
import psycopg2
from subprocess import call

# Define Task for inserting HDF file into PostgreSQL DB
class insertHDF(luigi.Task):
    """
    Inserts an HDF file into the target Db
    """

    task_namespace = 'load'
    file_name = luigi.Parameter()

    def requires(self):
        # Requires file in tif format
        return HDF2TIF(file_name=self.file_name, layer_num=1)

    def output(self):
        db = Db('127.0.0.1', '8432', 'postgres', 'geodatatoolchainps', 'postgres', 'test', '14')
        return PostgresTarget(db)
    
    def run(self):
        sql = """SELECT * FROM table_updates;"""
        print(self.output().connect().executeQuery(sql).result)
