import luigi
from conversion import HDF2TIF, TIF2SQL
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
    db = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        return PostgresTarget(self.db)
    
    def run(self):
        # Requires file in tif format
        luigi.build([HDF2TIF(file_name=self.file_name, layer_num=1)], local_scheduler=True)
        
        # Requires sql query to insert file
        luigi.build([TIF2SQL(file_name=self.file_name, coord_sys=4326, db=self.db, layer_path='./{}.tif'.format(self.file_name))], local_scheduler=True)
        
        # Insert into PostgreSQL/Postgis db
        with open(self.file_name + '.sql', "r") as file:
            sql = file.read()
            self.output().connect().executeQuery(sql)
