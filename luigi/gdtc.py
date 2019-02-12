import luigi
from conversion import HDF2TIF, TIF2SQL
from load import execSQL
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
from config import Config
from gdtc_base_tasks.config_env import ConfigEnv

class insertHDF(luigi.Task):
    task_namespace = 'gdtc'
    file_name = luigi.Parameter()
    db = luigi.Parameter()

    def requires(self):
        return ConfigEnv(self.file_name + '.hdf')
    
    def output(self):
        return PostgresTarget(self.db)

    def run(self):
        yield HDF2TIF(file_name='{luigi_tmp}\\{file_name}'.format(luigi_tmp=Config.LUIGI_TMP, file_name=self.file_name),
                      layer_num="1"), 

        yield TIF2SQL(file_name='{luigi_tmp}\\{file_name}'.format(luigi_tmp=Config.LUIGI_TMP, file_name=self.file_name),
                      coord_sys="4326",
                      db=self.db,
                      layer_path='{luigi_tmp}\\{file_name}'.format(luigi_tmp=Config.LUIGI_TMP, file_name=self.file_name)), 

        yield execSQL(file_name='{luigi_tmp}\\{file_name}'.format(luigi_tmp=Config.LUIGI_TMP, file_name=self.file_name),
                      db=self.db)

    def complete(self):
        if self.output().exists():
            return True

        return False