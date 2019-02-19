import luigi

import numpy
import os
import subprocess
import psycopg2

from osgeo import gdal
from osgeo import ogr

from subprocess import call

from ..conversion.hdf2tif import HDF2TIF
from ..conversion.tif2sql import TIF2SQL
from ..load.exec_sql import execSQL
from ..documents.sql import SQL
from ..targets.postgres_target import PostgresTarget
from ..gdtc_base.db import Db
from ..parameters.db_parameter import DBParameter
from ..gdtc_base.config import Config
from ..gdtc_base.config_env import ConfigEnv
from ..parameters.db_parameter import DBParameter
from ..gdtc_base.sequential_task import sequentialTask

class insertHDF(luigi.Task):
    task_namespace = 'gdtc'
    file_name = luigi.Parameter()
    db = DBParameter()

    def requires(self):
        task_list = [
            ConfigEnv(self.file_name + '.hdf'),
            HDF2TIF(file_name='{luigi_tmp}\\{file_name}'.format(luigi_tmp=Config.LUIGI_TMP, file_name=self.file_name),layer_num="1"), 
            TIF2SQL(file_name='{luigi_tmp}\\{file_name}'.format(luigi_tmp=Config.LUIGI_TMP, file_name=self.file_name),coord_sys="4326", db=self.db, layer_path='{luigi_tmp}\\{file_name}'.format(luigi_tmp=Config.LUIGI_TMP, file_name=self.file_name)), 
            execSQL(file_name='{luigi_tmp}\\{file_name}'.format(luigi_tmp=Config.LUIGI_TMP, file_name=self.file_name),db=self.db)
        ]

        return sequentialTask(task_list = task_list)
    
    def output(self):
        return PostgresTarget(self.db)

    def run(self):
        pass

    def complete(self):
        return self.output().exists()