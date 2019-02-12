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
from gdtc_base_tasks import config_env
import uuid
import shutil

class ConfigEnv(luigi.Task):
    task_namespace = 'config'

    file_name = luigi.Parameter()
    task_id = str(uuid.uuid4())

    def requires(self):
        pass
    
    def output(self):
        return luigi.LocalTarget('{luigi_tmp}\\{file_name}'.format(luigi_tmp=Config.LUIGI_TMP, file_name=self.file_name))

    def run(self):
        dst = '{luigi_tmp}\\'.format(luigi_tmp=Config.LUIGI_TMP)
        src = Config.LUIGI_BASE + '\\' + self.file_name
        
        if not os.path.exists(dst):
            os.makedirs(dst)

        shutil.copy(src, dst)

        
