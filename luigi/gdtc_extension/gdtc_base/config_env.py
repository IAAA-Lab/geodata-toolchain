import luigi

import numpy
import os
import subprocess
import psycopg2
import uuid
import shutil

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

        
