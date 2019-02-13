import luigi

import numpy
import os
import subprocess
import psycopg2
import uuid

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

class sequentialTask(luigi.Task):
    task_namespace = 'gdtc'

    task_id = str(uuid.uuid4())
    task_list = luigi.Parameter()

    def requires(self):
        *pending_tasks, self.to_run = list(self.task_list)

        if pending_tasks != []:
            return sequentialTask(task_list = pending_tasks)

    def output(self):
        return luigi.LocalTarget('{}\\{}.txt'.format(Config.LUIGI_TMP, self.task_id))

    def run(self):
        yield self.to_run
        open('{}\\{}.txt'.format(Config.LUIGI_TMP, self.task_id), 'w+')
