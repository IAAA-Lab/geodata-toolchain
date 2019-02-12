import luigi

import numpy
import os
import psycopg2
import subprocess

from osgeo import gdal
from osgeo import ogr

from ..documents.tif import TIF
from ..documents.sql import SQL
from ..gdtc_base.db import Db
from ..parameters.db_parameter import DBParameter

class TIF2SQL(luigi.Task):
    """
    Generates sql query to insert into postgis db from tif file
    """
    task_namespace = 'document'

    file_name = luigi.Parameter()
    layer_path = luigi.Parameter()
    coord_sys = luigi.Parameter()
    db = DBParameter()

    def requires(self):
        return TIF(self.file_name)

    def output(self):
        return luigi.LocalTarget('{file_name}.sql'.format(file_name=self.file_name))
    
    def run(self):
        # Generate sql file
        cmd = 'raster2pgsql -I -C -s {} \"{}.tif\" -F -d {} > \"{}.sql\"'.format(self.coord_sys, self.layer_path, self.db.table, self.file_name)
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
