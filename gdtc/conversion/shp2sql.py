import numpy
import os
import subprocess
import psycopg2

from osgeo import gdal
from osgeo import ogr

from subprocess import call

from ..gdtc_aux.config import Config

class SHP2SQL():
    """
    Generates sql query to insert into postgis db from shp file
    """

    def __init__(self, file_name, *params):
        self.file_name = file_name
        self.params = params

    def run(self):
        # Generate sql file
        cmd = 'shp2pgsql -s 4269 -g geom_4269 -I -W \"latin1\" \"{file_name}\" > {file_name2}.sql'.format(file_name=self.file_name, file_name2=self.file_name)
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
