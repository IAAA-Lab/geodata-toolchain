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

    def __init__(self, file_name, coord_sys, *params):
        self.file_name = file_name
        self.coord_sys = coord_sys
        self.params = params

    def run(self):
        # Generate sql file
        cmd = 'shp2pgsql -c -s {coord_sys} -g {field_name} -I -W \"latin1\" \"{file_name}\" > {file_name2}.sql'.format(coord_sys=self.coord_sys, field_name='geom_'+self.coord_sys, file_name=self.file_name, file_name2=self.file_name)
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
