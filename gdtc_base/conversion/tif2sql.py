import numpy
import os
import psycopg2
import subprocess

from osgeo import gdal
from osgeo import ogr

class TIF2SQL():
    """
    Generates sql query to insert into postgis db from tif file
    """

    def __init__(self, file_name, layer_path, coord_sys, db):
        self.file_name = file_name
        self.layer_path = layer_path
        self.coord_sys = coord_sys
        self.db = db
    
    def run(self):
        # Generate sql file
        cmd = 'raster2pgsql -I -C -s {} \"{}.tif\" -F -d {} > \"{}.sql\"'.format(self.coord_sys, self.layer_path, self.db.table, self.file_name)
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
