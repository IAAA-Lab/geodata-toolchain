import luigi
from documents import HDF, TIF

from DB import Db
from osgeo import gdal
from osgeo import ogr
import numpy
import os
import subprocess
import psycopg2
from subprocess import call

class HDF2TIF(luigi.Task):
    """
    Generates tif file from hdf layer
    """
    task_namespace = 'document'

    file_name = luigi.Parameter()
    layer_num = luigi.Parameter()

    def requires(self):
        return HDF(self.file_name)

    def output(self):
        return luigi.LocalTarget('{file_name}.tif'.format(file_name=self.file_name))
    
    def run(self):
        # Load file and get layer
        hdf = gdal.Open('{file_name}.hdf'.format(file_name=self.file_name), gdal.GA_ReadOnly)
        layer = gdal.Open(hdf.GetSubDatasets()[int(self.layer_num)][0], gdal.GA_ReadOnly)
        layer_array = layer.ReadAsArray()

        out_file_name = '{}.tif'.format(str(self.file_name))

        # Generate file in tif format
        out = gdal.GetDriverByName('GTiff').Create(out_file_name, layer.RasterXSize, layer.RasterYSize, 1, gdal.GDT_Byte, ['COMPRESS=LZW', 'TILED=YES'])
        out.SetGeoTransform(layer.GetGeoTransform())
        out.SetProjection(layer.GetProjection())
        out.GetRasterBand(1).WriteArray(layer_array)
        
        # Write file to disk
        out = None

class TIF2SQL(luigi.Task):
    """
    Generates sql query to insert into postgis db from tif file
    """
    task_namespace = 'document'

    file_name = luigi.Parameter()
    layer_path = luigi.Parameter()
    coord_sys = luigi.Parameter()
    db = luigi.Parameter()

    def requires(self):
        return TIF(self.file_name)

    def output(self):
        return luigi.LocalTarget('{file_name}.sql'.format(file_name=self.file_name))
    
    def run(self):
        # Generate sql file
        cmd = 'raster2pgsql -I -C -s {} {} -F -d {} > {}.sql'.format(self.coord_sys, self.layer_path, self.db.table, self.file_name)
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
