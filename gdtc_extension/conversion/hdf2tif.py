import luigi

import numpy
import os
import psycopg2

from osgeo import gdal
from osgeo import ogr

from ..documents.hdf import HDF
from ..documents.tif import TIF
from ..gdtc_base.db import Db
from ..parameters.db_parameter import DBParameter

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
