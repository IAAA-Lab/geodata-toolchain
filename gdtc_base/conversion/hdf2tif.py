import numpy
import os
import psycopg2

from osgeo import gdal
from osgeo import ogr

class HDF2TIF():
    """
    Generates tif file from hdf layer
    """
    
    def __init__(self, file_name, layer_num):
        self.file_name = file_name
        self.layer_num = layer_num
    
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
