import numpy
import os
import psycopg2
import math

from osgeo import gdal
from osgeo import ogr

class HDF2TIF():
    """
    Generates tif file from hdf layer
    """
    
    def __init__(self, file_name, layer_num, reproyect=False, srcSRS=None, dstSRS=None, cell_res=None):
        self.file_name = file_name
        self.layer_num = layer_num
        self.reproyect = reproyect
        self.srcSRS = srcSRS
        self.dstSRS = dstSRS
        self.cell_res = cell_res
    
    def run(self):
        # Load file and get layer
        hdf = gdal.Open('{file_name}.hdf'.format(file_name=self.file_name), gdal.GA_ReadOnly)
        src_ds = gdal.Open(hdf.GetSubDatasets()[int(self.layer_num)][0], gdal.GA_ReadOnly)

        if self.reproyect:
            warp_options = gdal.WarpOptions(srcSRS = self.srcSRS,
                                            dstSRS = self.dstSRS,
                                            xRes = self.cell_res, yRes = self.cell_res,
                                            errorThreshold = 0,
                                            resampleAlg = gdal.GRA_Average,                                                               
                                            warpOptions = ['SAMPLE_GRID=YES', 'SAMPLE_STEP=1000', 'SOURCE_EXTRA=1000'])

            gdal.Warp('./{file_name}.tif'.format(file_name=self.file_name), src_ds, options=warp_options) 

        else:
            out_file_name = '{}.tif'.format(str(self.file_name))

            # Generate file in tif format
            layer_array = src_ds.ReadAsArray()
            out = gdal.GetDriverByName('GTiff').Create(out_file_name, src_ds.RasterXSize, src_ds.RasterYSize, 1, gdal.GDT_Byte, ['COMPRESS=LZW', 'TILED=YES'])
            out.SetGeoTransform(src_ds.GetGeoTransform())
            out.SetProjection(src_ds.GetProjection())
            out.GetRasterBand(1).WriteArray(layer_array)

            # Write file to disk
            out = None
